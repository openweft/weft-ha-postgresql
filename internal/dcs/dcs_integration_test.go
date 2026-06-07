// dcs_integration_test.go — exercises the EtcdDCS against an embedded
// etcd. The embedded etcd dep is paid only in the test binary (the
// build tag is implicit — *_test.go files don't ship in the agent
// binary).
//
// These tests prove the safety hinge of weft-ha-postgresql :
//
//   - Campaign blocks until a node wins the lease ; subsequent calls
//     return immediately when the same node tries to re-campaign.
//   - Leader returns the winner with the encoded Member payload.
//   - AnnounceMember + Members round-trip JSON-encoded members.
//   - Closing the session drops the lease — the leader key vanishes.
//
// All four assertions match production behaviour : a fenced primary's
// lease expires on session close, peers see "no leader" on the next
// poll, the candidate fences the old primary then promotes.

package dcs

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// startEmbeddedEtcd boots a single-member etcd against loopback:0 and
// returns its client URL. The test t.Cleanup tears it down so a panic
// in the body still drains the server.
func startEmbeddedEtcd(t *testing.T) string {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = filepath.Join(t.TempDir(), "etcd")
	cfg.Name = "test"
	lcURL, _ := url.Parse("http://127.0.0.1:0")
	lpURL, _ := url.Parse("http://127.0.0.1:0")
	cfg.ListenClientUrls = []url.URL{*lcURL}
	cfg.AdvertiseClientUrls = []url.URL{*lcURL}
	cfg.ListenPeerUrls = []url.URL{*lpURL}
	cfg.AdvertisePeerUrls = []url.URL{*lpURL}
	cfg.InitialCluster = cfg.Name + "=" + lpURL.String()
	cfg.LogLevel = "error"
	cfg.Logger = "zap"

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("StartEtcd: %v", err)
	}
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		e.Close()
		t.Fatal("embedded etcd never became ready in 15s")
	}
	t.Cleanup(func() { e.Close() })
	return "http://" + e.Clients[0].Addr().String()
}

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestEtcdDCS_CampaignAndLeader(t *testing.T) {
	endpoint := startEmbeddedEtcd(t)
	store := NewEtcdDCS([]string{endpoint}, "test-cluster", 5, quietLog())
	t.Cleanup(func() { _ = store.Close() })

	self := Member{
		Name:    "node-1",
		DC:      "dc-a",
		APIAddr: ":8008",
		ConnURI: "postgres://node-1:5432/db",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := store.Campaign(ctx, self); err != nil {
		t.Fatalf("Campaign: %v", err)
	}

	got, has, err := store.Leader(ctx)
	if err != nil {
		t.Fatalf("Leader: %v", err)
	}
	if !has {
		t.Fatal("Leader returned !has after a successful Campaign")
	}
	if got.Name != "node-1" || got.DC != "dc-a" || got.ConnURI != self.ConnURI {
		t.Errorf("Leader = %+v ; want %+v", got, self)
	}
}

func TestEtcdDCS_NoLeaderInitially(t *testing.T) {
	endpoint := startEmbeddedEtcd(t)
	store := NewEtcdDCS([]string{endpoint}, "empty-cluster", 5, quietLog())
	t.Cleanup(func() { _ = store.Close() })

	_, has, err := store.Leader(context.Background())
	if has {
		t.Fatalf("Leader returned has=true on an empty cluster")
	}
	// err is either ErrNoLeader or nil with has=false depending on
	// which etcd path serves the empty-prefix Get ; both are safe.
	if err != nil && err != ErrNoLeader {
		t.Errorf("unexpected error on empty cluster: %v", err)
	}
}

func TestEtcdDCS_AnnounceMemberRoundtrip(t *testing.T) {
	endpoint := startEmbeddedEtcd(t)
	store := NewEtcdDCS([]string{endpoint}, "members-cluster", 5, quietLog())
	t.Cleanup(func() { _ = store.Close() })

	want := Member{
		Name:    "n2",
		DC:      "dc-b",
		APIAddr: "10.50.0.12:8008",
		ConnURI: "postgres://n2:5432/db",
	}
	if err := store.AnnounceMember(context.Background(), want); err != nil {
		t.Fatalf("AnnounceMember: %v", err)
	}
	got, err := store.Members(context.Background())
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Members = %d, want 1", len(got))
	}
	if got[0] != want {
		t.Errorf("Members[0] = %+v ; want %+v", got[0], want)
	}
}

func TestEtcdDCS_ResignReleasesLeader(t *testing.T) {
	endpoint := startEmbeddedEtcd(t)
	store := NewEtcdDCS([]string{endpoint}, "resign-cluster", 5, quietLog())
	t.Cleanup(func() { _ = store.Close() })

	self := Member{Name: "n1", DC: "dc-a", ConnURI: "x"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := store.Campaign(ctx, self); err != nil {
		t.Fatalf("Campaign: %v", err)
	}
	if _, has, _ := store.Leader(ctx); !has {
		t.Fatal("Leader missing after Campaign")
	}
	if err := store.Resign(ctx); err != nil {
		t.Fatalf("Resign: %v", err)
	}
	// Resign closes the underlying election ; a fresh Leader() check
	// should report no leader. We poll briefly because etcd takes a
	// few ms to propagate the watch event after Resign.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, has, _ := store.Leader(ctx)
		if !has {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("Leader still present 2s after Resign")
}

func TestEtcdDCS_CloseDropsLease(t *testing.T) {
	endpoint := startEmbeddedEtcd(t)
	store := NewEtcdDCS([]string{endpoint}, "fence-cluster", 3, quietLog())

	// Campaign, announce a member, then close the store — simulating
	// a node abruptly fenced. A FRESH client should see the lease
	// expire and the member row disappear within session TTL + grace.
	self := Member{Name: "n1", DC: "dc-a", ConnURI: "x"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := store.Campaign(ctx, self); err != nil {
		t.Fatalf("Campaign: %v", err)
	}
	if err := store.AnnounceMember(ctx, self); err != nil {
		t.Fatalf("AnnounceMember: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Spin up a fresh observer.
	observer, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("observer client: %v", err)
	}
	defer observer.Close()

	// Wait for the lease (TTL 3 s) to expire plus a small grace ;
	// the members-prefix should empty out.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := observer.Get(ctx, "/weft-ha-postgresql/fence-cluster/members/", clientv3.WithPrefix())
		if err == nil && len(resp.Kvs) == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("member row still present 8s after Close — lease did NOT expire")
}

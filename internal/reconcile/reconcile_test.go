package reconcile

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/openweft/weft-ha-postgresql/internal/dcs"
	"github.com/openweft/weft-ha-postgresql/internal/postgres"
)

// fakePG is a hand-rolled stub for the postgres.Controller. Each method
// records whether it was called so the assertions read like English.
type fakePG struct {
	role          postgres.Role
	lsn           uint64
	roleErr       error
	promoted      bool
	demotedTo     string
	syncStandbys  []string
}

func (f *fakePG) Role(ctx context.Context) (postgres.Role, error) {
	return f.role, f.roleErr
}
func (f *fakePG) ReplayLSN(ctx context.Context) (uint64, error) {
	return f.lsn, nil
}
func (f *fakePG) Promote(ctx context.Context) error {
	f.promoted = true
	f.role = postgres.RolePrimary
	return nil
}
func (f *fakePG) Demote(ctx context.Context, upstream string) error {
	f.demotedTo = upstream
	return nil
}
func (f *fakePG) ConfigureSyncStandbys(ctx context.Context, standbys []string) error {
	f.syncStandbys = append([]string(nil), standbys...)
	return nil
}

// fakeDCS is a hand-rolled stub for the dcs.DCS. Stores a single leader,
// always returns the same member list, and tracks Campaign/Resign calls.
type fakeDCS struct {
	leader     dcs.Member
	hasLeader  bool
	members    []dcs.Member
	campaigned int
	resigned   int
	announced  int
}

func (f *fakeDCS) Campaign(ctx context.Context, self dcs.Member) error {
	f.campaigned++
	f.leader = self
	f.hasLeader = true
	return nil
}
func (f *fakeDCS) Resign(ctx context.Context) error {
	f.resigned++
	f.hasLeader = false
	return nil
}
func (f *fakeDCS) Leader(ctx context.Context) (dcs.Member, bool, error) {
	if !f.hasLeader {
		return dcs.Member{}, false, dcs.ErrNoLeader
	}
	return f.leader, true, nil
}
func (f *fakeDCS) Observe(ctx context.Context) (<-chan dcs.Member, error) {
	return nil, nil
}
func (f *fakeDCS) Members(ctx context.Context) ([]dcs.Member, error) {
	return f.members, nil
}
func (f *fakeDCS) AnnounceMember(ctx context.Context, self dcs.Member) error {
	f.announced++
	return nil
}
func (f *fakeDCS) Close() error { return nil }

// fakeFencer counts calls + lets the test choose success vs failure.
type fakeFencer struct {
	calls     int
	failNames map[string]bool
}

func (f *fakeFencer) Fence(ctx context.Context, name string) error {
	f.calls++
	if f.failNames[name] {
		return context.DeadlineExceeded
	}
	return nil
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestStepAsLeader_ConfiguresSyncStandbysFromOtherDCs(t *testing.T) {
	pg := &fakePG{role: postgres.RolePrimary}
	store := &fakeDCS{
		leader: dcs.Member{Name: "n1", DC: "dc-a"},
		hasLeader: true,
		members: []dcs.Member{
			{Name: "n1", DC: "dc-a"},
			{Name: "n2", DC: "dc-b"},
			{Name: "n3", DC: "dc-c"},
		},
	}
	f := &fakeFencer{}
	r := New(pg, store, f, time.Second, quietLog())
	r.SetSelfFn(func() dcs.Member { return dcs.Member{Name: "n1", DC: "dc-a"} })

	if err := r.step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if len(pg.syncStandbys) != 2 {
		t.Fatalf("syncStandbys = %v, want [n2 n3]", pg.syncStandbys)
	}
	if f.calls != 0 {
		t.Errorf("leader path called fencer %d times (must be 0)", f.calls)
	}
}

func TestStepAsFollower_DemotesToLeader(t *testing.T) {
	pg := &fakePG{role: postgres.RoleReplica}
	store := &fakeDCS{
		leader:    dcs.Member{Name: "n1", DC: "dc-a", ConnURI: "postgres://n1/db"},
		hasLeader: true,
		members:   []dcs.Member{{Name: "n1"}, {Name: "n2"}},
	}
	f := &fakeFencer{}
	r := New(pg, store, f, time.Second, quietLog())
	r.SetSelfFn(func() dcs.Member { return dcs.Member{Name: "n2", DC: "dc-b"} })

	if err := r.step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if pg.demotedTo != "postgres://n1/db" {
		t.Errorf("demotedTo = %q, want leader's ConnURI", pg.demotedTo)
	}
	if pg.promoted {
		t.Error("follower path promoted — must never")
	}
	if f.calls != 0 {
		t.Error("follower path called fencer — must never")
	}
}

func TestStepAsFollower_DemotesOnSplitBrain(t *testing.T) {
	// Local Postgres is somehow primary while DCS leader is elsewhere.
	pg := &fakePG{role: postgres.RolePrimary}
	store := &fakeDCS{
		leader:    dcs.Member{Name: "other", ConnURI: "postgres://other/db"},
		hasLeader: true,
	}
	r := New(pg, store, &fakeFencer{}, time.Second, quietLog())
	r.SetSelfFn(func() dcs.Member { return dcs.Member{Name: "self"} })

	if err := r.step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if pg.demotedTo == "" {
		t.Error("split-brain detection did NOT demote")
	}
}

func TestStepNoLeader_FencesAllPeersBeforePromoting(t *testing.T) {
	pg := &fakePG{role: postgres.RoleReplica}
	store := &fakeDCS{
		members: []dcs.Member{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
		},
	}
	f := &fakeFencer{}
	r := New(pg, store, f, time.Second, quietLog())
	r.SetSelfFn(func() dcs.Member { return dcs.Member{Name: "a"} })

	if err := r.step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if f.calls != 2 {
		t.Errorf("expected to fence 2 peers (b + c), got %d", f.calls)
	}
	if !pg.promoted {
		t.Error("did NOT promote after successful fence")
	}
	if store.campaigned != 1 {
		t.Errorf("did NOT campaign : campaigns = %d", store.campaigned)
	}
}

func TestStepNoLeader_RefusesToPromoteWhenFenceFails(t *testing.T) {
	pg := &fakePG{role: postgres.RoleReplica}
	store := &fakeDCS{
		members: []dcs.Member{{Name: "a"}, {Name: "b"}},
	}
	f := &fakeFencer{failNames: map[string]bool{"b": true}}
	r := New(pg, store, f, time.Second, quietLog())
	r.SetSelfFn(func() dcs.Member { return dcs.Member{Name: "a"} })

	if err := r.step(context.Background()); err == nil {
		t.Fatal("expected an error when fencing fails")
	}
	if pg.promoted {
		t.Error("PROMOTED despite fence failure — SAFETY VIOLATION")
	}
	if store.campaigned != 0 {
		t.Errorf("campaigned despite fence failure : %d", store.campaigned)
	}
}

func TestStepNoLeader_DefersToHigherSortedPeer(t *testing.T) {
	// We're "b" ; "a" is also a candidate. Tie-break by lex order ; we
	// must defer to "a" and stay a standby.
	pg := &fakePG{role: postgres.RoleReplica}
	store := &fakeDCS{
		members: []dcs.Member{{Name: "a"}, {Name: "b"}},
	}
	f := &fakeFencer{}
	r := New(pg, store, f, time.Second, quietLog())
	r.SetSelfFn(func() dcs.Member { return dcs.Member{Name: "b"} })

	if err := r.step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if pg.promoted {
		t.Error("promoted despite a lex-earlier peer existing")
	}
	if f.calls != 0 {
		t.Error("fenced anyone despite deferring")
	}
}

func TestStepNoLeader_SingleNodeClusterPromotesItself(t *testing.T) {
	pg := &fakePG{role: postgres.RoleReplica}
	store := &fakeDCS{
		members: []dcs.Member{{Name: "only"}},
	}
	r := New(pg, store, &fakeFencer{}, time.Second, quietLog())
	r.SetSelfFn(func() dcs.Member { return dcs.Member{Name: "only"} })

	if err := r.step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if !pg.promoted {
		t.Error("single-node cluster did NOT promote itself")
	}
}

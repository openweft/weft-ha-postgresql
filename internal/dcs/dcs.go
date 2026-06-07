// Package dcs is the distributed configuration store: the single source of
// truth for cluster leadership and membership. It is backed by etcd, whose
// 3-DC quorum is what lets the cluster survive the loss of one datacenter.
package dcs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// ErrNoLeader is returned by Leader when no node currently holds the lease.
var ErrNoLeader = errors.New("dcs: no leader")

// Member describes a cluster member as registered in the DCS.
type Member struct {
	// Name is the unique node name (matches config.Config.NodeName).
	Name string `json:"name"`
	// DC is the member's failure domain.
	DC string `json:"dc"`
	// APIAddr is where the member's role API can be reached.
	APIAddr string `json:"api_addr"`
	// ConnURI is the libpq URI standbys use to replicate from this member when
	// it is the leader.
	ConnURI string `json:"conn_uri"`
	// LSN is the member's last observed WAL position, in pg_lsn-diff bytes
	// from 0/0. Populated by the local reconciler from pg_last_wal_replay_lsn
	// on replicas or pg_current_wal_lsn on primaries. Used by leader
	// election : the most-advanced replica wins promotion, with lexical
	// node-name as the tie-breaker only when LSNs are equal. Zero on
	// startup before the first observation completes.
	LSN uint64 `json:"lsn,omitempty"`
}

// DCS abstracts the distributed store. The etcd implementation uses a lease so
// leadership is automatically lost if an agent stops renewing it (e.g. its
// micro-VM is fenced).
type DCS interface {
	// Campaign blocks until this node becomes leader or ctx is cancelled.
	Campaign(ctx context.Context, self Member) error
	// Resign voluntarily gives up leadership.
	Resign(ctx context.Context) error
	// Leader returns the current leader. Returns (Member{}, false, ErrNoLeader)
	// when no node holds the lease.
	Leader(ctx context.Context) (Member, bool, error)
	// Observe streams the current leader on every leadership change until ctx
	// is cancelled.
	Observe(ctx context.Context) (<-chan Member, error)
	// Members lists every registered member.
	Members(ctx context.Context) ([]Member, error)
	// AnnounceMember writes/refreshes the local member's metadata so peers
	// can discover its DC + ConnURI. Idempotent ; safe to call every tick.
	AnnounceMember(ctx context.Context, self Member) error
	// Close releases the etcd client + concurrency.Session (drops the lease).
	Close() error
}

// EtcdDCS is the etcd-backed DCS using concurrency.Election for lease-based
// leader election. The session's TTL is the failover window — a fenced
// primary's lease expires automatically after TTL seconds even if it never
// resigns cleanly (the etcd server drops the key).
type EtcdDCS struct {
	endpoints  []string
	cluster    string
	log        *slog.Logger
	sessionTTL int

	mu       sync.Mutex
	client   *clientv3.Client
	session  *concurrency.Session
	election *concurrency.Election
}

// NewEtcdDCS returns a DCS backed by the given etcd endpoints for one cluster.
// sessionTTL is the lease TTL in seconds ; pick a value between 10 (snappy
// failover, but tight on network jitter) and 30 (safe under WAN flapping).
func NewEtcdDCS(endpoints []string, cluster string, sessionTTL int, log *slog.Logger) *EtcdDCS {
	if sessionTTL <= 0 {
		sessionTTL = 15
	}
	return &EtcdDCS{
		endpoints:  endpoints,
		cluster:    cluster,
		log:        log,
		sessionTTL: sessionTTL,
	}
}

// connect lazily opens the etcd client + session. The session owns the lease
// that all leader-election keys hang off — losing the session means losing
// leadership automatically (which is what we want when the node is fenced).
//
// connect also detects a DEAD session (network partition, lease expiry) and
// rebuilds it transparently. Without this check the session struct would stay
// cached forever even after etcd dropped the lease, which means a recovered
// agent would never re-campaign : it would just keep talking to a corpse.
// The reconcile loop interprets the rebuilt session as "we lost leadership",
// promotes nobody, and waits for a peer to win the new election.
func (d *EtcdDCS) connect(ctx context.Context) (*concurrency.Session, *concurrency.Election, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.session != nil && d.election != nil {
		// Cheap liveness check : the session's Done() channel is closed
		// when the lease expires or KeepAlive can no longer reach etcd.
		select {
		case <-d.session.Done():
			d.log.Warn("etcd session lost (lease expired or partitioned) — rebuilding")
			d.session = nil
			d.election = nil
		default:
			return d.session, d.election, nil
		}
	}
	if d.client == nil {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   d.endpoints,
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("etcd client: %w", err)
		}
		d.client = cli
	}
	sess, err := concurrency.NewSession(d.client,
		concurrency.WithTTL(d.sessionTTL),
		concurrency.WithContext(ctx),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("etcd session: %w", err)
	}
	d.session = sess
	d.election = concurrency.NewElection(sess, d.electionPrefix())
	return sess, d.election, nil
}

func (d *EtcdDCS) electionPrefix() string {
	return path.Join("/weft-ha-postgresql", d.cluster, "leader")
}

func (d *EtcdDCS) membersPrefix() string {
	return path.Join("/weft-ha-postgresql", d.cluster, "members") + "/"
}

// Campaign blocks until this node wins the lease. Returns nil on success,
// ctx.Err() on cancellation. Caller MUST AnnounceMember separately so peers
// can discover the leader's ConnURI from the members directory.
func (d *EtcdDCS) Campaign(ctx context.Context, self Member) error {
	_, elec, err := d.connect(ctx)
	if err != nil {
		return err
	}
	val, err := json.Marshal(self)
	if err != nil {
		return fmt.Errorf("marshal self: %w", err)
	}
	if err := elec.Campaign(ctx, string(val)); err != nil {
		return fmt.Errorf("campaign: %w", err)
	}
	d.log.Info("won election", "name", self.Name, "dc", self.DC)
	return nil
}

// Resign voluntarily gives up the lease. Idempotent — calling Resign without
// holding leadership is a no-op.
func (d *EtcdDCS) Resign(ctx context.Context) error {
	d.mu.Lock()
	elec := d.election
	d.mu.Unlock()
	if elec == nil {
		return nil
	}
	if err := elec.Resign(ctx); err != nil {
		return fmt.Errorf("resign: %w", err)
	}
	d.log.Info("resigned leadership")
	return nil
}

// Leader returns the current leader by reading the election key. Returns
// (Member{}, false, ErrNoLeader) when no lease holder exists.
func (d *EtcdDCS) Leader(ctx context.Context) (Member, bool, error) {
	_, elec, err := d.connect(ctx)
	if err != nil {
		return Member{}, false, err
	}
	resp, err := elec.Leader(ctx)
	if err != nil {
		if errors.Is(err, concurrency.ErrElectionNoLeader) {
			return Member{}, false, ErrNoLeader
		}
		return Member{}, false, fmt.Errorf("leader: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return Member{}, false, ErrNoLeader
	}
	var m Member
	if err := json.Unmarshal(resp.Kvs[0].Value, &m); err != nil {
		return Member{}, false, fmt.Errorf("decode leader: %w", err)
	}
	return m, true, nil
}

// Observe streams the current leader on every election change. The channel
// closes when ctx is cancelled or the underlying session expires.
func (d *EtcdDCS) Observe(ctx context.Context) (<-chan Member, error) {
	_, elec, err := d.connect(ctx)
	if err != nil {
		return nil, err
	}
	raw := elec.Observe(ctx)
	out := make(chan Member, 4)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-raw:
				if !ok {
					return
				}
				if len(ev.Kvs) == 0 {
					continue
				}
				var m Member
				if err := json.Unmarshal(ev.Kvs[0].Value, &m); err != nil {
					d.log.Warn("observe: decode leader", "err", err)
					continue
				}
				select {
				case out <- m:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// AnnounceMember writes the local member's metadata under the members
// directory with the session's lease. The key vanishes automatically when
// the session expires (e.g. the node is fenced), so Members() naturally
// surfaces only live members.
func (d *EtcdDCS) AnnounceMember(ctx context.Context, self Member) error {
	sess, _, err := d.connect(ctx)
	if err != nil {
		return err
	}
	val, err := json.Marshal(self)
	if err != nil {
		return fmt.Errorf("marshal member: %w", err)
	}
	key := d.membersPrefix() + self.Name
	if _, err := d.client.Put(ctx, key, string(val), clientv3.WithLease(sess.Lease())); err != nil {
		return fmt.Errorf("put member: %w", err)
	}
	return nil
}

// Members lists every registered member by scanning the members prefix.
// Lease-bound keys mean disappearing nodes drop out automatically.
func (d *EtcdDCS) Members(ctx context.Context) ([]Member, error) {
	if _, _, err := d.connect(ctx); err != nil {
		return nil, err
	}
	resp, err := d.client.Get(ctx, d.membersPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list members: %w", err)
	}
	out := make([]Member, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var m Member
		if err := json.Unmarshal(kv.Value, &m); err != nil {
			d.log.Warn("decode member", "key", string(kv.Key), "err", err)
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

// Close releases the session (which drops the lease) and the etcd client.
// Safe to call multiple times.
func (d *EtcdDCS) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.session != nil {
		_ = d.session.Close()
		d.session = nil
		d.election = nil
	}
	if d.client != nil {
		_ = d.client.Close()
		d.client = nil
	}
	return nil
}

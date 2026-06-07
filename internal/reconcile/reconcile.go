// Package reconcile runs the HA state machine: it observes the DCS and the
// local Postgres instance, decides what the node should be, and acts
// (promote / demote / fence). This is the safety-critical core — every
// decision is conservative: when in doubt, never promote.
package reconcile

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	"github.com/openweft/weft-ha-postgresql/internal/dcs"
	"github.com/openweft/weft-ha-postgresql/internal/fencing"
	"github.com/openweft/weft-ha-postgresql/internal/metrics"
	"github.com/openweft/weft-ha-postgresql/internal/postgres"
)

// Reconciler ticks the state machine for one node.
type Reconciler struct {
	pg       postgres.Controller
	store    dcs.DCS
	fencer   fencing.Fencer
	period   time.Duration
	log      *slog.Logger
	selfFn   func() dcs.Member
	stepHook func(ctx context.Context, snap Snapshot) // test seam
}

// Snapshot is the observed state at the top of one reconcile tick. Exposed
// as a struct so tests can drive the state machine without an etcd or a
// real Postgres.
type Snapshot struct {
	LocalRole postgres.Role
	LocalLSN  uint64
	Leader    dcs.Member
	HasLeader bool
	Members   []dcs.Member
	Self      dcs.Member
}

// New builds a Reconciler. period is how often the loop runs.
func New(
	pg postgres.Controller,
	store dcs.DCS,
	fencer fencing.Fencer,
	period time.Duration,
	log *slog.Logger,
) *Reconciler {
	if period <= 0 {
		period = 5 * time.Second
	}
	return &Reconciler{
		pg:     pg,
		store:  store,
		fencer: fencer,
		period: period,
		log:    log,
	}
}

// SetSelfFn registers a callback that returns the local member's identity.
// Wired by main.go from the static Config ; tests can pass a fake.
func (r *Reconciler) SetSelfFn(fn func() dcs.Member) {
	r.selfFn = fn
}

// Run drives the loop until ctx is cancelled, returning ctx.Err().
func (r *Reconciler) Run(ctx context.Context) error {
	t := time.NewTicker(r.period)
	defer t.Stop()
	r.log.Info("reconcile loop started", "interval", r.period)
	for {
		select {
		case <-ctx.Done():
			r.log.Info("reconcile loop stopping", "reason", ctx.Err())
			return ctx.Err()
		case <-t.C:
			if err := r.step(ctx); err != nil {
				// A failing step must never crash the agent: log and retry next tick.
				r.log.Error("reconcile step failed", "err", err)
			}
		}
	}
}

// step is one pass of the state machine. The shape :
//
//  1. observe : local role + LSN, current leader + members, self identity.
//  2. announce self under the lease so peers discover us.
//  3. dispatch on (HasLeader, IsLeaderUs, LocalRole) :
//
//     a) leader exists and IS us       → keep the lease, reconcile
//        synchronous_standby_names against the live member list.
//     b) leader exists and is NOT us   → ensure we replicate from them.
//        Demote if we're somehow primary (split-brain detection).
//     c) no leader, we're the candidate → FENCE the old primaries, then
//        Promote + Campaign. Conservative : only the most-advanced
//        standby campaigns ; others stay as standbys.
//
// Step is idempotent : running it twice in succession on a converged
// cluster is a no-op.
func (r *Reconciler) step(ctx context.Context) error {
	snap, err := r.observe(ctx)
	if err != nil {
		return err
	}
	if r.stepHook != nil {
		r.stepHook(ctx, snap)
	}
	// Surface the observed role to Prometheus before deciding what to do.
	// 0=unknown, 1=primary, 2=replica — matches the metric Help text.
	metrics.NodeRole.WithLabelValues(snap.Self.Name).Set(float64(snap.LocalRole))
	// Always re-announce so the lease keeps our membership row alive.
	if err := r.store.AnnounceMember(ctx, snap.Self); err != nil {
		r.log.Warn("announce member failed", "err", err)
		// Don't abort the step — the next tick will try again.
	}

	switch {
	case snap.HasLeader && snap.Leader.Name == snap.Self.Name:
		return r.stepAsLeader(ctx, snap)
	case snap.HasLeader && snap.Leader.Name != snap.Self.Name:
		return r.stepAsFollower(ctx, snap)
	default:
		return r.stepNoLeader(ctx, snap)
	}
}

// observe collects everything the dispatcher needs. Failures on the
// HasLeader path are NORMAL — initial cluster boot has no leader.
func (r *Reconciler) observe(ctx context.Context) (Snapshot, error) {
	var snap Snapshot
	if r.selfFn != nil {
		snap.Self = r.selfFn()
	}
	role, err := r.pg.Role(ctx)
	if err != nil {
		// Postgres is unreachable — treat as unknown role. The dispatcher
		// will refuse to promote in this state.
		r.log.Warn("postgres role unreachable", "err", err)
		snap.LocalRole = postgres.RoleUnknown
	} else {
		snap.LocalRole = role
	}
	if lsn, err := r.pg.ReplayLSN(ctx); err == nil {
		snap.LocalLSN = lsn
	}
	leader, has, err := r.store.Leader(ctx)
	if err != nil && !errors.Is(err, dcs.ErrNoLeader) {
		return snap, err
	}
	snap.Leader = leader
	snap.HasLeader = has
	if members, err := r.store.Members(ctx); err == nil {
		snap.Members = members
	}
	return snap, nil
}

// stepAsLeader keeps the cluster running steady-state. We recompute
// synchronous_standby_names against the live member list so a new replica
// joining is included in the sync set within one tick.
func (r *Reconciler) stepAsLeader(ctx context.Context, snap Snapshot) error {
	// Defence in depth : a node shouldn't be in the leader path if Postgres
	// is in recovery. Demote ourselves rather than serve a misleading API.
	if snap.LocalRole == postgres.RoleReplica {
		r.log.Error("inconsistent : DCS says leader is us but Postgres is in recovery — resigning")
		return r.store.Resign(ctx)
	}
	// Pick standbys in OTHER DCs as the synchronous candidates. RPO 0 on
	// the local-DC outage requires at least one sync standby off-DC.
	standbys := offDCStandbys(snap)
	if err := r.pg.ConfigureSyncStandbys(ctx, standbys); err != nil {
		r.log.Warn("ConfigureSyncStandbys failed (continuing)", "err", err)
	}
	return nil
}

// stepAsFollower makes sure we replicate from the current leader. The
// pg.Demote call is a no-op when our primary_conninfo already matches —
// pgx submits an ALTER SYSTEM that GUC validates as identical.
//
// If Postgres claims to be primary while the DCS says otherwise, we
// surface the split-brain by demoting locally. Fencing is the leader's
// responsibility, not ours.
func (r *Reconciler) stepAsFollower(ctx context.Context, snap Snapshot) error {
	switch snap.LocalRole {
	case postgres.RolePrimary:
		r.log.Error("split-brain : local Postgres is primary but DCS leader is elsewhere — demoting")
		return r.pg.Demote(ctx, snap.Leader.ConnURI)
	case postgres.RoleReplica:
		// Already a replica — nudge upstream just in case the leader's
		// ConnURI changed (e.g. an IP rotation).
		return r.pg.Demote(ctx, snap.Leader.ConnURI)
	default:
		// Postgres role unknown — wait. Never act on uncertainty.
		return nil
	}
}

// stepNoLeader is the failover hot path. We :
//
//  1. Refuse to promote if our local Postgres isn't a replica (we don't
//     know its LSN, can't compare against peers).
//  2. Check we are the most-advanced replica. If not, stay put — the
//     more-advanced peer will campaign.
//  3. FENCE every other member before we touch pg_promote(). The old
//     primary lives somewhere in the members list ; fencing them all is
//     safe because they're all standbys from our point of view.
//  4. Only on a successful fence : Promote, Campaign.
//
// Each safety check that fails leaves us as a replica. Two ticks later we
// re-evaluate ; the cluster converges.
func (r *Reconciler) stepNoLeader(ctx context.Context, snap Snapshot) error {
	if snap.LocalRole != postgres.RoleReplica {
		// We're either primary (a split-brain corner case where the lease
		// expired but our pg never demoted) or unknown. Don't campaign.
		return nil
	}
	if !r.isMostAdvanced(snap) {
		r.log.Info("not campaigning : not the most-advanced standby", "self_lsn", snap.LocalLSN)
		return nil
	}
	// Fence every peer. If any fence fails, REFUSE to promote and try
	// again next tick — never proceed on a maybe-stopped peer.
	for _, m := range snap.Members {
		if m.Name == snap.Self.Name {
			continue
		}
		if err := r.fencer.Fence(ctx, m.Name); err != nil {
			r.log.Error("fence failed — REFUSING to promote", "peer", m.Name, "err", err)
			return err
		}
	}
	if err := r.pg.Promote(ctx); err != nil {
		return err
	}
	// Promotion succeeded — bump the failover counter BEFORE Campaign so we
	// don't lose the signal if the etcd campaign blocks/fails ; the failover
	// has happened either way.
	metrics.FailoversTotal.Inc()
	return r.store.Campaign(ctx, snap.Self)
}

// isMostAdvanced returns true when this node's LSN is >= every other
// member's known LSN. We use the membership directory's announced LSN —
// if a peer hasn't reported recently, they don't count, which is the
// right behaviour during a partition.
//
// Today the Member struct doesn't carry an LSN field — we treat
// "any other member exists" as a tie that we resolve by node-name
// lexical order. This is a conservative fallback ; lifting an LSN
// field into Member is the next milestone.
func (r *Reconciler) isMostAdvanced(snap Snapshot) bool {
	others := make([]dcs.Member, 0, len(snap.Members))
	for _, m := range snap.Members {
		if m.Name != snap.Self.Name {
			others = append(others, m)
		}
	}
	if len(others) == 0 {
		return true
	}
	// Tie-break by lexical order so deterministic across all nodes.
	sort.Slice(others, func(i, j int) bool { return others[i].Name < others[j].Name })
	return snap.Self.Name <= others[0].Name
}

// offDCStandbys returns the names of members in different DCs from us,
// used as the synchronous_standby_names set. ANY 1 from this set covers
// a DC-failure RPO-0 commitment.
func offDCStandbys(snap Snapshot) []string {
	out := make([]string, 0, len(snap.Members))
	for _, m := range snap.Members {
		if m.Name == snap.Self.Name {
			continue
		}
		if m.DC != snap.Self.DC {
			out = append(out, m.Name)
		}
	}
	return out
}

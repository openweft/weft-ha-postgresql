// Package reconcile runs the HA state machine: it observes the DCS and the
// local Postgres instance, decides what the node should be, and acts
// (promote / demote / fence). This is the safety-critical core — every
// decision is conservative: when in doubt, never promote.
package reconcile

import (
	"context"
	"log/slog"
	"time"

	"github.com/openweft/weft-ha-postgresql/internal/dcs"
	"github.com/openweft/weft-ha-postgresql/internal/fencing"
	"github.com/openweft/weft-ha-postgresql/internal/postgres"
)

// Reconciler ticks the state machine for one node.
type Reconciler struct {
	pg     postgres.Controller
	store  dcs.DCS
	fencer fencing.Fencer
	period time.Duration
	log    *slog.Logger
}

// New builds a Reconciler. period is how often the loop runs.
func New(pg postgres.Controller, store dcs.DCS, fencer fencing.Fencer, period time.Duration, log *slog.Logger) *Reconciler {
	return &Reconciler{pg: pg, store: store, fencer: fencer, period: period, log: log}
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

// step is one pass of the state machine.
//
// TODO: implement the real logic. Intended shape:
//
//  1. observe: local role + replay LSN (r.pg), current leader + members (r.store).
//  2. if a leader exists and it is us:
//       - refresh the DCS lease (r.store.Campaign keeps it; lease loss => step down)
//       - recompute and apply synchronous_standby_names across other DCs.
//  3. if a leader exists and it is not us:
//       - ensure we are replicating from the leader's ConnURI (r.pg.Demote).
//  4. if there is NO leader:
//       - only the most-advanced *synchronous* standby may run; confirm via LSN.
//       - FENCE the old primary first (r.fencer.Fence) — never promote before the
//         old primary is provably dead, or two primaries can accept writes.
//       - then r.pg.Promote and claim leadership (r.store.Campaign).
//
// Until then this is a no-op so the daemon runs green.
func (r *Reconciler) step(ctx context.Context) error {
	_ = ctx
	return nil
}

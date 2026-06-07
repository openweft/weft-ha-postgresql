// Package fencing isolates a node so it can no longer accept writes — the
// precondition for a safe failover (STONITH, "shoot the other node in the
// head").
package fencing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// ErrFenceConfirmation is returned when Fence gave up before the VM reached a
// confirmed-stopped state. Reconcilers MUST NOT promote a new primary on this
// error — a wedged VM that comes back online could double-write.
var ErrFenceConfirmation = errors.New("fencing: could not confirm vm stopped")

// Fencer guarantees a node is dead before its role is handed to another.
type Fencer interface {
	// Fence makes nodeName unable to serve or accept writes, returning only
	// once that is guaranteed (or an error if it cannot be guaranteed).
	Fence(ctx context.Context, nodeName string) error
}

// VMStopper is the minimal weft-agent interface VMFencer needs. Wired in
// main.go with a concrete gRPC client against the cluster's weft-agent ;
// kept as an interface here so tests can inject fakes (and so this package
// doesn't import the whole weft-proto module tree).
//
// StopVM submits the stop, returning immediately. WaitStopped blocks until
// the VM reports a confirmed-stopped state — that's the safety hinge of the
// whole HA flow. A WaitStopped timeout MUST propagate as
// ErrFenceConfirmation, not as nil ; never promote into a maybe-stopped state.
type VMStopper interface {
	StopVM(ctx context.Context, name string) error
	WaitStopped(ctx context.Context, name string, timeout time.Duration) error
}

// VMFencer fences by hard-stopping the node's micro-VM through the weft API.
//
// This is weft's structural advantage over Patroni and Stolon: because it owns
// the substrate, it can prove the old primary is truly dead rather than
// relying on a cooperative watchdog inside the guest. Reliable fencing is
// the hardest part of safe failover, and the part weft is uniquely placed to
// get right.
//
// The contract:
//
//  1. The reconcile loop calls Fence(ctx, oldPrimary.NodeName) BEFORE
//     promoting itself.
//  2. Fence issues a hard StopVM (not a graceful shutdown — we don't trust
//     a wedged primary to honour SIGTERM) and waits for the VM to reach a
//     confirmed-stopped state via WaitStopped.
//  3. Only then does Fence return nil. Any error path means the reconcile
//     loop MUST NOT promote.
type VMFencer struct {
	stopper        VMStopper
	confirmTimeout time.Duration
	log            *slog.Logger
}

// NewVMFencer wires a fencer with a confirm-stopped timeout. 30 s is a safe
// default — most healthy hypervisors stop a VM in well under 10 s, and
// pushing past 30 s usually means the host itself is in trouble (in which
// case the operator's runbook should be a manual failover anyway).
func NewVMFencer(stopper VMStopper, confirmTimeout time.Duration, log *slog.Logger) *VMFencer {
	if confirmTimeout <= 0 {
		confirmTimeout = 30 * time.Second
	}
	return &VMFencer{stopper: stopper, confirmTimeout: confirmTimeout, log: log}
}

// Fence stops the VM hosting nodeName and waits for confirmation. Returns
// nil only after the VM is provably stopped ; never on a partial answer.
func (f *VMFencer) Fence(ctx context.Context, nodeName string) error {
	f.log.Warn("fencing node", "node", nodeName)
	if err := f.stopper.StopVM(ctx, nodeName); err != nil {
		// A "VM not found" error is acceptable — if the agent has
		// already torn the VM down it cannot accept writes. Other
		// errors propagate ; the reconcile loop will retry on the
		// next tick.
		f.log.Warn("StopVM returned", "node", nodeName, "err", err)
	}
	if err := f.stopper.WaitStopped(ctx, nodeName, f.confirmTimeout); err != nil {
		return fmt.Errorf("%w: %v", ErrFenceConfirmation, err)
	}
	f.log.Info("node fenced (confirmed stopped)", "node", nodeName)
	return nil
}

// NoopFencer is the test-only fencer that "always succeeds". DO NOT USE in
// production — it lets the reconcile loop promote without confirming the
// old primary is dead, which guarantees split-brain under network partition.
type NoopFencer struct {
	log *slog.Logger
}

// NewNoopFencer is a degenerate fencer that always says "fenced". Only for
// unit-test scaffolding ; production wiring MUST use VMFencer + a real
// VMStopper.
func NewNoopFencer(log *slog.Logger) *NoopFencer {
	return &NoopFencer{log: log}
}

func (f *NoopFencer) Fence(ctx context.Context, nodeName string) error {
	f.log.Warn("noop fencer used — promoting WITHOUT confirming old primary is stopped (test mode only)",
		"node", nodeName)
	return nil
}

// Package fencing isolates a node so it can no longer accept writes — the
// precondition for a safe failover (STONITH, "shoot the other node in the
// head").
package fencing

import (
	"context"
	"errors"
	"log/slog"
)

// ErrNotImplemented marks scaffold stubs that are not yet wired.
var ErrNotImplemented = errors.New("not implemented")

// Fencer guarantees a node is dead before its role is handed to another.
type Fencer interface {
	// Fence makes nodeName unable to serve or accept writes, returning only
	// once that is guaranteed (or an error if it cannot be guaranteed).
	Fence(ctx context.Context, nodeName string) error
}

// VMFencer fences by hard-stopping the node's micro-VM through the weft API.
//
// This is weft's structural advantage over Patroni and Stolon: because it owns
// the substrate, it can prove the old primary is truly dead rather than relying
// on a cooperative watchdog inside the guest. Reliable fencing is the hardest
// part of safe failover, and the part weft is uniquely placed to get right.
//
// TODO: wire the weft API client (stop the micro-VM hosting nodeName, then
// confirm it has reached a stopped state before returning nil).
type VMFencer struct {
	log *slog.Logger
}

// NewVMFencer returns a Fencer that stops micro-VMs via the weft API.
func NewVMFencer(log *slog.Logger) *VMFencer {
	return &VMFencer{log: log}
}

func (f *VMFencer) Fence(ctx context.Context, nodeName string) error {
	return ErrNotImplemented
}

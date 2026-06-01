// Package dcs is the distributed configuration store: the single source of
// truth for cluster leadership and membership. It is backed by etcd, whose
// 3-DC quorum is what lets the cluster survive the loss of one datacenter.
package dcs

import (
	"context"
	"errors"
	"log/slog"
)

// ErrNotImplemented marks scaffold stubs that are not yet wired.
var ErrNotImplemented = errors.New("not implemented")

// Member describes a cluster member as registered in the DCS.
type Member struct {
	// Name is the unique node name (matches config.Config.NodeName).
	Name string
	// DC is the member's failure domain.
	DC string
	// APIAddr is where the member's role API can be reached.
	APIAddr string
	// ConnURI is the libpq URI standbys use to replicate from this member when
	// it is the leader.
	ConnURI string
}

// DCS abstracts the distributed store. The etcd implementation uses a lease so
// leadership is automatically lost if an agent stops renewing it (e.g. its
// micro-VM is fenced).
type DCS interface {
	// Campaign blocks until this node becomes leader or ctx is cancelled.
	Campaign(ctx context.Context, self Member) error
	// Resign voluntarily gives up leadership.
	Resign(ctx context.Context) error
	// Leader returns the current leader, and false if there is none.
	Leader(ctx context.Context) (Member, bool, error)
	// Observe streams the current leader on every leadership change until ctx
	// is cancelled.
	Observe(ctx context.Context) (<-chan Member, error)
	// Members lists every registered member.
	Members(ctx context.Context) ([]Member, error)
}

// EtcdDCS is the etcd-backed DCS.
//
// TODO: wire the real implementation using go.etcd.io/etcd/client/v3 and its
// concurrency package (concurrency.NewSession + concurrency.NewElection) for
// lease-based leader election under the key prefix /weft-ha/<cluster>/.
type EtcdDCS struct {
	endpoints []string
	cluster   string
	log       *slog.Logger
}

// NewEtcdDCS returns a DCS backed by the given etcd endpoints for one cluster.
func NewEtcdDCS(endpoints []string, cluster string, log *slog.Logger) *EtcdDCS {
	return &EtcdDCS{endpoints: endpoints, cluster: cluster, log: log}
}

func (d *EtcdDCS) Campaign(ctx context.Context, self Member) error {
	return ErrNotImplemented
}

func (d *EtcdDCS) Resign(ctx context.Context) error {
	return ErrNotImplemented
}

func (d *EtcdDCS) Leader(ctx context.Context) (Member, bool, error) {
	return Member{}, false, ErrNotImplemented
}

func (d *EtcdDCS) Observe(ctx context.Context) (<-chan Member, error) {
	return nil, ErrNotImplemented
}

func (d *EtcdDCS) Members(ctx context.Context) ([]Member, error) {
	return nil, ErrNotImplemented
}

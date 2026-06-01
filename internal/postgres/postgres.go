// Package postgres controls and inspects the local Postgres instance: its
// replication role and LSN, promotion and demotion, and the management of
// synchronous_standby_names.
package postgres

import (
	"context"
	"errors"
	"log/slog"
)

// ErrNotImplemented marks scaffold stubs that are not yet wired.
var ErrNotImplemented = errors.New("not implemented")

// Role is the replication role of a Postgres instance.
type Role int

const (
	// RoleUnknown means the role could not be determined (treat as not-primary).
	RoleUnknown Role = iota
	// RolePrimary is a read-write primary (pg_is_in_recovery() = false).
	RolePrimary
	// RoleReplica is a read-only standby (pg_is_in_recovery() = true).
	RoleReplica
)

// String renders the role for the API and logs.
func (r Role) String() string {
	switch r {
	case RolePrimary:
		return "primary"
	case RoleReplica:
		return "replica"
	default:
		return "unknown"
	}
}

// Controller manages a single local Postgres instance.
type Controller interface {
	// Role reports whether the instance is currently primary or replica.
	Role(ctx context.Context) (Role, error)
	// ReplayLSN reports the latest replayed WAL position, used to pick the
	// most-advanced standby during failover.
	ReplayLSN(ctx context.Context) (uint64, error)
	// Promote turns a standby into the primary.
	Promote(ctx context.Context) error
	// Demote reconfigures the instance to follow leaderConnURI as a standby.
	Demote(ctx context.Context, leaderConnURI string) error
	// ConfigureSyncStandbys sets synchronous_standby_names so commits wait for
	// at least one standby in another DC (RPO 0 on failover).
	ConfigureSyncStandbys(ctx context.Context, standbys []string) error
}

// LocalController drives Postgres over libpq and pg_ctl.
//
// TODO: wire the real implementation:
//   - Role:      SELECT pg_is_in_recovery()
//   - ReplayLSN: SELECT pg_last_wal_replay_lsn() (standby) / pg_current_wal_lsn() (primary)
//   - Promote:   pg_ctl promote (or pg_promote())
//   - Demote:    write standby.signal + primary_conninfo, restart
//   - ConfigureSyncStandbys: ALTER SYSTEM SET synchronous_standby_names + reload
type LocalController struct {
	connURI string
	log     *slog.Logger
}

// NewLocalController returns a Controller for the Postgres reachable at connURI.
func NewLocalController(connURI string, log *slog.Logger) *LocalController {
	return &LocalController{connURI: connURI, log: log}
}

func (c *LocalController) Role(ctx context.Context) (Role, error) {
	return RoleUnknown, ErrNotImplemented
}

func (c *LocalController) ReplayLSN(ctx context.Context) (uint64, error) {
	return 0, ErrNotImplemented
}

func (c *LocalController) Promote(ctx context.Context) error {
	return ErrNotImplemented
}

func (c *LocalController) Demote(ctx context.Context, leaderConnURI string) error {
	return ErrNotImplemented
}

func (c *LocalController) ConfigureSyncStandbys(ctx context.Context, standbys []string) error {
	return ErrNotImplemented
}

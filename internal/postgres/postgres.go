// Package postgres controls and inspects the local Postgres instance: its
// replication role and LSN, promotion and demotion, and the management of
// synchronous_standby_names.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotConnected is returned when the connection pool cannot be opened.
var ErrNotConnected = errors.New("postgres controller: not connected")

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

// LocalController drives Postgres over pgx. The pool is lazily created on
// first use so the reconcile loop survives a momentarily-down Postgres
// without crashing — the connection comes back on its own through the
// pool's reconnect logic.
type LocalController struct {
	connURI string
	log     *slog.Logger

	mu   sync.Mutex
	pool *pgxpool.Pool
}

// NewLocalController returns a Controller for the Postgres reachable at connURI.
func NewLocalController(connURI string, log *slog.Logger) *LocalController {
	return &LocalController{connURI: connURI, log: log}
}

// connect lazily creates a pgxpool. Pool config is intentionally small —
// the reconcile loop only opens 1-2 connections at a time, and we don't
// want stale-pool surprises after a Postgres restart.
func (c *LocalController) connect(ctx context.Context) (*pgxpool.Pool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pool != nil {
		return c.pool, nil
	}
	cfg, err := pgxpool.ParseConfig(c.connURI)
	if err != nil {
		return nil, fmt.Errorf("parse postgres uri: %w", err)
	}
	cfg.MaxConns = 4
	cfg.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotConnected, err)
	}
	c.pool = pool
	return pool, nil
}

// Close releases the connection pool. Safe to call multiple times.
func (c *LocalController) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pool != nil {
		c.pool.Close()
		c.pool = nil
	}
}

func (c *LocalController) Role(ctx context.Context) (Role, error) {
	pool, err := c.connect(ctx)
	if err != nil {
		return RoleUnknown, err
	}
	var inRecovery bool
	if err := pool.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return RoleUnknown, fmt.Errorf("pg_is_in_recovery: %w", err)
	}
	if inRecovery {
		return RoleReplica, nil
	}
	return RolePrimary, nil
}

// ReplayLSN returns the LSN as a uint64 by converting pg_lsn → numeric via
// pg_wal_lsn_diff. The standby reports its replay LSN ; the primary reports
// its current write LSN. We coalesce the two so callers can compare
// uniformly across roles when picking the most-advanced standby.
func (c *LocalController) ReplayLSN(ctx context.Context) (uint64, error) {
	pool, err := c.connect(ctx)
	if err != nil {
		return 0, err
	}
	const q = `
		SELECT COALESCE(
			pg_wal_lsn_diff(
				CASE WHEN pg_is_in_recovery()
					THEN pg_last_wal_replay_lsn()
					ELSE pg_current_wal_lsn()
				END,
				'0/0'::pg_lsn
			),
			0
		)::bigint
	`
	var lsn int64
	if err := pool.QueryRow(ctx, q).Scan(&lsn); err != nil {
		return 0, fmt.Errorf("replay lsn: %w", err)
	}
	if lsn < 0 {
		return 0, fmt.Errorf("replay lsn negative: %d", lsn)
	}
	return uint64(lsn), nil
}

// Promote calls pg_promote() with wait=true so the function only returns once
// Postgres has finished WAL recovery and switched out of standby mode. A 60 s
// internal timeout caps the wait — beyond that, the caller's ctx takes over.
func (c *LocalController) Promote(ctx context.Context) error {
	pool, err := c.connect(ctx)
	if err != nil {
		return err
	}
	var ok bool
	if err := pool.QueryRow(ctx, "SELECT pg_promote(wait => true, wait_seconds => 60)").Scan(&ok); err != nil {
		return fmt.Errorf("pg_promote: %w", err)
	}
	if !ok {
		return errors.New("pg_promote returned false (timeout or non-standby state)")
	}
	c.log.Info("postgres promoted to primary")
	return nil
}

// Demote points the local instance at leaderConnURI as a streaming-replication
// source. The image's entrypoint owns the cold-start standby.signal dance ;
// for an already-running standby we just ALTER SYSTEM the connection so the
// next walreceiver reconnect picks up the new upstream.
//
// We don't restart Postgres here — the openweft postgres-ha image's entrypoint
// owns the lifecycle. The signal to restart is the next reconcile tick
// noticing a misaligned primary_conninfo.
func (c *LocalController) Demote(ctx context.Context, leaderConnURI string) error {
	pool, err := c.connect(ctx)
	if err != nil {
		return err
	}
	// Escape single quotes in the conninfo so the SQL stays well-formed
	// even when the operator's password contains them.
	safe := strings.ReplaceAll(leaderConnURI, "'", "''")
	stmt := fmt.Sprintf("ALTER SYSTEM SET primary_conninfo = '%s'", safe)
	if _, err := pool.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("alter system primary_conninfo: %w", err)
	}
	if _, err := pool.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		return fmt.Errorf("pg_reload_conf: %w", err)
	}
	c.log.Info("postgres demoted to replica", "upstream", leaderConnURI)
	return nil
}

// ConfigureSyncStandbys updates synchronous_standby_names atomically. We use
// "ANY 1 (s1,s2,...)" semantics so a commit waits for ACK from any one of the
// listed standbys — the safest minimum that gives RPO 0 on failure of the
// primary's local DC.
func (c *LocalController) ConfigureSyncStandbys(ctx context.Context, standbys []string) error {
	pool, err := c.connect(ctx)
	if err != nil {
		return err
	}
	var setting string
	if len(standbys) == 0 {
		setting = "" // sync replication disabled
	} else {
		setting = fmt.Sprintf("ANY 1 (%s)", strings.Join(standbys, ","))
	}
	safe := strings.ReplaceAll(setting, "'", "''")
	stmt := fmt.Sprintf("ALTER SYSTEM SET synchronous_standby_names = '%s'", safe)
	if _, err := pool.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("alter system synchronous_standby_names: %w", err)
	}
	if _, err := pool.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		return fmt.Errorf("pg_reload_conf: %w", err)
	}
	c.log.Info("synchronous_standby_names updated", "count", len(standbys), "value", setting)
	return nil
}

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `internal/api.handleHealth` now emits the IETF Health Check Response Format
  vocabulary (`status: pass | fail`) alongside the openweft `role` extension.
  Previously the endpoint only emitted `{role}`, which forced router /
  dashboard code to special-case postgres-ha against the forgejo-ha and
  irods-ha siblings. Regression guard : `TestHealthIETFVocabulary`.
- `internal/dcs.EtcdDCS.connect` detects a dead `concurrency.Session`
  (lease expired or partitioned away from the etcd quorum) and rebuilds it.
  Before, a cached corpse-session would persist across the partition and
  the agent never re-campaigned after recovery.
- `internal/reconcile.Reconciler.step` now updates
  `metrics.NodeRole{node}` every tick and bumps `metrics.FailoversTotal`
  on a successful promotion. The metrics package previously registered both
  vectors with `prometheus.MustRegister` but no code ever wrote to them.

## [v0.2.0-rc1] — 2026-06-07

### Added

- **Real HA reconcile loop** (replaces the previous scaffold that returned
  `ErrNotImplemented` everywhere). The agent is now operationally complete :
  - `internal/postgres.LocalController` drives Postgres over pgx v5 :
    `pg_is_in_recovery()` for role detection, `pg_wal_lsn_diff` for
    LSN comparison, `pg_promote(wait=>true)` for failover, ALTER SYSTEM
    + `pg_reload_conf()` for `primary_conninfo` and
    `synchronous_standby_names`.
  - `internal/dcs.EtcdDCS` implements the DCS interface via
    `concurrency.NewSession` + `concurrency.NewElection`. Members
    announce themselves under the session's lease so a fenced node
    drops out automatically. Configurable lease TTL (default 15 s).
  - `internal/fencing.VMFencer` calls `weft-agent.StopVM` via gRPC,
    then polls `VMStatus` until the agent reports a stopped state
    (STOPPED / NOT_CREATED / ERROR). A timeout MUST block promotion
    — never invents "probably stopped". `NoopFencer` available for
    unit tests only ; loud warning when used at runtime.
  - `internal/reconcile.Reconciler.step()` implements the safe state
    machine : observe → announce → dispatch on (HasLeader, IsLeaderUs,
    LocalRole). leader branch reconciles `synchronous_standby_names`
    against off-DC members ; follower branch nudges `primary_conninfo`
    and demotes on split-brain ; no-leader branch fences every peer
    before promoting, refuses to promote if any fence fails.
  - 7 reconcile_test cases covering leader-path sync standbys,
    follower-demote-on-split-brain, no-leader-fences-then-promotes,
    fence-failure-blocks-promotion, lex-tie-break, single-node cluster.

### Changed

- `Config` gains `WeftEndpoint`, `WeftProject`, `EtcdSessionTTLSec`,
  `FenceTimeout`. Defaults : 30 s fence timeout, 15 s lease TTL.
- `postgres.ErrNotImplemented` → `postgres.ErrNotConnected` (the new
  pool-creation failure path).

### Removed

- All `ErrNotImplemented` returns. The package previously shipped as a
  bare scaffold ; this release implements the contract end-to-end.

### Wired by

- New consumer : `weft/catalogue/postgres-ha/plugin.hcl` v2 (replaces
  the Patroni v1 layout). The catalogue plugin ships
  `ghcr.io/openweft/postgres-ha:v0.2.0` which bundles Postgres + this
  agent in one rootfs. Caddy in `weft-agent` active-health-checks each
  replica's `:8008/primary` and routes 5432 traffic to whichever returns
  200 — automatic failover with zero operator intervention.

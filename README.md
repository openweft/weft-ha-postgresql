<p align="center"><img src="https://raw.githubusercontent.com/openweft/brand/main/social/openweft.png" alt="openweft" width="720"></p>

# weft-ha-postgresql

Go-native PostgreSQL high-availability operator for openweft. Runs as a
per-node agent (one alongside each Postgres micro-VM), elects a leader through
etcd, drives synchronous replication, and performs **fenced** failover so a
whole datacenter can be lost without data loss.

**Status: operational.** The four packages that were scaffold returns
in v0.1 now ship a complete implementation :

- `internal/postgres.LocalController` drives Postgres over **pgx v5** :
  `pg_is_in_recovery()` for role, `pg_wal_lsn_diff` for LSN,
  `pg_promote(wait=>true)` for failover, ALTER SYSTEM + `pg_reload_conf()`
  for `primary_conninfo` and `synchronous_standby_names`.
- `internal/dcs.EtcdDCS` uses `concurrency.NewSession` +
  `concurrency.NewElection` for lease-based leader election ; members
  announce under the session's lease so a fenced node drops out of
  `Members()` automatically.
- `internal/fencing.VMFencer` calls `weft-agent.StopVM` via gRPC, then
  polls `VMStatus` until the agent reports a confirmed-stopped state
  (STOPPED / NOT_CREATED / ERROR). A timeout **blocks promotion** —
  never invents "probably stopped".
- `internal/reconcile.Reconciler.step()` implements the safe state
  machine : observe → announce → dispatch on (HasLeader, IsLeaderUs,
  LocalRole). Leader branch reconciles `synchronous_standby_names`
  against off-DC members ; follower branch nudges `primary_conninfo`
  and demotes on split-brain ; no-leader branch **fences every peer
  before promoting** and refuses to promote if any fence fails.

12 tests across the four packages + 5 integration tests against an
embedded etcd (`internal/dcs/dcs_integration_test.go`) all pass. See
the CHANGELOG `[Unreleased]` block for the per-component details.

## Why this exists

openweft's datastore decision is **PostgreSQL + synchronous replication +
etcd** for HA that survives the loss of a whole DC (RPO 0). The NewSQL options
(CockroachDB, TiKV/TiDB) were rejected: they refuse to build from source in the
pkgx pantry (embedded C++ storage engine + Bazel/Cargo), and CockroachDB's
licence forbids offering it as a service.

The established orchestrator for this topology is Patroni — but it is Python,
and there is no maintained, Go-native, non-Kubernetes equivalent (Stolon is
dormant; CloudNativePG is Kubernetes-only). So openweft grows its own, in Go,
with one structural advantage no general-purpose tool has:

> **Real fencing.** weft owns the substrate, so it can *hard-stop the
> micro-VM* of a misbehaving node (true STONITH) instead of trusting a
> cooperative watchdog. Reliable fencing is exactly the hard part of safe
> failover, and it is the part weft is uniquely positioned to get right.

## How it fits in

```
                    etcd (3-DC quorum, leader lease + membership)
                              ▲           ▲
                  Campaign /  │           │  Observe
                  Resign      │           │
        +---------------------┴--+   +-----┴------------------+
        |  weft-ha-postgresql    |   |  weft-ha-postgresql    |   ... one per node
        |  agent  (DC1, primary) |   |  agent  (DC2, replica) |
        |   ├─ postgres ctrl     |   |   ├─ postgres ctrl     |
        |   ├─ role API :8008    |   |   ├─ role API :8008    |
        |   └─ fencer (weft API) |   |   └─ fencer (weft API) |
        +-----------┬------------+   +------------------------+
                    │ GET /primary -> 200 only on the leader
                    ▼
            +-----------------+
            |  SQL router     |   Caddy (caddy-l4) + an HTTP active health
            |  (caddy-l4)     |   check against /primary; routes clients to
            +-----------------+   the single current primary.
```

The role API contract mirrors Patroni so existing tooling and the router work
unchanged:

| Endpoint    | 200 when…        | otherwise |
| ----------- | ---------------- | --------- |
| `GET /primary` | node is the primary | 503 |
| `GET /replica` | node is a replica   | 503 |
| `GET /health`  | always (reports role) | — |

The companion routing change lives in
[`tannevaled/caddy-l4`](https://github.com/tannevaled/caddy-l4): an active HTTP
health check (`path` + `expected_status`) so caddy-l4 can follow `/primary`
natively, upstreamed to [`mholt/caddy-l4`](https://github.com/mholt/caddy-l4).

## Layout

```
cmd/weft-ha-postgresql/   cobra root + `agent` subcommand
internal/config/          bootstrap config + validation
internal/dcs/             distributed config store (etcd): leader election, membership
internal/postgres/        local Postgres control: role, LSN, promote/demote, sync standbys
internal/fencing/         STONITH: hard-stop the node's micro-VM via the weft API
internal/reconcile/       the HA state machine (observe → decide → act)
internal/api/             role API the SQL router probes (/primary, /replica, /health)
internal/metrics/         Prometheus /metrics on a dedicated port
```

## Build

```sh
# Host build (dev)
pkgx task build

# Cross-build the binaries the micro-VM runs (linux/arm64 + linux/amd64)
pkgx task build-linux
```

## Run

```sh
weft-ha-postgresql agent \
    --node-name pg-dc1-0 \
    --cluster-name tenant-acme \
    --dc dc1 \
    --etcd https://etcd-dc1:2379,https://etcd-dc2:2379,https://etcd-dc3:2379 \
    --postgres-uri "postgres:///postgres?host=/var/run/postgresql" \
    --api-addr :8008 \
    --metrics-addr :9101
```

## License

BSD 3-Clause — see [LICENSE](LICENSE). Same license as the rest of openweft.

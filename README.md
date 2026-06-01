# weft-ha-postgresql

Go-native PostgreSQL high-availability operator for openweft. Runs as a
per-node agent (one alongside each Postgres micro-VM), elects a leader through
etcd, drives synchronous replication, and performs **fenced** failover so a
whole datacenter can be lost without data loss.

**Status: scaffolding.** The lifecycle wiring (cobra root, `agent`
subcommand, role API, Prometheus metrics, reconcile loop, DCS / Postgres /
fencing interfaces) is in place; the safety-critical bits — the failover state
machine, the etcd leader election, the libpq/`pg_ctl` control, and the weft-API
fence call — are stubbed with `// TODO:` markers so the daemon builds and tests
green while the rest is wired.

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

# Orbix — Resilient Distributed Job Scheduler

**TECH26-045 · Arun & Mohammad Kaif Iqbal**

A leader-worker cron scheduler in Go on etcd, where every node runs the same
binary: REST API + worker loop, and the elected leader additionally runs the
scheduler tick. Kill any process at any moment — fires are never lost and
execution migrates to surviving peers within seconds.

Open any node in a browser for the control plane: live cluster ledger, job
console with cron preview, queue depth, and full run history with stdout/stderr.

## Run it (Docker, 3 nodes + etcd)

```bash
cd orbix-scheduler/deploy
docker compose up -d --build
./../scripts/seed.sh              # Windows: powershell -File ../scripts/seed.ps1
# dashboard: http://localhost:8081 (every node serves it; 8082, 8083 too)
```

## Chaos test (the whole point)

```bash
docker compose kill node2   # worker dies mid-run  -> ticket re-claimed elsewhere, attempt+1
docker compose kill node1   # leader dies           -> atomic re-election, schedule resumes
docker compose up -d --force-recreate node1 node2
docker compose ps           # watch the ledger keep moving
```

## API

| Method | Route | Notes |
|---|---|---|
| GET | `/api/status` | node, leader, members, queue depth, run counts |
| GET/POST | `/api/jobs` | list / create (validated cron, timezone, timeouts) |
| GET/PUT/DELETE | `/api/jobs/{id}` | read / replace / delete |
| POST | `/api/jobs/{id}/enable`, `/disable`, `/trigger` | pause, resume, run-now |
| GET | `/api/jobs/{id}/runs`, `/api/runs` | history from local SQLite |
| GET | `/api/queue` | pending tickets + in-flight claims |
| GET | `/api/cron/preview?expr=..&tz=..&n=5` | next fire times (powers the form hint) |
| GET | `/api/nodes` | etcd members merged with Docker state (leader, host port) |
| POST | `/api/nodes` | spawn a node (`{"node_id"}` optional, else `node<N>`) |
| POST | `/api/nodes/{id}/kill` | SIGKILL a node; it stays down (graceful self-leave without socket) |
| POST | `/api/nodes/{id}/start` | (re)start an exited node container |
| GET | `/healthz`, `/readyz` | liveness / etcd+SQLite readiness (compose probes) |
| GET | `/metrics` | Prometheus exposition, zero extra deps |

Example — a job that fires every 5 seconds:

```bash
curl -X POST localhost:8081/api/jobs -H 'content-type: application/json' -d '{
  "name": "heartbeat", "cron": "*/5 * * * * *",
  "command": "sleep 8 && echo alive", "max_retries": 3
}'
```## Local development (single node + etcd in Docker)

```bash
cd orbix-scheduler/deploy && docker compose up -d etcd && cd ..
go build ./... && go test ./...
go run ./cmd/orbixd            # needs etcd on :2379; config: orbix.yaml <- ORBIX_* env
node scripts/ui-check.js       # dashboard/REST contract check
```

Configuration (`orbix.yaml`, every field has an `ORBIX_*` override —
`ORBIX_NODE_ID`, `ORBIX_ETCD`, `ORBIX_LISTEN`, `ORBIX_DATA_DIR`,
`ORBIX_CATCHUP`, …): election TTL (leader failover time), worker lease
(crash-migration time), tick/claim intervals, catch-up policy, concurrency.
See `internal/config/config.go`.

## Why it is resilient

| Failure | Mechanism | Recovery |
|---|---|---|
| Worker dies mid-run | Claim rides a short lease; the ticket never left the queue | Any peer re-claims (`attempt+1`) |
| Leader dies | Lease-backed leader key evaporates with its session | Followers re-elect atomically |
| Leader enqueues twice | Put-if-not-exists on the ticket key | Duplicate is a no-op |
| Missed fires (leader gap) | Per-job watermark + `run_latest`/`run_all`/`skip` | New leader replays the gap |
| Partitioned zombie leader | Etcd lease fencing: it cannot commit anything | Split-brain impossible |
| Slow worker, lost lease | Completion re-verifies claim ownership | Replacement run never corrupted |

## Layout

```
cmd/orbixd/          single binary; embeds the web console (no build step)
cmd/orbixd/web/      dashboard + product page: index.html, app.css, app.js
internal/cron/       Vixie-style parser: 5/6 fields, macros, timezones + tests
internal/cluster/    etcd session, membership, leader election
internal/scheduler/  leader tick loop, watermark, catch-up
internal/queue/      tickets, lease-guarded claims, retries, overlap locks
internal/worker/     execution loop with bounded concurrency + drain
internal/executor/   shell exec with timeout, captured stdout/stderr/exit code
internal/history/    results watcher -> local SQLite (WAL)
internal/store/      SQLite schema + queries
internal/api/        REST + /healthz, /readyz, /metrics, cron preview
deploy/              Dockerfile (static Go -> alpine) + 3-node compose demo
scripts/             seed.sh / seed.ps1 demo jobs, ui-check.js contract test
```

## Semantics

Fires are **at-least-once**: a run may execute twice if a worker dies after
completing but before reporting (the tiny window the claim lease guards).
Enqueue itself is at-most-once via idempotent ticket keys — the
industry-standard trade-off (same as Airflow, Temporal, Kubernetes CronJobs).

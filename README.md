# pg-durable-queue

A durable background-job queue built only on PostgreSQL, written in TypeScript.

A REST API enqueues jobs. Worker processes claim them with `FOR UPDATE SKIP LOCKED`, run them
and record the outcome. Every job runs at least once, even when workers crash mid-job.
Idempotency keys make enqueueing and side effects effectively once.

**Measured results:** [RESULTS.md](RESULTS.md). In short, on an 8-core laptop:

- **Crash test:** 1,000,000 jobs across 20 runs with 530 `SIGKILL`ed workers: **0 lost jobs,
  0 duplicate side effects.**
- **Throughput:** up to **12,632 jobs/s** (4 workers, no-op jobs, median of 3 runs).
  `SKIP LOCKED` drained **1.9× faster** than naive `FOR UPDATE` at 16 workers.
- **Latency:** at **2,000 enqueues/s** sustained for 60 s, job wait p99 was **19 ms**, with
  0 errors.

## Why

Most services need background jobs (emails, billing runs, video processing) that must not be
lost if a worker crashes and must not run twice when retried. Many teams already run Postgres,
so a queue that needs no extra infrastructure (no Redis, no SQS) is a real trade-off worth
understanding: simpler operations, in exchange for a throughput ceiling.

## Goals

- **At-least-once delivery.** A job is never lost, even if a worker dies mid-job.
- **Effectively-once side effects** through idempotency keys.
- **No double-claims** under concurrent workers.
- **Observable:** queue depth, latency and retries visible on a dashboard.
- **Measured:** throughput and tail latency from repeatable load tests.

## Architecture

```
 client ──REST──► API (Fastify) ──INSERT + NOTIFY──► jobs table (Postgres)
                                                        ▲      │ NOTIFY
                                                        │      ▼
                 workers (N processes) ◄──LISTEN────────┘   claim with
                 heartbeat / complete / fail ──────────────► FOR UPDATE SKIP LOCKED
                 reaper: re-queue expired locks              │
                                       status = 'dead' ◄─────┘ (max attempts reached)
                 /metrics (API + each worker) ──► Prometheus ──► Grafana
```

| File | What it does |
|---|---|
| [`migrations/001_jobs.sql`](migrations/001_jobs.sql) | `jobs` table, partial indexes for ready and running jobs, `job_effects` |
| [`src/queue.ts`](src/queue.ts) | `enqueue`, `claim`, `complete`, `fail`, `heartbeat`, `reap`: one SQL statement each |
| [`src/worker.ts`](src/worker.ts) | Worker loop: concurrency slots, LISTEN/NOTIFY plus 1 s poll, heartbeats, reaper, graceful shutdown |
| [`src/api.ts`](src/api.ts) | `POST /jobs`, `GET /jobs/:id`, `GET /metrics`, `GET /health` |
| [`src/metrics.ts`](src/metrics.ts) | `queue_depth`, `job_wait_seconds`, `job_duration_seconds`, `jobs_total`, `reaped_jobs_total` |
| [`src/migrate.ts`](src/migrate.ts) | Applies `migrations/*.sql` once each, under an advisory lock |
| [`bench/`](bench/) | Crash test, drain benchmark, k6 steady-state load test |

### Claiming a job

```sql
UPDATE jobs SET status = 'running', locked_by = $1, locked_until = now() + $2::interval,
       attempts = attempts + 1, started_at = COALESCE(started_at, now())
WHERE id = (SELECT id FROM jobs
            WHERE queue = $3 AND status = 'queued' AND run_at <= now()
            ORDER BY priority DESC, run_at
            FOR UPDATE SKIP LOCKED LIMIT 1)
RETURNING *;
```

`SKIP LOCKED` lets many workers claim at once without blocking each other or claiming the same
row. The partial index `(queue, priority DESC, run_at) WHERE status = 'queued'` holds only
ready jobs, in exactly the order this query reads them.

### How a job survives a crash

1. The claim sets `locked_until = now() + visibility timeout`.
2. While the handler runs, the worker extends `locked_until` every timeout/3 (heartbeat).
3. If the worker dies, heartbeats stop. Once `locked_until` passes, the reaper in any worker
   puts the job back to `queued`, and another worker runs it.
4. `complete` and `fail` only apply if `locked_by` still matches, so a stalled worker can't
   overwrite a job someone else now owns.
5. A re-run may repeat a side effect, so side effects must be idempotent (the `write-effect`
   handler uses `INSERT ... ON CONFLICT DO NOTHING` keyed by job id).

Failures retry with exponential backoff and full jitter (1 s base, 5 min cap). After
`max_attempts`, a job moves to `status = 'dead'`.

## How to run

Needs Node.js 24 and PostgreSQL. Node 24 runs the TypeScript sources directly, so there's no
build step.

```bash
npm install
cp .env.example .env          # DATABASE_URL and TEST_DATABASE_URL
createdb pgq && createdb pgq_test
npm run migrate
```

Start the API and a worker in two terminals:

```bash
node src/api.ts               # http://localhost:3000
CONCURRENCY=4 node src/worker.ts
```

Enqueue a job and check on it:

```bash
curl -X POST localhost:3000/jobs -H 'content-type: application/json' \
  -d '{"type": "sleep", "payload": {"ms": 100}, "idempotencyKey": "demo-1"}'
curl localhost:3000/jobs/1
```

Worker settings (environment variables): `QUEUE` (default `default`), `CONCURRENCY` (4),
`VISIBILITY_MS` (30000), `SHUTDOWN_TIMEOUT_MS` (30000), `CLAIM_MODE` (`skip_locked` or
`naive`), `METRICS_PORT` (off unless set). API settings: `PORT` (3000), `PG_POOL_MAX` (10),
`LOG_LEVEL` (`info`).

### Tests

```bash
npm test                      # 43 tests against TEST_DATABASE_URL (tables are truncated)
npm run typecheck
```

### Benchmarks

These use `DATABASE_URL` and **truncate its tables**. The 16-worker cases need
`max_connections` of about 150 or more. k6 is needed for the steady-state run.

```bash
npm run bench:crash           # crash test, 5 runs
npm run bench:drain           # drain throughput and latency, 3 runs per case
npm run bench:steady          # k6 against the API at 200 to 6,000 req/s
```

### Full stack with Docker

```bash
docker compose up --build
```

This starts Postgres (host port 5433), the API (3000), two workers, Prometheus (9090) and
Grafana (3001, dashboard "pg-durable-queue", login admin/admin). CI includes a smoke test for
this stack, but it hasn't run yet (see [CI status](#ci-status)).

## Build plan

- [x] **Core.** Schema and migrations; enqueue API with idempotency keys; worker loop with
  claim, complete and fail; retries with exponential backoff plus jitter; dead-letter after
  `max_attempts`.
- [x] **Recovery.** Heartbeats extend `locked_until`; a reaper re-queues jobs whose lock expired
  (crashed worker); graceful shutdown on SIGTERM.
- [x] **Tests.** 43 integration tests against real Postgres; a crash test that `kill -9`s
  workers mid-job: 0 lost jobs across 20 runs.
- [x] **Observability.** Prometheus metrics (queue depth, wait and duration histograms, outcomes,
  reaped jobs); Grafana dashboard; LISTEN/NOTIFY wake-ups with polling fallback.
- [x] **Benchmarks.** Drain benchmark at 1/4/16 workers; k6 steady-state load test; `SKIP LOCKED`
  vs naive `FOR UPDATE`. Write-up in [RESULTS.md](RESULTS.md).
- [ ] **CI verified.** The workflow and the docker-compose smoke test are written but haven't run
  yet (see below).
- [ ] **Later: deploy.** Terraform on AWS (ECS Fargate + RDS), blue-green deploys, autoscaling on
  queue depth, OpenTelemetry tracing.

## CI status

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs the type check and tests against
`postgres:16`, then starts the docker-compose stack and checks that a job goes through the API
and a worker, that Prometheus scrapes all three targets, and that Grafana has the dashboard.
GitHub hasn't started these jobs yet because of an account billing lock (support ticket open),
so neither has passed in CI so far. The tests pass locally against Postgres 14.

## Interview talking points

- Why at-least-once and not exactly-once, and how idempotency keys close the gap.
- What happens if a worker stalls but doesn't die (visibility timeout vs. heartbeat): crash-test
  run 3 in [RESULTS.md](RESULTS.md) shows a real example.
- Why throughput peaks at 4 workers on this laptop and drops at 16: lock contention at the head
  of the queue, measured with `pg_stat_activity`.
- Where Postgres stops scaling as a queue (vacuum pressure from updates, index bloat, lock
  contention), and when you'd move to SQS or Kafka.
- Why the partial index matters for the claim query.

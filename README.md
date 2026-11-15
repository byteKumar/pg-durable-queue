# pg-durable-queue

A durable background-job queue built only on PostgreSQL, written in TypeScript.

> **Status: in progress.** This README is the design. Sections are checked off as they're built, and benchmark numbers are added only after they're measured.

## Why

Most services need background jobs (emails, billing runs, video processing) that must not be lost if a worker crashes and must not run twice when retried. Many teams already run Postgres, so a queue that needs no extra infrastructure (no Redis, no SQS) is a real trade-off worth understanding: simpler operations against a throughput ceiling.

## Goals

- **At-least-once delivery.** A job is never lost, even if a worker dies mid-job.
- **Effectively-once side effects** through idempotency keys.
- **No double-claims** under concurrent workers.
- **Observable:** queue depth, latency and retries visible on a dashboard.
- **Measured:** throughput and tail latency from repeatable load tests.

## Architecture

```
 client ──REST──► API (Fastify/NestJS) ──INSERT──► jobs table (Postgres)
                                                    ▲      │ NOTIFY
                                                    │      ▼
                 workers (N processes) ◄──LISTEN────┘   claim with
                 heartbeat / complete / fail ──────────► FOR UPDATE SKIP LOCKED
                                                         │
                                       dead_letter ◄─────┘ (max attempts reached)
                 /metrics ──► Prometheus ──► Grafana
```

### Schema (first draft)

```sql
CREATE TABLE jobs (
  id              bigserial PRIMARY KEY,
  queue           text        NOT NULL,
  payload         jsonb       NOT NULL,
  idempotency_key text,
  priority        int         NOT NULL DEFAULT 0,
  run_at          timestamptz NOT NULL DEFAULT now(),
  attempts        int         NOT NULL DEFAULT 0,
  max_attempts    int         NOT NULL DEFAULT 5,
  locked_by       text,
  locked_until    timestamptz,          -- visibility timeout
  status          text        NOT NULL DEFAULT 'queued',  -- queued | running | done | dead
  last_error      text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (queue, idempotency_key)
);
CREATE INDEX jobs_ready ON jobs (queue, priority DESC, run_at)
  WHERE status = 'queued';
```

### Claiming a job

```sql
UPDATE jobs SET status = 'running', locked_by = $1,
       locked_until = now() + $2::interval, attempts = attempts + 1
WHERE id = (
  SELECT id FROM jobs
  WHERE queue = $3 AND status = 'queued' AND run_at <= now()
  ORDER BY priority DESC, run_at
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING *;
```

`SKIP LOCKED` lets many workers poll at once without blocking each other or claiming the same row.

## Build plan

- [ ] **Week 1: core.** Schema and migrations; enqueue API with idempotency keys; worker loop with claim, complete and fail; retries with exponential backoff plus jitter; dead-letter after `max_attempts`.
- [ ] **Week 1: recovery.** Heartbeats extend `locked_until`; a reaper re-queues jobs whose lock expired (crashed worker); graceful shutdown on SIGTERM.
- [ ] **Week 2: tests.** Integration tests against real Postgres (Testcontainers) in GitHub Actions; a crash test that `kill -9`s workers mid-job and asserts zero lost jobs across N runs.
- [ ] **Week 2: observability.** Prometheus metrics (queue depth, job latency p50/p95/p99, retry rate, dead-letter count); Grafana dashboard; LISTEN/NOTIFY wakeups with polling fallback.
- [ ] **Week 3: benchmarks.** k6 load tests at 1/4/16 workers; compare `SKIP LOCKED` against a naive `FOR UPDATE` claim; write up results and trade-offs below.
- [ ] **Later: deploy (Project A+).** Terraform on AWS (ECS Fargate + RDS), blue-green deploys, autoscaling on queue depth, OpenTelemetry tracing.

## Results

_Not measured yet._ This section gets throughput, p99 latency, crash-test results and the SKIP LOCKED comparison once they exist. Only then do resume bullets get numbers.

## Interview talking points to be ready for

- Why at-least-once and not exactly-once, and how idempotency keys close the gap.
- What happens if a worker stalls but doesn't die (visibility timeout vs. heartbeat).
- Where Postgres stops scaling as a queue (vacuum pressure from updates, index bloat), and when you'd move to SQS or Kafka.
- Why the partial index matters for the claim query.

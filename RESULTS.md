# Results

Every number here was measured on one laptop and comes from the raw output files in
[`bench/results/`](bench/results/). Nothing is estimated or rounded up. Where a result was
noisy or surprising, this file says so.

## Machine

| | |
|---|---|
| Chip | Apple M3, 8 cores |
| RAM | 16 GB |
| PostgreSQL | 14.17 (Homebrew), `max_connections = 200` (raised from 100 for the 16-worker runs), `shared_buffers = 128MB` (default) |
| Node.js | 24.20.0 |
| Load generator | k6 2.4.0 |

Postgres, the API, the workers and k6 all ran on this same machine, so they compete for the
same 8 cores. CI uses Postgres 16. The local numbers are from Postgres 14.

## Tests

**43 tests, all passing** (`npm test`, Vitest against a real Postgres database).

They cover idempotent enqueue (including 20 concurrent requests with one key), 50 concurrent
claims on 20 jobs (each claimed exactly once), priority and delayed jobs, retries with backoff
and dead-lettering, lock-holder checks, heartbeats, the reaper, graceful shutdown (in-process
and a real `SIGTERM` to a worker process), API validation, and metrics.

Two tests were checked by breaking the code on purpose. With heartbeats disabled, the heartbeat
test fails: the job is reaped 3 times, so it starts 4 times. With NOTIFY ignored, the wake-up
test times out.

## Crash test

[`bench/crash-test.ts`](bench/crash-test.ts), raw output in
[`bench/results/crash-test.txt`](bench/results/crash-test.txt).

**Setup:** 50,000 `write-effect` jobs (insert a side-effect row, then sleep 5–50 ms), 8 worker
processes × 4 concurrency, 5 s visibility timeout. Every 1–3 s a random worker is `SIGKILL`ed
and a replacement starts at once, until the queue drains.

The spec says 10,000 jobs, but on this machine 10,000 drain in about 14 s, which allows only
about 6 kills at one kill every 1–3 s. 40,000 jobs still gave one run with 19 kills. 50,000
jobs gives the ≥ 20 kills the spec asks for, with every other setting unchanged.

| run | kills | not done | lost jobs | jobs run > 1 time | conflicts absorbed | wall time |
|---|---|---|---|---|---|---|
| 1 | 24 | 0 | 0 | 88 | 86 | 51.72 s |
| 2 | 27 | 0 | 0 | 96 | 94 | 50.70 s |
| 3 | 49 | 0 | 0 | 165 | 110 | 92.87 s |
| 4 | 27 | 0 | 0 | 94 | 93 | 51.42 s |
| 5 | 23 | 0 | 0 | 83 | 74 | 49.67 s |

- Every run ended with exactly 50,000 `job_effects` rows: no job finished without its side
  effect, and no side effect happened twice.
- "Conflicts absorbed" counts duplicate runs that found the effect row already written. It is
  counted from worker logs and cross-checked against the database, and the two agree. It can
  be lower than "jobs run > 1 time" because a job killed *before* writing its effect re-runs
  without a conflict.
- **15 more runs** with the same settings
  ([`crash-test-extra-runs.txt`](bench/results/crash-test-extra-runs.txt)) also had 0 lost
  and 0 not done. Across all 20 saved runs: **1,000,000 jobs, 530 `SIGKILL`s, 0 lost jobs,
  0 duplicate side effects.**

## Drain throughput and latency

[`bench/drain.ts`](bench/drain.ts), raw output in [`bench/results/drain.txt`](bench/results/drain.txt).

50,000 pre-loaded jobs, `CONCURRENCY=4` per worker process. Each case ran **3 times**, with the
cases interleaved (all cases, then all again). The table shows the median of the 3 runs for each
column. jobs/s = 50,000 ÷ time from launching the workers until every job is done, so worker
start-up is included.

| workers | handler | jobs/s (median) | jobs/s range | wait p50 / p95 / p99 (ms) | end-to-end p50 / p95 / p99 (ms) |
|---|---|---|---|---|---|
| 1 | noop | 10,445 | 10,266–10,564 | 2,815.5 / 4,881.5 / 5,064.5 | 2,815.7 / 4,881.7 / 5,064.8 |
| 4 | noop | 12,632 | 12,485–12,874 | 2,428.5 / 4,027.9 / 4,156.1 | 2,439.3 / 4,028.6 / 4,156.5 |
| 16 | noop | 4,596 | 4,147–6,187 | 5,710.6 / 10,416.3 / 10,948.2 | 5,715.0 / 10,421.9 / 10,951.3 |
| 1 | sleep 10 ms | 285 | 284–292 | 86,199.2 / 167,174.7 / 174,145.3 | 86,211.9 / 167,187.3 / 174,157.2 |
| 4 | sleep 10 ms | 1,169 | 1,147–1,185 | 21,463.5 / 41,049.0 / 42,712.4 | 21,474.7 / 41,063.9 / 42,724.8 |
| 16 | sleep 10 ms | 4,895 | 4,712–5,126 | 5,624.9 / 9,950.3 / 10,361.4 | 5,636.0 / 9,960.7 / 10,384.5 |

The jobs are pre-loaded, so wait latency here mostly measures **backlog**: the last job waits
for the 49,999 ahead of it. The latency to quote is the steady-state one below.

## Steady-state latency under load

[`bench/steady-state.ts`](bench/steady-state.ts) and [`bench/enqueue.k6.js`](bench/enqueue.k6.js),
raw output in [`steady-state.txt`](bench/results/steady-state.txt) and
[`steady-state-high.txt`](bench/results/steady-state-high.txt), k6 summaries in
[`bench/results/k6/`](bench/results/k6/).

One API process (`LOG_LEVEL=warn`, default pool of 10 connections) plus 8 workers × 4 (`noop`).
k6 `constant-arrival-rate` sends `POST /jobs` for 60 s per rate, after a 10 s warm-up at
100 req/s that isn't reported. The tables were truncated before each rate, so each row's
latencies cover exactly the jobs from that run.

| rate (req/s) | requests | achieved req/s | enqueue p95 / p99 (ms) | errors | k6 couldn't send | wait p50 / p95 / p99 (ms) |
|---|---|---|---|---|---|---|
| 200 | 11,996 | 199.9 | 4.14 / 32.20 | 0% | 4 | 1.0 / 2.8 / 17.6 |
| 500 | 29,818 | 497.0 | 1.80 / 143.43 | 0% | 182 | 0.4 / 1.0 / 15.9 |
| 1,000 | 59,997 | 999.9 | 1.98 / 58.41 | 0% | 4 | 0.2 / 1.2 / 8.7 |
| 2,000 | 118,802 | 1,979.9 | 77.27 / 265.03 | 0% | 1,198 | 0.2 / 4.9 / 19.0 |
| 4,000 | 143,870 | 2,345.1 | 2,522.36 / 2,737.83 | 0% | 96,130 | 19.1 / 363.2 / 672.9 |
| 6,000 | 102,314 | 1,343.8 | 10,928.96 / 17,856.12 | 0% | 257,688 | 6.7 / 62.9 / 109.5 |

**Highest tested rate with job wait p99 < 100 ms: 2,000 req/s** (p99 19.0 ms). The spec asked
for 3 rates. The first three passed easily, so 2,000, 4,000 and 6,000 were added to find the
limit.

## SKIP LOCKED vs naive FOR UPDATE

Same drain benchmark: 16 workers, `noop`, `CLAIM_MODE=naive` vs the default. 3 runs each,
medians.

| mode | jobs/s (median) | jobs/s range | wait p99 (ms) |
|---|---|---|---|
| skip_locked | 4,596 | 4,147–6,187 | 10,948.2 |
| naive | 2,404 | 1,317–2,840 | 19,891.4 |

**SKIP LOCKED drained 1.9× faster**, and naive locking was much less predictable (one run fell
to 1,317 jobs/s). Naive mode never double-claimed a job, in the tests or in the benchmark. Its
cost is that workers wait in line for the same row; it never produces wrong results.

## What failed or surprised us

1. **More workers made no-op jobs slower.** Throughput peaks at 4 workers (12,632 jobs/s) and
   drops at 16 (4,596). Sampling `pg_stat_activity` during a 16-worker run showed the 8 cores
   saturated (0.2% idle) and backends mostly waiting on `Lock:transactionid`,
   `LWLock:LockManager` and `LWLock:BufferContent`: 64 claimers contending for the same rows at
   the head of the queue, on a laptop that also runs the 16 worker processes. At 4 workers the
   waits were mostly CPU and `LWLock:WALWrite` (commit flushes), which is close to this single
   Postgres's ceiling. I/O-bound jobs (10 ms sleep) scale well instead: 285 → 1,169 → 4,895
   jobs/s, because a sleeping slot doesn't hit the database.
2. **The 16-worker runs are noisy** (4,147–6,187 jobs/s for the same case), which is why every
   case ran 3 times.
3. **The enqueue ceiling is the API, not the queue.** One Node API process tops out around
   2,345 enqueues/s. Above that, requests pile up at the HTTP layer (p99 2.7 s at 4,000 req/s)
   and k6 runs out of virtual users, while jobs that do get in are still picked up quickly.
   The 6,000 req/s row's wait numbers look better than 4,000 only because far fewer jobs got
   in. Scaling past this means running more API processes.
4. **500 req/s had an enqueue p99 spike (143 ms)** and 182 requests k6 couldn't send on time,
   while 1,000 req/s was smoother. It looks like a machine hiccup, not a load limit; it's
   reported as measured.
5. **Crash-test run 3 took 93 s instead of about 51 s.** About 35 jobs were re-run without being
   in flight at a kill: live workers went over 5 s without a heartbeat, and the reaper correctly
   handed their jobs to other workers. Nothing was lost. The Postgres log and macOS power log
   showed nothing. With per-worker logging and stall tracking added, 15 more runs were all
   normal (longest stretch with no progress 1.9–3.3 s), so the cause is most likely the laptop
   stalling, but that isn't proven.
6. **The first version of the crash test's own cross-check was wrong.** It counted a job as
   "should have conflicted" when its worker finished it in the same fraction of a millisecond
   as the kill. It also stopped reading log output when a process *exited* instead of when its
   output stream *closed*. Both were fixed before the official runs.
7. **Naive `FOR UPDATE` was expected to sometimes return nothing while jobs were waiting.** It
   didn't: after waiting, Postgres re-checks the row and moves on to the next one, so naive
   mode is slow but correct.

## CI

GitHub Actions (tests against `postgres:16`, plus a docker-compose smoke test that enqueues a
job through the API and checks Prometheus and Grafana) is set up in
[`.github/workflows/ci.yml`](.github/workflows/ci.yml), but **it hasn't run yet**. GitHub
refuses to start the jobs because of a lock on the account ("your account is locked due to a
billing issue"), although the account is on GitHub Free with no balance. A support ticket is
open. Until CI runs, the docker-compose setup is untested: Docker isn't installed on the
benchmark machine.

import { createServer, type Server } from 'node:http';
import { Counter, Gauge, Histogram, Registry } from 'prom-client';
import type { Pool } from './db.ts';

// Seconds. Covers sub-millisecond queue overhead up to multi-minute jobs and backlogs.
const LATENCY_BUCKETS = [
  0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300,
];

// Each process (API, every worker) keeps its own registry and serves it on /metrics;
// Prometheus scrapes all of them and adds the series up.
export class Metrics {
  readonly registry = new Registry();

  readonly jobDuration = new Histogram({
    name: 'job_duration_seconds',
    help: 'Time spent running the handler, per attempt',
    labelNames: ['queue', 'type'] as const,
    buckets: LATENCY_BUCKETS,
    registers: [this.registry],
  });

  readonly jobWait = new Histogram({
    name: 'job_wait_seconds',
    help: 'Time from enqueue to first claim (started_at - created_at)',
    labelNames: ['queue'] as const,
    buckets: LATENCY_BUCKETS,
    registers: [this.registry],
  });

  readonly jobsTotal = new Counter({
    name: 'jobs_total',
    help: 'Finished attempts by outcome: done, retried or dead',
    labelNames: ['queue', 'result'] as const,
    registers: [this.registry],
  });

  readonly reapedJobs = new Counter({
    name: 'reaped_jobs_total',
    help: 'Jobs re-queued by the reaper after their lock expired',
    registers: [this.registry],
  });

  // queue_depth is read from the database at scrape time, so it is right no matter which
  // process enqueued or finished the jobs. Only the API registers it, to avoid every
  // worker running the same count query.
  enableQueueDepth(pool: Pool): void {
    new Gauge({
      name: 'queue_depth',
      help: 'Jobs per queue and status (done jobs are history, not depth, and are left out)',
      labelNames: ['queue', 'status'] as const,
      registers: [this.registry],
      async collect() {
        const { rows } = await pool.query<{ queue: string; status: string; n: number }>(
          `SELECT queue, status, count(*) AS n FROM jobs
           WHERE status <> 'done' GROUP BY queue, status`,
        );
        this.reset();
        // Report 0 instead of a missing series when a queue empties out.
        const queues = new Set(['default', ...rows.map((r) => r.queue)]);
        for (const queue of queues) {
          for (const status of ['queued', 'running', 'dead']) this.set({ queue, status }, 0);
        }
        for (const r of rows) this.set({ queue: r.queue, status: r.status }, r.n);
      },
    });
  }
}

// Minimal /metrics server for worker processes (they have no HTTP API of their own).
export function serveMetrics(metrics: Metrics, port: number): Server {
  const server = createServer((req, res) => {
    if (req.url !== '/metrics') {
      res.writeHead(404).end();
      return;
    }
    metrics.registry.metrics().then(
      (body) => res.writeHead(200, { 'Content-Type': metrics.registry.contentType }).end(body),
      (err: Error) => res.writeHead(500).end(err.message),
    );
  });
  return server.listen(port);
}

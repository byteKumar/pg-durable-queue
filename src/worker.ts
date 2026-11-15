import { hostname } from 'node:os';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { claimModeFromEnv, envInt } from './config.ts';
import { createPool, type Pool } from './db.ts';
import { defaultHandlers, type Handler } from './handlers/index.ts';
import { Metrics, serveMetrics } from './metrics.ts';
import {
  channelFor,
  claim,
  complete,
  fail,
  heartbeat,
  reap,
  type BackoffOptions,
  type ClaimMode,
  type Job,
} from './queue.ts';

export interface WorkerOptions {
  connectionString?: string; // defaults to DATABASE_URL
  queue?: string;
  workerId?: string;
  concurrency?: number; // jobs run in parallel by this process
  visibilityMs?: number; // lock length; heartbeats renew it every visibilityMs / 3
  pollMs?: number; // fallback check for work when no NOTIFY arrives
  reapIntervalMs?: number;
  shutdownTimeoutMs?: number; // how long stop() waits for in-flight jobs
  claimMode?: ClaimMode;
  handlers?: Record<string, Handler>;
  backoff?: BackoffOptions;
  log?: (message: string) => void;
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// One worker process: `concurrency` slots, each repeatedly claiming and running a job.
export class Worker {
  readonly id: string;
  readonly pool: Pool;
  readonly metrics = new Metrics();

  private readonly connectionString: string | undefined;
  private readonly queue: string;
  private readonly concurrency: number;
  private readonly visibilityMs: number;
  private readonly pollMs: number;
  private readonly reapIntervalMs: number;
  private readonly shutdownTimeoutMs: number;
  private readonly claimMode: ClaimMode;
  private readonly handlers: Record<string, Handler>;
  private readonly backoff: BackoffOptions;
  private readonly log: (message: string) => void;

  private started = false;
  private stopping = false;
  private stopPromise: Promise<{ timedOut: boolean }> | null = null;
  private slots: Promise<void>[] = [];
  private idleSlots: Array<() => void> = []; // wake-up functions of slots waiting for work
  private listener: pg.Client | null = null;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private reapTimer: NodeJS.Timeout | undefined;
  private reaping = false;

  constructor(opts: WorkerOptions = {}) {
    this.id = opts.workerId ?? `${hostname()}-${process.pid}`;
    this.connectionString = opts.connectionString ?? process.env.DATABASE_URL;
    this.queue = opts.queue ?? 'default';
    this.concurrency = opts.concurrency ?? 4;
    this.visibilityMs = opts.visibilityMs ?? 30_000;
    this.pollMs = opts.pollMs ?? 1_000;
    this.reapIntervalMs = opts.reapIntervalMs ?? 5_000;
    this.shutdownTimeoutMs = opts.shutdownTimeoutMs ?? 30_000;
    this.claimMode = opts.claimMode ?? 'skip_locked';
    this.handlers = opts.handlers ?? defaultHandlers;
    this.backoff = opts.backoff ?? {};
    this.log = opts.log ?? ((message) => console.error(`[worker ${this.id}] ${message}`));

    // One connection per slot, plus room for heartbeats and the reaper.
    this.pool = createPool(this.connectionString, this.concurrency + 2);
    // An idle pooled connection can error (e.g. Postgres restarts). Without a listener
    // Node treats that as an uncaught error and crashes the process.
    this.pool.on('error', (err) => this.log(`idle connection error: ${err.message}`));
  }

  async start(): Promise<void> {
    if (this.started) throw new Error('worker already started');
    this.started = true;
    await this.listen(); // fail fast if the database is unreachable at startup
    this.reapTimer = setInterval(() => void this.reapOnce(), this.reapIntervalMs);
    for (let i = 0; i < this.concurrency; i++) this.slots.push(this.slotLoop());
    this.log(
      `started queue=${this.queue} concurrency=${this.concurrency} ` +
        `visibility=${this.visibilityMs}ms claim=${this.claimMode}`,
    );
  }

  // Stop claiming, wait for in-flight jobs (up to shutdownTimeoutMs), then disconnect.
  // Jobs still running at the timeout keep their lock until it expires; then the reaper of
  // another worker re-queues them, so nothing is lost.
  stop(): Promise<{ timedOut: boolean }> {
    this.stopPromise ??= this.shutdown();
    return this.stopPromise;
  }

  private async shutdown(): Promise<{ timedOut: boolean }> {
    this.stopping = true;
    clearInterval(this.reapTimer);
    clearTimeout(this.reconnectTimer);
    for (const wake of [...this.idleSlots]) wake(); // idle slots see `stopping` and exit

    let timer: NodeJS.Timeout | undefined;
    const timedOut = await Promise.race([
      Promise.all(this.slots).then(() => false),
      new Promise<boolean>((resolve) => {
        timer = setTimeout(() => resolve(true), this.shutdownTimeoutMs);
      }),
    ]);
    clearTimeout(timer);

    await this.listener?.end().catch(() => {});
    if (timedOut) {
      this.log(`shutdown timed out after ${this.shutdownTimeoutMs}ms with jobs still running`);
      // Don't wait for the pool: a stuck handler may never give its connection back.
      void this.pool.end().catch(() => {});
    } else {
      await this.pool.end();
      this.log('stopped');
    }
    return { timedOut };
  }

  // A dedicated connection (not from the pool) because LISTEN belongs to one session.
  private async listen(): Promise<void> {
    const client = new pg.Client({ connectionString: this.connectionString });
    client.on('notification', () => this.wakeOne());
    client.on('error', (err) => this.onListenerLost(client, err));
    client.on('end', () => this.onListenerLost(client));
    try {
      await client.connect();
      await client.query(`LISTEN ${client.escapeIdentifier(channelFor(this.queue))}`);
    } catch (err) {
      await client.end().catch(() => {});
      throw err;
    }
    this.listener = client;
  }

  // If the LISTEN connection drops, the 1 s poll keeps work moving while we reconnect.
  private onListenerLost(client: pg.Client, err?: Error): void {
    if (this.stopping || this.listener !== client) return;
    this.listener = null;
    this.log(`listener lost (${err?.message ?? 'connection closed'}); polling until it reconnects`);
    client.end().catch(() => {});
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    this.reconnectTimer = setTimeout(() => {
      if (this.stopping) return;
      this.listen().then(
        () => this.log('listener reconnected'),
        () => this.scheduleReconnect(),
      );
    }, 1_000);
  }

  private async slotLoop(): Promise<void> {
    while (!this.stopping) {
      let job: Job | null;
      try {
        job = await claim(this.pool, {
          queue: this.queue,
          workerId: this.id,
          visibilityMs: this.visibilityMs,
          mode: this.claimMode,
        });
      } catch (err) {
        this.log(`claim failed: ${errorMessage(err)}`);
        await this.waitForWork();
        continue;
      }
      if (!job) {
        await this.waitForWork();
        continue;
      }
      // We found a job, so there may be more: let one more idle slot look. A backlog wakes
      // every slot within a few claims; an empty queue wakes none.
      this.wakeOne();
      await this.runJob(job); // a job claimed even during shutdown still runs to the end
    }
  }

  // Resolves on the next NOTIFY, after pollMs, or when stop() is called.
  private waitForWork(): Promise<void> {
    if (this.stopping) return Promise.resolve();
    return new Promise((resolve) => {
      const wake = () => {
        clearTimeout(timer);
        const i = this.idleSlots.indexOf(wake);
        if (i >= 0) this.idleSlots.splice(i, 1);
        resolve();
      };
      const timer = setTimeout(wake, this.pollMs);
      this.idleSlots.push(wake);
    });
  }

  // Wake a single idle slot, not all of them: one new job needs one claim attempt.
  private wakeOne(): void {
    this.idleSlots[0]?.();
  }

  private async runJob(job: Job): Promise<void> {
    // Wait time is measured once, on the first claim; both timestamps come from Postgres.
    if (job.attempts === 1 && job.started_at) {
      const waitSeconds = (job.started_at.getTime() - job.created_at.getTime()) / 1000;
      this.metrics.jobWait.observe({ queue: job.queue }, waitSeconds);
    }
    const stopTimer = this.metrics.jobDuration.startTimer({ queue: job.queue, type: job.type });

    let finished = false;
    const beat = setInterval(() => {
      heartbeat(this.pool, job.id, this.id, this.visibilityMs).then(
        (stillOurs) => {
          if (!stillOurs && !finished) this.log(`job ${job.id}: lock lost while running`);
        },
        (err) => this.log(`job ${job.id}: heartbeat failed: ${errorMessage(err)}`),
      );
    }, Math.max(1, Math.floor(this.visibilityMs / 3)));

    let failure: { error: unknown } | null = null;
    try {
      const handler = this.handlers[job.type];
      if (!handler) throw new Error(`no handler registered for type "${job.type}"`);
      await handler(job, { pool: this.pool, workerId: this.id });
    } catch (error) {
      failure = { error };
    } finally {
      finished = true;
      clearInterval(beat);
      stopTimer();
    }

    // Recording the result is separate from running the handler, so a database error here
    // is never mistaken for a handler failure.
    try {
      if (!failure) {
        if (await complete(this.pool, job.id, this.id)) {
          this.metrics.jobsTotal.inc({ queue: job.queue, result: 'done' });
        } else {
          this.log(`job ${job.id}: finished after losing its lock; it will run again`);
        }
      } else {
        const result = await fail(this.pool, job, this.id, errorMessage(failure.error), this.backoff);
        if (!result) {
          this.log(`job ${job.id}: failed after losing its lock`);
        } else if (result.status === 'queued') {
          this.metrics.jobsTotal.inc({ queue: job.queue, result: 'retried' });
        } else {
          this.metrics.jobsTotal.inc({ queue: job.queue, result: 'dead' });
          this.log(`job ${job.id}: dead after ${job.attempts} attempts: ${errorMessage(failure.error)}`);
        }
      }
    } catch (err) {
      this.log(
        `job ${job.id}: could not record result (${errorMessage(err)}); ` +
          'the reaper will re-queue it when the lock expires',
      );
    }
  }

  private async reapOnce(): Promise<void> {
    if (this.reaping || this.stopping) return; // never overlap with a slow previous run
    this.reaping = true;
    try {
      const ids = await reap(this.pool);
      if (ids.length > 0) {
        this.metrics.reapedJobs.inc(ids.length);
        this.log(`reaper re-queued ${ids.length} job(s): ${ids.join(', ')}`);
      }
    } catch (err) {
      this.log(`reaper failed: ${errorMessage(err)}`);
    } finally {
      this.reaping = false;
    }
  }
}

// CLI: `node src/worker.ts`, configured through environment variables.
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const worker = new Worker({
    queue: process.env.QUEUE ?? 'default',
    workerId: process.env.WORKER_ID,
    concurrency: envInt('CONCURRENCY', 4),
    visibilityMs: envInt('VISIBILITY_MS', 30_000),
    shutdownTimeoutMs: envInt('SHUTDOWN_TIMEOUT_MS', 30_000),
    claimMode: claimModeFromEnv(),
  });

  let signals = 0;
  const onSignal = (signal: string) => {
    if (++signals > 1) {
      console.error(`[worker ${worker.id}] second ${signal}: exiting without waiting`);
      process.exit(1);
    }
    console.error(`[worker ${worker.id}] ${signal}: finishing in-flight jobs`);
    void worker.stop().then(({ timedOut }) => process.exit(timedOut ? 1 : 0));
  };
  process.on('SIGTERM', () => onSignal('SIGTERM'));
  process.on('SIGINT', () => onSignal('SIGINT'));

  await worker.start();

  // Optional: expose this worker's metrics for Prometheus (docker-compose sets it).
  const metricsPort = process.env.METRICS_PORT;
  if (metricsPort) serveMetrics(worker.metrics, envInt('METRICS_PORT', 9100)).unref();
}

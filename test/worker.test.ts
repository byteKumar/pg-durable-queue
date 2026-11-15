import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { insertEffect } from '../src/handlers/write-effect.ts';
import { sleep } from '../src/handlers/types.ts';
import { migrate } from '../src/migrate.ts';
import { claim, enqueue, reap, type Job } from '../src/queue.ts';
import { Worker, type WorkerOptions } from '../src/worker.ts';
import { TEST_DATABASE_URL, createTestPool, resetDb, waitFor } from './helpers.ts';

const pool = createTestPool();
const workers: Worker[] = [];

function startWorker(opts: WorkerOptions = {}): Promise<Worker> {
  const worker = new Worker({
    connectionString: TEST_DATABASE_URL,
    log: () => {},
    pollMs: 100,
    ...opts,
  });
  workers.push(worker);
  return worker.start().then(() => worker);
}

async function getJob(id: number): Promise<Job> {
  const { rows } = await pool.query<Job>('SELECT * FROM jobs WHERE id = $1', [id]);
  if (!rows[0]) throw new Error(`job ${id} not found`);
  return rows[0];
}

function waitForStatus(id: number, status: Job['status'], timeoutMs = 5_000) {
  return waitFor(async () => ((await getJob(id)).status === status ? getJob(id) : null), {
    timeoutMs,
    what: `job ${id} to be ${status}`,
  });
}

beforeAll(async () => {
  await migrate(pool);
});
beforeEach(async () => {
  await resetDb(pool);
});
afterEach(async () => {
  await Promise.all(workers.splice(0).map((w) => w.stop()));
});
afterAll(async () => {
  await pool.end();
});

describe('worker', () => {
  it('wakes on NOTIFY and runs a job without waiting for the poll', async () => {
    await startWorker({ pollMs: 60_000 }); // polling effectively off
    await sleep(100); // let the slots finish their first empty claim and go idle
    const t0 = Date.now();
    const { id } = await enqueue(pool, { type: 'noop' });
    const job = await waitForStatus(id, 'done', 2_000);
    expect(Date.now() - t0).toBeLessThan(1_000);
    expect(job).toMatchObject({ attempts: 1, locked_by: null });
  });

  it('retries a failing handler with growing delays and dead-letters it after max_attempts', async () => {
    const runAts: number[] = [];
    await startWorker({
      pollMs: 20,
      handlers: {
        flaky: async (job) => {
          runAts.push(job.run_at.getTime());
          throw new Error('downstream unavailable');
        },
      },
      backoff: { baseMs: 200, random: () => 0.999 }, // ~200 ms, then ~400 ms
    });
    const { id } = await enqueue(pool, { type: 'flaky', maxAttempts: 3 });

    const job = await waitForStatus(id, 'dead');
    expect(job).toMatchObject({ attempts: 3, last_error: 'downstream unavailable' });
    expect(runAts).toHaveLength(3);
    const gap1 = runAts[1]! - runAts[0]!;
    const gap2 = runAts[2]! - runAts[1]!;
    expect(gap1).toBeGreaterThanOrEqual(199);
    expect(gap2).toBeGreaterThanOrEqual(399);
    expect(gap2).toBeGreaterThan(gap1);
  });

  it('fails jobs whose type has no handler', async () => {
    await startWorker();
    const { id } = await enqueue(pool, { type: 'missing', maxAttempts: 1 });
    const job = await waitForStatus(id, 'dead');
    expect(job.last_error).toBe('no handler registered for type "missing"');
  });

  it('heartbeats keep a long job locked past the visibility timeout', async () => {
    // 1.2 s job, 300 ms lock, reaper every 50 ms: without heartbeats it would be reaped.
    await startWorker({ visibilityMs: 300, reapIntervalMs: 50 });
    const { id } = await enqueue(pool, { type: 'sleep', payload: { ms: 1_200 } });
    const job = await waitForStatus(id, 'done');
    expect(job.attempts).toBe(1);
    expect(job.last_error).toBeNull();
  });
});

describe('reaper', () => {
  it('re-queues only running jobs whose lock expired', async () => {
    const expired = await enqueue(pool, { type: 'noop' });
    await claim(pool, { queue: 'default', workerId: 'gone', visibilityMs: 1 });
    const live = await enqueue(pool, { type: 'noop' });
    await claim(pool, { queue: 'default', workerId: 'alive', visibilityMs: 60_000 });
    const queued = await enqueue(pool, { type: 'noop' });
    await sleep(20);

    expect(await reap(pool)).toEqual([expired.id]);
    expect(await getJob(expired.id)).toMatchObject({
      status: 'queued',
      locked_by: null,
      last_error: 'lock expired: worker died or stalled',
    });
    expect((await getJob(live.id)).status).toBe('running');
    expect((await getJob(queued.id)).status).toBe('queued');
  });

  it("recovers a crashed worker's job and another worker completes it", async () => {
    // A "worker" claims the job and then dies: no heartbeat, no complete.
    const { id } = await enqueue(pool, { type: 'noop' });
    await claim(pool, { queue: 'default', workerId: 'crashed', visibilityMs: 300 });

    await startWorker({ reapIntervalMs: 100 });
    const job = await waitForStatus(id, 'done');
    expect(job.attempts).toBe(2);
    expect(job.locked_by).toBeNull();
  });
});

describe('graceful shutdown', () => {
  it('stop() lets the in-flight job finish and claims nothing new', async () => {
    const worker = await startWorker({ concurrency: 1 });
    const { id } = await enqueue(pool, { type: 'sleep', payload: { ms: 600 } });
    await waitForStatus(id, 'running');

    const stopped = worker.stop();
    const later = await enqueue(pool, { type: 'noop' });
    expect(await stopped).toEqual({ timedOut: false });

    expect((await getJob(id)).status).toBe('done');
    expect((await getJob(later.id)).status).toBe('queued');
  });

  it('gives up after the shutdown timeout and leaves the job for the reaper', async () => {
    const worker = await startWorker({ concurrency: 1, shutdownTimeoutMs: 200 });
    const { id } = await enqueue(pool, { type: 'sleep', payload: { ms: 1_500 } });
    await waitForStatus(id, 'running');
    expect(await worker.stop()).toEqual({ timedOut: true });
    expect((await getJob(id)).status).toBe('running');
  });

  it('SIGTERM: a real worker process finishes its in-flight job before exiting', async () => {
    const workerFile = fileURLToPath(new URL('../src/worker.ts', import.meta.url));
    const child = spawn(process.execPath, [workerFile], {
      env: { ...process.env, DATABASE_URL: TEST_DATABASE_URL, CONCURRENCY: '1' },
      stdio: ['ignore', 'ignore', 'pipe'],
    });
    let stderr = '';
    child.stderr.on('data', (chunk) => (stderr += chunk));
    const exited = new Promise<{ code: number | null; at: number }>((resolve) =>
      child.on('exit', (code) => resolve({ code, at: Date.now() })),
    );

    try {
      const { id } = await enqueue(pool, { type: 'sleep', payload: { ms: 1_000 } });
      await waitForStatus(id, 'running');
      child.kill('SIGTERM');
      const later = await enqueue(pool, { type: 'noop' });

      const { code, at } = await exited;
      expect(code, stderr).toBe(0);
      const job = await getJob(id);
      expect(job.status).toBe('done');
      expect(job.finished_at!.getTime()).toBeLessThanOrEqual(at);
      expect((await getJob(later.id)).status).toBe('queued');
      expect(stderr).toContain('SIGTERM: finishing in-flight jobs');
    } finally {
      child.kill('SIGKILL');
    }
  });
});

describe('write-effect', () => {
  it('writes the effect once; a second run is absorbed as a conflict', async () => {
    const { id } = await enqueue(pool, { type: 'write-effect', payload: { ms: 0 } });
    expect(await insertEffect(pool, id, 'w1')).toBe(true);
    expect(await insertEffect(pool, id, 'w2')).toBe(false);
    const { rows } = await pool.query('SELECT job_id, worker_id FROM job_effects');
    expect(rows).toEqual([{ job_id: id, worker_id: 'w1' }]);
  });
});

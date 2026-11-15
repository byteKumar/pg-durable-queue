import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { migrate } from '../src/migrate.ts';
import { backoffMs, claim, complete, enqueue, fail, type Job } from '../src/queue.ts';
import { createTestPool, resetDb } from './helpers.ts';

// 50 connections so the concurrent-claim test really sends 50 queries at once.
const pool = createTestPool(50);
const VIS = 30_000; // visibility timeout used where it doesn't matter

beforeAll(async () => {
  await migrate(pool);
});
beforeEach(async () => {
  await resetDb(pool);
});
afterAll(async () => {
  await pool.end();
});

async function getJob(id: number): Promise<Job> {
  const { rows } = await pool.query<Job>('SELECT * FROM jobs WHERE id = $1', [id]);
  if (!rows[0]) throw new Error(`job ${id} not found`);
  return rows[0];
}

async function countJobs(): Promise<number> {
  const { rows } = await pool.query<{ n: number }>('SELECT count(*) AS n FROM jobs');
  return rows[0]!.n;
}

describe('backoffMs', () => {
  it('doubles from 1 s and is capped at 5 min', () => {
    const max = () => 0.999999;
    expect(backoffMs(1, { random: max })).toBe(999);
    expect(backoffMs(2, { random: max })).toBe(1_999);
    expect(backoffMs(4, { random: max })).toBe(7_999);
    expect(backoffMs(20, { random: max })).toBe(299_999);
  });

  it('uses full jitter: anywhere from 0 up to the ceiling', () => {
    expect(backoffMs(3, { random: () => 0 })).toBe(0);
    expect(backoffMs(3, { random: () => 0.5 })).toBe(2_000);
  });
});

describe('enqueue', () => {
  it('creates one job for the same idempotency key used twice', async () => {
    const a = await enqueue(pool, { type: 'noop', idempotencyKey: 'order-42' });
    const b = await enqueue(pool, { type: 'noop', idempotencyKey: 'order-42' });
    expect(a).toEqual({ id: a.id, created: true });
    expect(b).toEqual({ id: a.id, created: false });
    expect(await countJobs()).toBe(1);
  });

  it('creates one job when 20 requests with the same key arrive at once', async () => {
    const results = await Promise.all(
      Array.from({ length: 20 }, () => enqueue(pool, { type: 'noop', idempotencyKey: 'k' })),
    );
    expect(new Set(results.map((r) => r.id)).size).toBe(1);
    expect(results.filter((r) => r.created)).toHaveLength(1);
    expect(await countJobs()).toBe(1);
  });

  it('scopes keys per queue and never dedupes jobs without a key', async () => {
    await enqueue(pool, { type: 'noop', queue: 'a', idempotencyKey: 'k' });
    await enqueue(pool, { type: 'noop', queue: 'b', idempotencyKey: 'k' });
    await enqueue(pool, { type: 'noop' });
    await enqueue(pool, { type: 'noop' });
    expect(await countJobs()).toBe(4);
  });

  it('stores the payload as JSON, arrays included', async () => {
    const { id } = await enqueue(pool, { type: 'noop', payload: [1, { ms: 5 }] });
    expect((await getJob(id)).payload).toEqual([1, { ms: 5 }]);
  });

  it('notifies the queue channel so idle workers wake up', async () => {
    const listener = await pool.connect();
    try {
      await listener.query('LISTEN "jobs_emails"');
      const got = new Promise<string>((resolve) => {
        listener.once('notification', (msg) => resolve(msg.channel));
      });
      await enqueue(pool, { type: 'noop', queue: 'emails' });
      expect(await got).toBe('jobs_emails');
    } finally {
      await listener.query('UNLISTEN *');
      listener.release();
    }
  });
});

describe('claim', () => {
  it('50 concurrent claims on 20 jobs: each job claimed exactly once, 30 get nothing', async () => {
    for (let i = 0; i < 20; i++) await enqueue(pool, { type: 'noop' });

    const results = await Promise.all(
      Array.from({ length: 50 }, (_, i) =>
        claim(pool, { queue: 'default', workerId: `w${i}`, visibilityMs: VIS }),
      ),
    );
    const claimed = results.filter((j): j is Job => j !== null);
    expect(claimed).toHaveLength(20);
    expect(new Set(claimed.map((j) => j.id)).size).toBe(20);
    expect(results.filter((j) => j === null)).toHaveLength(30);

    const { rows } = await pool.query<{ status: string; attempts: number; n: number }>(
      'SELECT status, attempts, count(*) AS n FROM jobs GROUP BY status, attempts',
    );
    expect(rows).toEqual([{ status: 'running', attempts: 1, n: 20 }]);
  });

  it('naive FOR UPDATE mode never double-claims either (it is only slower)', async () => {
    for (let i = 0; i < 20; i++) await enqueue(pool, { type: 'noop' });
    const results = await Promise.all(
      Array.from({ length: 50 }, (_, i) =>
        claim(pool, { queue: 'default', workerId: `w${i}`, visibilityMs: VIS, mode: 'naive' }),
      ),
    );
    const ids = results.filter((j): j is Job => j !== null).map((j) => j.id);
    expect(new Set(ids).size).toBe(ids.length);
    const { rows } = await pool.query<{ n: number }>(
      `SELECT count(*) AS n FROM jobs WHERE status = 'running'`,
    );
    expect(rows[0]!.n).toBe(ids.length);
  });

  it('sets the lock, attempt count and first start time', async () => {
    const { id } = await enqueue(pool, { type: 'noop' });
    const job = await claim(pool, { queue: 'default', workerId: 'w1', visibilityMs: 5_000 });
    expect(job).toMatchObject({ id, status: 'running', attempts: 1, locked_by: 'w1' });
    expect(job!.started_at).toBeInstanceOf(Date);
    const lockMs = job!.locked_until!.getTime() - job!.started_at!.getTime();
    expect(lockMs).toBe(5_000); // both come from the same now() in one statement
  });

  it('claims higher priority first, then oldest run_at', async () => {
    const low = await enqueue(pool, { type: 'noop', priority: 0 });
    const high = await enqueue(pool, { type: 'noop', priority: 10 });
    const mid1 = await enqueue(pool, { type: 'noop', priority: 5 });
    const mid2 = await enqueue(pool, { type: 'noop', priority: 5 });

    const order: number[] = [];
    for (let i = 0; i < 4; i++) {
      const job = await claim(pool, { queue: 'default', workerId: 'w', visibilityMs: VIS });
      order.push(job!.id);
    }
    expect(order).toEqual([high.id, mid1.id, mid2.id, low.id]);
  });

  it('does not claim a delayed job before its run_at', async () => {
    const { id } = await enqueue(pool, { type: 'noop', runAt: new Date(Date.now() + 400) });
    expect(await claim(pool, { queue: 'default', workerId: 'w', visibilityMs: VIS })).toBeNull();
    await new Promise((r) => setTimeout(r, 500));
    const job = await claim(pool, { queue: 'default', workerId: 'w', visibilityMs: VIS });
    expect(job?.id).toBe(id);
  });

  it('only claims from the requested queue', async () => {
    await enqueue(pool, { type: 'noop', queue: 'other' });
    expect(await claim(pool, { queue: 'default', workerId: 'w', visibilityMs: VIS })).toBeNull();
  });
});

describe('complete', () => {
  it('marks the job done for the lock holder only', async () => {
    const { id } = await enqueue(pool, { type: 'noop' });
    await claim(pool, { queue: 'default', workerId: 'w1', visibilityMs: VIS });

    expect(await complete(pool, id, 'someone-else')).toBe(false);
    expect((await getJob(id)).status).toBe('running');

    expect(await complete(pool, id, 'w1')).toBe(true);
    const job = await getJob(id);
    expect(job).toMatchObject({ status: 'done', locked_by: null, locked_until: null });
    expect(job.finished_at).toBeInstanceOf(Date);
  });
});

describe('fail', () => {
  it('retries with a growing run_at, then ends dead after max_attempts', async () => {
    const { id } = await enqueue(pool, { type: 'noop', maxAttempts: 4 });
    const half = { random: () => 0.5 }; // fixed jitter so the delays are predictable
    const delays: number[] = [];
    const runAts: number[] = [];

    for (let attempt = 1; attempt <= 3; attempt++) {
      const job = await claim(pool, { queue: 'default', workerId: 'w', visibilityMs: VIS });
      expect(job).toMatchObject({ id, attempts: attempt });

      const result = await fail(pool, job!, 'w', `boom ${attempt}`, half);
      expect(result?.status).toBe('queued');
      delays.push(result!.backoffMs);

      const row = await getJob(id);
      expect(row).toMatchObject({ status: 'queued', last_error: `boom ${attempt}`, locked_by: null });
      runAts.push(row.run_at.getTime() - Date.now());

      // Backoff really holds the job back...
      expect(await claim(pool, { queue: 'default', workerId: 'w', visibilityMs: VIS })).toBeNull();
      // ...so skip the wait to keep the test fast.
      await pool.query('UPDATE jobs SET run_at = now() WHERE id = $1', [id]);
    }

    expect(delays).toEqual([500, 1_000, 2_000]);
    // run_at was set about `delay` ms in the future each time (allow 250 ms of test overhead).
    runAts.forEach((ahead, i) => expect(Math.abs(ahead - delays[i]!)).toBeLessThan(250));

    const last = await claim(pool, { queue: 'default', workerId: 'w', visibilityMs: VIS });
    expect(last?.attempts).toBe(4);
    expect(await fail(pool, last!, 'w', 'boom 4', half)).toEqual({ status: 'dead', backoffMs: 0 });

    const dead = await getJob(id);
    expect(dead).toMatchObject({ status: 'dead', attempts: 4, last_error: 'boom 4' });
    expect(dead.finished_at).toBeInstanceOf(Date);
    expect(await claim(pool, { queue: 'default', workerId: 'w', visibilityMs: VIS })).toBeNull();
  });

  it('returns null and changes nothing if the worker no longer holds the lock', async () => {
    await enqueue(pool, { type: 'noop' });
    const job = await claim(pool, { queue: 'default', workerId: 'w1', visibilityMs: VIS });
    expect(await fail(pool, job!, 'w2', 'boom')).toBeNull();
    expect((await getJob(job!.id)).status).toBe('running');
  });
});

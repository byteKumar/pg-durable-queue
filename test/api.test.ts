import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { buildApi } from '../src/api.ts';
import { migrate } from '../src/migrate.ts';
import { Worker } from '../src/worker.ts';
import { TEST_DATABASE_URL, createTestPool, resetDb, waitFor } from './helpers.ts';

const pool = createTestPool();
const app = buildApi({ pool });

// Fastify's inject() calls the routes in-process, without opening a network port.
function post(body: unknown) {
  return app.inject({ method: 'POST', url: '/jobs', payload: body as object });
}

beforeAll(async () => {
  await migrate(pool);
  await app.ready();
});
beforeEach(async () => {
  await resetDb(pool);
});
afterAll(async () => {
  await app.close();
  await pool.end();
});

describe('POST /jobs', () => {
  it('returns 201 with the new id, then 200 with the same id for a repeated key', async () => {
    const first = await post({ type: 'noop', idempotencyKey: 'invoice-7' });
    expect(first.statusCode).toBe(201);
    const { id } = first.json();
    expect(id).toBeTypeOf('number');

    const again = await post({ type: 'noop', idempotencyKey: 'invoice-7' });
    expect(again.statusCode).toBe(200);
    expect(again.json()).toEqual({ id });
  });

  it('stores every field', async () => {
    const runAt = '2030-01-01T00:00:00.000Z';
    const res = await post({
      type: 'sleep',
      payload: { ms: 5 },
      queue: 'emails',
      priority: 3,
      runAt,
      idempotencyKey: 'k1',
      maxAttempts: 2,
    });
    const job = (await app.inject({ url: `/jobs/${res.json().id}` })).json();
    expect(job).toMatchObject({
      type: 'sleep',
      payload: { ms: 5 },
      queue: 'emails',
      priority: 3,
      run_at: runAt,
      idempotency_key: 'k1',
      max_attempts: 2,
      status: 'queued',
    });
  });

  it.each([
    ['missing type', { payload: {} }],
    ['empty type', { type: '' }],
    ['unknown field (typo of idempotencyKey)', { type: 'noop', idempotency_key: 'k' }],
    ['queue name with a space', { type: 'noop', queue: 'my queue' }],
    ['queue name too long for a NOTIFY channel', { type: 'noop', queue: 'q'.repeat(51) }],
    ['maxAttempts 0', { type: 'noop', maxAttempts: 0 }],
    ['fractional priority', { type: 'noop', priority: 1.5 }],
    ['runAt that is not a date', { type: 'noop', runAt: 'tomorrow' }],
  ])('rejects %s with 400 and creates nothing', async (_name, body) => {
    expect((await post(body)).statusCode).toBe(400);
    const { rows } = await pool.query<{ n: number }>('SELECT count(*) AS n FROM jobs');
    expect(rows[0]!.n).toBe(0);
  });
});

describe('GET /jobs/:id', () => {
  it('returns 404 for a missing job and 400 for a non-numeric id', async () => {
    expect((await app.inject({ url: '/jobs/999' })).statusCode).toBe(404);
    expect((await app.inject({ url: '/jobs/abc' })).statusCode).toBe(400);
  });
});

describe('GET /health', () => {
  it('reports ok when the database answers', async () => {
    const res = await app.inject({ url: '/health' });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ status: 'ok' });
  });
});

describe('metrics', () => {
  it('API /metrics reports queue depth per queue and status from the database', async () => {
    await post({ type: 'noop' });
    await post({ type: 'noop' });
    await post({ type: 'noop', queue: 'emails' });
    const res = await app.inject({ url: '/metrics' });
    expect(res.statusCode).toBe(200);
    expect(res.headers['content-type']).toContain('text/plain');
    expect(res.body).toContain('queue_depth{queue="default",status="queued"} 2');
    expect(res.body).toContain('queue_depth{queue="emails",status="queued"} 1');
    expect(res.body).toContain('queue_depth{queue="default",status="dead"} 0');
  });

  it('a worker counts done, retried, dead and reaped jobs and records wait and duration', async () => {
    // A job held by a "crashed" worker, for the reaper to recover.
    await post({ type: 'noop' });
    await pool.query(
      `UPDATE jobs SET status = 'running', locked_by = 'crashed', attempts = 1,
              started_at = now(), locked_until = now() - interval '1 second'`,
    );
    await post({ type: 'noop' });
    await post({ type: 'boom', maxAttempts: 2 });

    const worker = new Worker({
      connectionString: TEST_DATABASE_URL,
      handlers: { noop: async () => {}, boom: async () => Promise.reject(new Error('x')) },
      backoff: { baseMs: 10 },
      pollMs: 20,
      reapIntervalMs: 50,
      log: () => {},
    });
    await worker.start();
    try {
      await waitFor(async () => {
        const { rows } = await pool.query<{ n: number }>(
          `SELECT count(*) AS n FROM jobs WHERE status IN ('done', 'dead')`,
        );
        return rows[0]!.n === 3;
      });
      const text = await worker.metrics.registry.metrics();
      expect(text).toContain('jobs_total{queue="default",result="done"} 2');
      expect(text).toContain('jobs_total{queue="default",result="retried"} 1');
      expect(text).toContain('jobs_total{queue="default",result="dead"} 1');
      expect(text).toContain('reaped_jobs_total 1');
      // Wait is observed on first claim only: the noop and the boom job (the reaped job's
      // first claim was by the crashed worker).
      expect(text).toContain('job_wait_seconds_count{queue="default"} 2');
      // Duration is observed for every attempt: 2 noop runs + 2 boom runs.
      expect(text).toContain('job_duration_seconds_count{queue="default",type="noop"} 2');
      expect(text).toContain('job_duration_seconds_count{queue="default",type="boom"} 2');
    } finally {
      await worker.stop();
    }
  });
});

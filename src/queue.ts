import type { Pool } from './db.ts';

export type JobStatus = 'queued' | 'running' | 'done' | 'dead';

// One row of the jobs table, as node-postgres returns it (column names unchanged).
export interface Job {
  id: number;
  queue: string;
  type: string;
  payload: unknown;
  idempotency_key: string | null;
  priority: number;
  run_at: Date;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  locked_by: string | null;
  locked_until: Date | null;
  last_error: string | null;
  created_at: Date;
  started_at: Date | null;
  finished_at: Date | null;
}

export interface EnqueueInput {
  type: string;
  payload?: unknown;
  queue?: string;
  priority?: number;
  runAt?: Date;
  idempotencyKey?: string;
  maxAttempts?: number;
}

export interface EnqueueResult {
  id: number;
  created: boolean; // false when an existing job with the same idempotency key was returned
}

// LISTEN/NOTIFY channel that workers on this queue listen to.
export function channelFor(queue: string): string {
  return `jobs_${queue}`;
}

// Inserts a job and wakes idle workers. If (queue, idempotencyKey) already exists, nothing
// is inserted and the existing job's id comes back with created = false.
export async function enqueue(pool: Pool, input: EnqueueInput): Promise<EnqueueResult> {
  const queue = input.queue ?? 'default';
  const key = input.idempotencyKey ?? null;

  // pg_notify runs only for a row the INSERT actually produced, and Postgres delivers the
  // notification when the statement's transaction commits, so a worker can't wake up
  // before the job is visible to it.
  const inserted = await pool.query<{ id: number }>(
    `WITH ins AS (
       INSERT INTO jobs (queue, type, payload, idempotency_key, priority, run_at, max_attempts)
       VALUES ($1, $2, $3::jsonb, $4, $5, COALESCE($6::timestamptz, now()), $7)
       ON CONFLICT (queue, idempotency_key) DO NOTHING
       RETURNING id
     )
     SELECT id, pg_notify($8, '') FROM ins`,
    [
      queue,
      input.type,
      // Stringify ourselves: node-postgres would turn a JS array into a Postgres array, not JSON.
      JSON.stringify(input.payload ?? {}),
      key,
      input.priority ?? 0,
      input.runAt ?? null,
      input.maxAttempts ?? 5,
      channelFor(queue),
    ],
  );
  const row = inserted.rows[0];
  if (row) return { id: row.id, created: true };

  // The key already existed. ON CONFLICT waits for a concurrent inserter to commit, so the
  // row is visible to this new statement.
  const existing = await pool.query<{ id: number }>(
    'SELECT id FROM jobs WHERE queue = $1 AND idempotency_key = $2',
    [queue, key],
  );
  const found = existing.rows[0];
  if (!found) throw new Error('enqueue: insert skipped but no existing job found');
  return { id: found.id, created: false };
}

export type ClaimMode = 'skip_locked' | 'naive';

// skip_locked: a worker passes over rows other workers have locked and takes the next one.
// naive: plain FOR UPDATE, so concurrent workers queue up behind the same row. It exists
// only for the benchmark comparison.
const CLAIM_SQL: Record<ClaimMode, string> = {
  skip_locked: claimSql('FOR UPDATE SKIP LOCKED'),
  naive: claimSql('FOR UPDATE'),
};

function claimSql(lockClause: string): string {
  return `
    UPDATE jobs SET status = 'running', locked_by = $1, locked_until = now() + $2::interval,
           attempts = attempts + 1, started_at = COALESCE(started_at, now())
    WHERE id = (SELECT id FROM jobs
                WHERE queue = $3 AND status = 'queued' AND run_at <= now()
                ORDER BY priority DESC, run_at
                ${lockClause} LIMIT 1)
    RETURNING *`;
}

export interface ClaimOptions {
  queue: string;
  workerId: string;
  visibilityMs: number; // how long the lock lasts before the reaper may take the job back
  mode?: ClaimMode;
}

// Atomically takes the highest-priority ready job, or returns null if none is ready.
export async function claim(pool: Pool, opts: ClaimOptions): Promise<Job | null> {
  const { rows } = await pool.query<Job>(CLAIM_SQL[opts.mode ?? 'skip_locked'], [
    opts.workerId,
    `${opts.visibilityMs} milliseconds`,
    opts.queue,
  ]);
  return rows[0] ?? null;
}

// Marks a job done. Only the worker that holds the lock can do this; returns false if the
// lock was lost (the reaper took the job back after the lock expired).
export async function complete(pool: Pool, jobId: number, workerId: string): Promise<boolean> {
  const { rowCount } = await pool.query(
    `UPDATE jobs SET status = 'done', finished_at = now(), locked_by = NULL, locked_until = NULL
     WHERE id = $1 AND status = 'running' AND locked_by = $2`,
    [jobId, workerId],
  );
  return rowCount === 1;
}

export interface BackoffOptions {
  baseMs?: number;
  capMs?: number;
  random?: () => number; // injectable for tests; Math.random in production
}

// Exponential backoff with full jitter: a random delay between 0 and
// min(base * 2^(attempt-1), cap). The randomness spreads out retries of jobs that failed
// together (e.g. a downstream outage), so they don't all come back at the same instant.
export function backoffMs(attempt: number, opts: BackoffOptions = {}): number {
  const base = opts.baseMs ?? 1_000;
  const cap = opts.capMs ?? 300_000;
  const random = opts.random ?? Math.random;
  const ceiling = Math.min(base * 2 ** (attempt - 1), cap);
  return Math.floor(random() * ceiling);
}

export interface FailResult {
  status: 'queued' | 'dead';
  backoffMs: number; // 0 when dead
}

// Records a failure. Retries later if attempts remain, otherwise moves the job to 'dead'
// (the dead-letter state). Returns null if this worker no longer holds the lock.
export async function fail(
  pool: Pool,
  job: Pick<Job, 'id' | 'attempts' | 'max_attempts'>,
  workerId: string,
  error: string,
  backoff: BackoffOptions = {},
): Promise<FailResult | null> {
  // attempts was already incremented by claim(), so it counts the run that just failed.
  const willRetry = job.attempts < job.max_attempts;
  const delay = willRetry ? backoffMs(job.attempts, backoff) : 0;

  const { rowCount } = willRetry
    ? await pool.query(
        `UPDATE jobs SET status = 'queued', run_at = now() + $4::interval, last_error = $3,
                locked_by = NULL, locked_until = NULL
         WHERE id = $1 AND status = 'running' AND locked_by = $2`,
        [job.id, workerId, error, `${delay} milliseconds`],
      )
    : await pool.query(
        `UPDATE jobs SET status = 'dead', finished_at = now(), last_error = $3,
                locked_by = NULL, locked_until = NULL
         WHERE id = $1 AND status = 'running' AND locked_by = $2`,
        [job.id, workerId, error],
      );
  if (rowCount !== 1) return null;
  return { status: willRetry ? 'queued' : 'dead', backoffMs: delay };
}

// Extends the lock of a job this worker is still running. Returns false if the lock was
// lost, e.g. the reaper already took the job back.
export async function heartbeat(
  pool: Pool,
  jobId: number,
  workerId: string,
  visibilityMs: number,
): Promise<boolean> {
  const { rowCount } = await pool.query(
    `UPDATE jobs SET locked_until = now() + $3::interval
     WHERE id = $1 AND status = 'running' AND locked_by = $2`,
    [jobId, workerId, `${visibilityMs} milliseconds`],
  );
  return rowCount === 1;
}

// Puts running jobs whose lock expired back in the queue: their worker died or stalled.
// Safe to run from many workers at once: SKIP LOCKED means two reapers never fight over
// the same row, and a job whose heartbeat is updating it right now is skipped too.
// Returns the ids it re-queued.
export async function reap(pool: Pool): Promise<number[]> {
  const { rows } = await pool.query<{ id: number; queue: string }>(
    `UPDATE jobs SET status = 'queued', locked_by = NULL, locked_until = NULL,
            last_error = 'lock expired: worker died or stalled'
     WHERE id IN (SELECT id FROM jobs
                  WHERE status = 'running' AND locked_until < now()
                  FOR UPDATE SKIP LOCKED)
     RETURNING id, queue`,
  );
  // Wake idle workers on each affected queue (one notification per queue is enough).
  const queues = [...new Set(rows.map((r) => r.queue))];
  for (const queue of queues) {
    await pool.query('SELECT pg_notify($1, $2)', [channelFor(queue), '']);
  }
  return rows.map((r) => r.id);
}

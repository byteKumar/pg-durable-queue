import { createPool, type Pool } from '../src/db.ts';

export const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? 'postgres://localhost:5432/pgq_test';

export function createTestPool(max = 10) {
  return createPool(TEST_DATABASE_URL, max);
}

// Empties the queue tables and restarts the id sequence so every test starts clean.
export async function resetDb(pool: Pool) {
  await pool.query('TRUNCATE jobs, job_effects RESTART IDENTITY');
}

// Polls `check` until it returns a truthy value or the timeout passes.
export async function waitFor<T>(
  check: () => Promise<T>,
  { timeoutMs = 5_000, intervalMs = 20, what = 'condition' } = {},
): Promise<NonNullable<T>> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await check();
    if (value) return value as NonNullable<T>;
    if (Date.now() > deadline) throw new Error(`timed out after ${timeoutMs}ms waiting for ${what}`);
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

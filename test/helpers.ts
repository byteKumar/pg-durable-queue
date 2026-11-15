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

import { createPool } from '../src/db.ts';

export const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? 'postgres://localhost:5432/pgq_test';

export function createTestPool(max = 10) {
  return createPool(TEST_DATABASE_URL, max);
}

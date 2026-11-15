import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { migrate } from '../src/migrate.ts';
import { createTestPool } from './helpers.ts';

const pool = createTestPool();

async function tableNames(): Promise<string[]> {
  const { rows } = await pool.query<{ table_name: string }>(
    `SELECT table_name FROM information_schema.tables
     WHERE table_schema = 'public' ORDER BY table_name`,
  );
  return rows.map((r) => r.table_name);
}

describe('migrate', () => {
  beforeEach(async () => {
    await pool.query('DROP TABLE IF EXISTS jobs, job_effects, schema_migrations');
  });

  afterAll(async () => {
    // Leave the test database migrated for the other test files.
    await migrate(pool);
    await pool.end();
  });

  it('creates the schema on an empty database and records it', async () => {
    expect(await migrate(pool)).toEqual(['001_jobs.sql']);
    expect(await tableNames()).toEqual(['job_effects', 'jobs', 'schema_migrations']);
  });

  it('is a no-op the second time', async () => {
    await migrate(pool);
    expect(await migrate(pool)).toEqual([]);
  });

  it('applies each migration once when several processes migrate at the same time', async () => {
    const results = await Promise.all([migrate(pool), migrate(pool), migrate(pool)]);
    expect(results.flat()).toEqual(['001_jobs.sql']);
  });
});

import { existsSync } from 'node:fs';
import pg from 'pg';

// Load .env for local runs. Values already in the environment (CI, tests, docker-compose) win.
if (!process.env.DATABASE_URL && existsSync('.env')) {
  process.loadEnvFile('.env');
}

// Postgres bigint (int8, type OID 20) arrives as a string by default because it can exceed
// JavaScript's safe integer range (2^53). Our ids and counts stay far below that, so plain
// numbers are simpler to work with.
pg.types.setTypeParser(20, (value) => Number(value));

export type Pool = pg.Pool;
export type PoolClient = pg.PoolClient;

export function createPool(
  connectionString = process.env.DATABASE_URL,
  max = 10,
): pg.Pool {
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set (copy .env.example to .env)');
  }
  return new pg.Pool({ connectionString, max });
}

// Runs fn inside BEGIN/COMMIT on one connection. Any error rolls back and is rethrown.
export async function withTransaction<T>(
  pool: pg.Pool,
  fn: (client: pg.PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  let broken = false;
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // If ROLLBACK itself fails the connection is unusable; don't hand it back to the pool.
      broken = true;
    }
    throw err;
  } finally {
    client.release(broken);
  }
}

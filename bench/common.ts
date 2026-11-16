// Shared helpers for bench/drain.ts and bench/steady-state.ts.
import { spawn, type ChildProcess } from 'node:child_process';
import { cpus, totalmem } from 'node:os';
import { fileURLToPath } from 'node:url';
import type { Pool } from '../src/db.ts';

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function count(pool: Pool, sql: string, params: unknown[] = []): Promise<number> {
  const { rows } = await pool.query<{ n: number }>(sql, params);
  return rows[0]!.n;
}

export async function machineInfo(pool: Pool): Promise<string> {
  const { rows } = await pool.query<{ version: string; max_connections: string; shared_buffers: string }>(
    `SELECT current_setting('server_version') AS version,
            current_setting('max_connections') AS max_connections,
            current_setting('shared_buffers') AS shared_buffers`,
  );
  const r = rows[0]!;
  const cpu = cpus();
  return (
    `machine: ${cpu[0]?.model} cores=${cpu.length} RAM=${Math.round(totalmem() / 2 ** 30)} GB, ` +
    `Postgres ${r.version}, max_connections=${r.max_connections}, ` +
    `shared_buffers=${r.shared_buffers}, node ${process.version}`
  );
}

export interface Child {
  child: ChildProcess;
  closed: Promise<number | null>; // exit code, after its output streams are closed
  stderr: () => string;
}

// Starts a Node process for one of the src/*.ts entry points with extra environment.
export function startProcess(entry: 'worker' | 'api', env: Record<string, string>): Child {
  const file = fileURLToPath(new URL(`../src/${entry}.ts`, import.meta.url));
  const child = spawn(process.execPath, [file], {
    env: { ...process.env, ...env },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  let err = '';
  child.stderr!.on('data', (chunk: Buffer) => (err += chunk.toString()));
  const closed = new Promise<number | null>((resolve) => child.once('close', (code) => resolve(code)));
  return { child, closed, stderr: () => err };
}

// Waits until no job is queued or running.
export async function waitForDrain(pool: Pool, timeoutMs: number, pollMs = 50): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while ((await count(pool, `SELECT count(*) AS n FROM jobs WHERE status IN ('queued', 'running')`)) > 0) {
    if (performance.now() > deadline) throw new Error('queue did not drain in time');
    await sleep(pollMs);
  }
}

export interface Latency {
  jobs: number;
  waitMs: [number, number, number]; // p50, p95, p99 of started_at - created_at
  e2eMs: [number, number, number]; // p50, p95, p99 of finished_at - created_at
  processingSeconds: number; // first claim to last completion
}

// Percentiles computed by Postgres over every done job (percentile_cont interpolates).
export async function latency(pool: Pool): Promise<Latency> {
  const { rows } = await pool.query<{
    jobs: number;
    wait: number[];
    e2e: number[];
    processing: number;
  }>(
    `SELECT count(*) AS jobs,
            percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP
              (ORDER BY extract(epoch FROM started_at - created_at) * 1000) AS wait,
            percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP
              (ORDER BY extract(epoch FROM finished_at - created_at) * 1000) AS e2e,
            extract(epoch FROM max(finished_at) - min(started_at)) AS processing
     FROM jobs WHERE status = 'done'`,
  );
  const r = rows[0]!;
  return {
    jobs: r.jobs,
    waitMs: r.wait as Latency['waitMs'],
    e2eMs: r.e2e as Latency['e2eMs'],
    processingSeconds: Number(r.processing),
  };
}

export const fmt3 = (xs: number[]) => xs.map((x) => x.toFixed(1)).join(' / ');

import { writeSync } from 'node:fs';
import type { Pool } from '../db.ts';
import { payloadMs, sleep, type Handler } from './types.ts';

// The side effect is "insert one row for this job". Keyed by job_id, so running the job
// again can't create a second row. Returns false when the row already existed, which
// means this job had already run before (a duplicate execution the write absorbed).
export async function insertEffect(pool: Pool, jobId: number, workerId: string): Promise<boolean> {
  const { rowCount } = await pool.query(
    `INSERT INTO job_effects (job_id, worker_id) VALUES ($1, $2)
     ON CONFLICT (job_id) DO NOTHING`,
    [jobId, workerId],
  );
  return rowCount === 1;
}

// Writes the effect, then sleeps payload.ms. A worker killed during the sleep leaves the
// effect written but the job not done, so the job runs again after the reaper re-queues it.
export const writeEffect: Handler = async (job, { pool, workerId }) => {
  const ms = payloadMs(job);
  const inserted = await insertEffect(pool, job.id, workerId);
  if (!inserted) {
    // Synchronous write straight to stderr's file descriptor: once this returns, the line
    // is in the OS pipe buffer, so it isn't lost even if the process is SIGKILLed next.
    // The crash test counts these lines.
    writeSync(2, `effect_conflict job=${job.id} worker=${workerId}\n`);
  }
  await sleep(ms);
};

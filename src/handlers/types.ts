import type { Pool } from '../db.ts';
import type { Job } from '../queue.ts';

export interface HandlerContext {
  pool: Pool;
  workerId: string;
}

// A handler runs one job. Resolving means success; throwing means failure (retry or dead).
export type Handler = (job: Job, ctx: HandlerContext) => Promise<void>;

// Reads payload.ms as a non-negative number, or throws so the job fails visibly.
export function payloadMs(job: Job): number {
  const ms = (job.payload as { ms?: unknown } | null)?.ms;
  if (typeof ms !== 'number' || !Number.isFinite(ms) || ms < 0) {
    throw new Error(`job ${job.id}: payload.ms must be a non-negative number`);
  }
  return ms;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

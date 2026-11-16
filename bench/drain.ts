// Worker throughput and latency: pre-load 50,000 jobs, start N workers, time the drain.
// Covers spec 8b (1/4/16 workers x noop/sleep 10 ms) and 8d (16 workers, naive FOR UPDATE).
//
//   node bench/drain.ts [--repeat 3] [--case 16:noop:naive]   (default: all cases, once)
//
// Uses DATABASE_URL (the dev database) and truncates the jobs tables before every case.
import { parseArgs } from 'node:util';
import { createPool } from '../src/db.ts';
import { migrate } from '../src/migrate.ts';
import type { ClaimMode } from '../src/queue.ts';
import { count, fmt3, latency, machineInfo, startProcess, waitForDrain } from './common.ts';

const JOBS = 50_000;
const CONCURRENCY = 4;

type HandlerName = 'noop' | 'sleep10ms';
interface Case {
  workers: number;
  handler: HandlerName;
  mode: ClaimMode;
}

const ALL_CASES: Case[] = [
  ...[1, 4, 16].map((workers) => ({ workers, handler: 'noop' as const, mode: 'skip_locked' as const })),
  ...[1, 4, 16].map((workers) => ({ workers, handler: 'sleep10ms' as const, mode: 'skip_locked' as const })),
  { workers: 16, handler: 'noop', mode: 'naive' },
];

const { values: args } = parseArgs({
  options: {
    case: { type: 'string', multiple: true },
    repeat: { type: 'string', default: '1' },
  },
});
const repeat = Number(args.repeat);
const cases = args.case
  ? args.case.map((c): Case => {
      const [workers, handler, mode] = c.split(':');
      return { workers: Number(workers), handler: handler as HandlerName, mode: (mode ?? 'skip_locked') as ClaimMode };
    })
  : ALL_CASES;

const pool = createPool(undefined, 2);

interface CaseResult {
  jobsPerSecWall: number;
  waitP99: number;
}

async function runCase(c: Case, rep: number): Promise<CaseResult> {
  await pool.query('TRUNCATE jobs, job_effects RESTART IDENTITY');
  const [type, payload] = c.handler === 'noop' ? ['noop', '{}'] : ['sleep', '{"ms": 10}'];
  await pool.query(
    `INSERT INTO jobs (type, payload) SELECT $1, $2::jsonb FROM generate_series(1, $3)`,
    [type, payload, JOBS],
  );

  // The clock starts when the workers are launched, so process start-up is included.
  const t0 = performance.now();
  const workers = Array.from({ length: c.workers }, (_, i) =>
    startProcess('worker', {
      WORKER_ID: `drain-w${i + 1}`,
      CONCURRENCY: String(CONCURRENCY),
      CLAIM_MODE: c.mode,
    }),
  );
  await waitForDrain(pool, 20 * 60_000);
  const wallSeconds = (performance.now() - t0) / 1000;

  for (const w of workers) w.child.kill('SIGTERM');
  const codes = await Promise.all(workers.map((w) => w.closed));
  const lat = await latency(pool);
  const notDone = await count(pool, `SELECT count(*) AS n FROM jobs WHERE status <> 'done'`);
  const workerErrors = workers
    .flatMap((w) => w.stderr().split('\n'))
    .filter((line) => /failed|lost|error/i.test(line));

  const row = [
    rep,
    c.workers,
    c.handler.padEnd(9),
    c.mode.padEnd(11),
    Math.round(JOBS / wallSeconds),
    Math.round(JOBS / lat.processingSeconds),
    wallSeconds.toFixed(2),
    fmt3(lat.waitMs),
    fmt3(lat.e2eMs),
  ].join(' | ');
  console.log(row);

  const problems: string[] = [];
  if (lat.jobs !== JOBS || notDone !== 0) problems.push(`${lat.jobs} done, ${notDone} not done`);
  if (codes.some((code) => code !== 0)) problems.push(`worker exit codes: ${codes.join(',')}`);
  if (workerErrors.length) problems.push(`worker log: ${workerErrors.slice(0, 5).join(' / ')}`);
  for (const p of problems) console.log(`  PROBLEM: ${p}`);
  return { jobsPerSecWall: JOBS / wallSeconds, waitP99: lat.waitMs[2] };
}

const median = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid]! : (s[mid - 1]! + s[mid]!) / 2;
};

await migrate(pool);
console.log(await machineInfo(pool));
console.log(`drain: ${JOBS} pre-loaded jobs, CONCURRENCY=${CONCURRENCY} per worker process`);
console.log(
  'jobs/s (wall) = jobs / time from launching the workers until all jobs are done (start-up included)\n' +
    'jobs/s (processing) = jobs / (last finished_at - first started_at)\n' +
    'latencies in ms, p50 / p95 / p99, from percentile_cont over all 50,000 jobs\n',
);
console.log(
  'run | workers | handler   | claim mode  | jobs/s (wall) | jobs/s (processing) | wall s | ' +
    'wait p50/p95/p99 | end-to-end p50/p95/p99',
);
// Repetitions are interleaved (all cases, then all cases again) so a slow minute on the
// machine doesn't land entirely on one case.
const results = new Map<Case, CaseResult[]>(cases.map((c) => [c, []]));
for (let rep = 1; rep <= repeat; rep++) {
  for (const c of cases) results.get(c)!.push(await runCase(c, rep));
}
if (repeat > 1) {
  console.log(`\nper case over ${repeat} runs: median jobs/s (wall) [min-max] | median wait p99 ms [min-max]`);
  for (const [c, rs] of results) {
    const tput = rs.map((r) => r.jobsPerSecWall);
    const p99 = rs.map((r) => r.waitP99);
    console.log(
      `${c.workers} | ${c.handler.padEnd(9)} | ${c.mode.padEnd(11)} | ` +
        `${Math.round(median(tput))} [${Math.round(Math.min(...tput))}-${Math.round(Math.max(...tput))}] | ` +
        `${median(p99).toFixed(1)} [${Math.min(...p99).toFixed(1)}-${Math.max(...p99).toFixed(1)}]`,
    );
  }
}
await pool.end();

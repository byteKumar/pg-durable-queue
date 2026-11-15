// Crash-recovery test: SIGKILLs workers mid-job and checks that no job is lost and that
// duplicate executions were absorbed by the idempotent write.
//
//   node bench/crash-test.ts [--runs 5]
//
// Uses DATABASE_URL (the dev database) and truncates the jobs tables on every run.
import { spawn, type ChildProcess } from 'node:child_process';
import { createWriteStream, mkdirSync } from 'node:fs';
import { cpus, totalmem } from 'node:os';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { createPool } from '../src/db.ts';
import { migrate } from '../src/migrate.ts';

// The spec says 10,000, but on an M3 that drains in ~14 s: only ~6 kills at one kill every
// 1-3 s. 40,000 jobs (~42 s) still produced a 19-kill run, so 50,000 (~53 s, ~26 kills on
// average) keeps every other setting and reliably gives the >= 20 kills the spec asks for.
const JOBS = 50_000;
const WORKERS = 8;
const CONCURRENCY = 4;
const VISIBILITY_MS = 5_000;
const KILL_EVERY_MS: [number, number] = [1_000, 3_000];
const MIN_KILLS = 20;
const TIMEOUT_MS = 10 * 60_000;

const WORKER_FILE = fileURLToPath(new URL('../src/worker.ts', import.meta.url));
// Every worker log line, per run, for investigating anything odd (gitignored).
const LOG_DIR = fileURLToPath(new URL('./results/logs/', import.meta.url));
const { values: args } = parseArgs({ options: { runs: { type: 'string', default: '1' } } });
const runs = Number(args.runs);

const pool = createPool(undefined, 4);

interface RunningWorker {
  id: string;
  child: ChildProcess;
  exited: Promise<void>;
}

interface RunResult {
  run: number;
  kills: number;
  inFlightAtKill: number;
  notDone: number;
  lostJobs: number;
  ranMoreThanOnce: number;
  conflictsAbsorbed: number;
  conflictJobs: number;
  conflictJobsExpected: number;
  mismatches: string[];
  reaped: number;
  longestStall: { seconds: number; endedAt: number };
  effectRows: number;
  wallSeconds: number;
}

const randomBetween = (min: number, max: number) => min + Math.random() * (max - min);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function count(sql: string, params: unknown[] = []): Promise<number> {
  const { rows } = await pool.query<{ n: number }>(sql, params);
  return rows[0]!.n;
}

async function runOnce(run: number): Promise<RunResult> {
  await pool.query('TRUNCATE jobs, job_effects RESTART IDENTITY');
  await pool.query(
    `INSERT INTO jobs (type, payload)
     SELECT 'write-effect', jsonb_build_object('ms', 5 + floor(random() * 46)::int)
     FROM generate_series(1, $1)`,
    [JOBS],
  );

  let conflictsAbsorbed = 0;
  const conflictJobs = new Set<number>(); // distinct jobs that hit at least one conflict
  let reaped = 0;
  let nextWorker = 0;
  const workers: RunningWorker[] = [];
  mkdirSync(LOG_DIR, { recursive: true });
  const log = createWriteStream(`${LOG_DIR}crash-run-${run}.log`);
  const t0 = performance.now();
  const stamp = () => ((performance.now() - t0) / 1000).toFixed(3).padStart(8);

  const startWorker = (): RunningWorker => {
    const id = `crash-r${run}-w${++nextWorker}`;
    const child = spawn(process.execPath, [WORKER_FILE], {
      env: {
        ...process.env,
        WORKER_ID: id,
        CONCURRENCY: String(CONCURRENCY),
        VISIBILITY_MS: String(VISIBILITY_MS),
      },
      stdio: ['ignore', 'ignore', 'pipe'],
    });
    // Count lines from stderr. The write-effect handler writes its conflict line
    // synchronously, so lines from a worker killed right afterwards still arrive.
    // 'close' (not 'exit') fires only after stderr has been read to the end.
    let buffered = '';
    child.stderr!.on('data', (chunk: Buffer) => {
      buffered += chunk.toString();
      const lines = buffered.split('\n');
      buffered = lines.pop()!;
      for (const line of lines) {
        log.write(`${stamp()} ${id} ${line}\n`);
        const conflict = /^effect_conflict job=(\d+)/.exec(line);
        if (conflict) {
          conflictsAbsorbed++;
          conflictJobs.add(Number(conflict[1]));
        }
        const m = /reaper re-queued (\d+) job/.exec(line);
        if (m) reaped += Number(m[1]);
      }
    });
    const exited = new Promise<void>((resolve) => child.once('close', () => resolve()));
    const worker = { id, child, exited };
    workers.push(worker);
    return worker;
  };

  const remaining = () => count(`SELECT count(*) AS n FROM jobs WHERE status IN ('queued', 'running')`);

  for (let i = 0; i < WORKERS; i++) startWorker();

  let kills = 0;
  let inFlightAtKill = 0;
  const killed: { id: string; at: Date }[] = [];
  let drained = false;
  // Longest stretch with no job finishing: a healthy run never stalls for long.
  let lastRemaining = Infinity;
  let lastProgressAt = performance.now();
  let longestStall = { seconds: 0, endedAt: 0 };
  const watcher = (async () => {
    for (;;) {
      const left = await remaining();
      const now = performance.now();
      if (left < lastRemaining) {
        const stalled = (now - lastProgressAt) / 1000;
        if (stalled > longestStall.seconds) longestStall = { seconds: stalled, endedAt: (now - t0) / 1000 };
        lastRemaining = left;
        lastProgressAt = now;
      }
      if ((drained = left === 0)) break;
      if (now - t0 > TIMEOUT_MS) throw new Error('queue did not drain in time');
      await sleep(100);
    }
  })();

  // Kill loop: every 1-3 s, SIGKILL a random live worker and start a replacement at once.
  while (!drained) {
    await Promise.race([sleep(randomBetween(...KILL_EVERY_MS)), watcher]);
    if (drained) break;
    const alive = workers.filter((w) => w.child.exitCode === null && w.child.signalCode === null);
    const victim = alive[Math.floor(Math.random() * alive.length)]!;
    inFlightAtKill += await count(
      `SELECT count(*) AS n FROM jobs WHERE status = 'running' AND locked_by = $1`,
      [victim.id],
    );
    victim.child.kill('SIGKILL');
    killed.push({ id: victim.id, at: new Date() });
    log.write(`${stamp()} bench SIGKILL ${victim.id}\n`);
    kills++;
    startWorker();
  }
  await watcher;
  const wallSeconds = (performance.now() - t0) / 1000;

  // Stop the survivors gracefully, then wait until every worker's stderr (killed ones
  // included) has been read to the end.
  for (const w of workers) w.child.kill('SIGTERM');
  await Promise.all(workers.map((w) => w.exited));
  log.end();

  const { rows: expected } = await pool.query<{
    id: number;
    attempts: number;
    writer: string;
    after_kill_ms: number;
  }>(
    `SELECT j.id, j.attempts, e.worker_id AS writer,
            extract(epoch FROM j.finished_at - k.killed_at) * 1000 AS after_kill_ms
     FROM jobs j
     JOIN job_effects e ON e.job_id = j.id
     JOIN unnest($1::text[], $2::timestamptz[]) AS k(worker_id, killed_at)
       ON k.worker_id = e.worker_id
     WHERE j.attempts > 1 AND j.finished_at > k.killed_at`,
    [killed.map((k) => k.id), killed.map((k) => k.at)],
  );
  const unexpected = [...conflictJobs].filter((id) => !expected.some((e) => e.id === id));
  const { rows: unexpectedRows } = await pool.query<{
    id: number;
    attempts: number;
    writer: string;
    last_error: string | null;
    effect_at: Date;
    finished_at: Date;
  }>(
    `SELECT j.id, j.attempts, e.worker_id AS writer, j.last_error,
            e.created_at AS effect_at, j.finished_at
     FROM jobs j JOIN job_effects e ON e.job_id = j.id WHERE j.id = ANY($1) ORDER BY j.id`,
    [unexpected],
  );
  const killedAt = new Map(killed.map((k) => [k.id, k.at]));
  const describe = (e: (typeof expected)[number]) =>
    `attempts=${e.attempts}, effect written by ${e.writer}, ` +
    `job finished ${Number(e.after_kill_ms).toFixed(3)} ms after that worker's kill`;

  return {
    run,
    kills,
    inFlightAtKill,
    notDone: await count(`SELECT count(*) AS n FROM jobs WHERE status <> 'done'`),
    lostJobs: await count(
      `SELECT count(*) AS n FROM jobs j
       WHERE NOT EXISTS (SELECT 1 FROM job_effects e WHERE e.job_id = j.id)`,
    ),
    ranMoreThanOnce: await count('SELECT count(*) AS n FROM jobs WHERE attempts > 1'),
    conflictsAbsorbed,
    conflictJobs: conflictJobs.size,
    // Cross-check from the database alone. A job hit a conflict exactly when the worker that
    // wrote its effect row was killed before finishing it: the job was finished (by someone
    // else) after that worker's kill time, and the finishing run found the row already there.
    conflictJobsExpected: expected.length,
    mismatches: [
      ...expected
        .filter((e) => !conflictJobs.has(e.id))
        .map((e) => `job ${e.id}: expected a conflict, none logged; ${describe(e)}`),
      ...unexpectedRows.map(
        (u) =>
          `job ${u.id}: logged a conflict the DB check did not expect; attempts=${u.attempts}, ` +
          `writer=${u.writer} (killed at ${killedAt.get(u.writer)?.toISOString() ?? 'never'}), ` +
          `effect at ${u.effect_at.toISOString()}, finished at ${u.finished_at.toISOString()}, ` +
          `last_error=${u.last_error}`,
      ),
    ],
    reaped,
    longestStall,
    effectRows: await count('SELECT count(*) AS n FROM job_effects'),
    wallSeconds,
  };
}

async function machineInfo(): Promise<string> {
  const { rows } = await pool.query<{ version: string; max_connections: string }>(
    `SELECT current_setting('server_version') AS version,
            current_setting('max_connections') AS max_connections`,
  );
  const cpu = cpus();
  return (
    `machine: ${cpu[0]?.model} cores=${cpu.length} RAM=${Math.round(totalmem() / 2 ** 30)} GB, ` +
    `Postgres ${rows[0]!.version}, max_connections=${rows[0]!.max_connections}, node ${process.version}`
  );
}

await migrate(pool);
console.log(await machineInfo());
console.log(
  `crash test: ${JOBS} write-effect jobs (5-50 ms), ${WORKERS} workers x ${CONCURRENCY} ` +
    `concurrency, visibility ${VISIBILITY_MS / 1000} s, SIGKILL every ` +
    `${KILL_EVERY_MS[0] / 1000}-${KILL_EVERY_MS[1] / 1000} s until drained\n`,
);
console.log(
  'run | kills | in-flight at kill | not done | lost jobs | jobs run >1 time | ' +
    'conflicts absorbed | jobs with a conflict (expected from DB) | reaped | effect rows | wall time | ' +
    'longest stretch with no progress',
);

const results: RunResult[] = [];
for (let run = 1; run <= runs; run++) {
  const r = await runOnce(run);
  results.push(r);
  console.log(
    [
      r.run,
      r.kills,
      r.inFlightAtKill,
      r.notDone,
      r.lostJobs,
      r.ranMoreThanOnce,
      r.conflictsAbsorbed,
      `${r.conflictJobs} (${r.conflictJobsExpected})`,
      r.reaped,
      r.effectRows,
      `${r.wallSeconds.toFixed(2)} s`,
      `${r.longestStall.seconds.toFixed(1)} s at t=${r.longestStall.endedAt.toFixed(0)} s`,
    ].join(' | '),
  );
}

const problems: string[] = [];
for (const r of results) {
  if (r.notDone !== 0) problems.push(`run ${r.run}: ${r.notDone} jobs not done`);
  if (r.lostJobs !== 0) problems.push(`run ${r.run}: ${r.lostJobs} lost jobs`);
  if (r.conflictJobs !== r.conflictJobsExpected) {
    problems.push(
      `run ${r.run}: ${r.conflictJobs} jobs logged a conflict, ${r.conflictJobsExpected} expected from DB`,
      ...r.mismatches.map((m) => `  ${m}`),
    );
  }
  if (r.effectRows !== JOBS) problems.push(`run ${r.run}: ${r.effectRows} effect rows, expected ${JOBS}`);
  if (r.kills < MIN_KILLS) problems.push(`run ${r.run}: only ${r.kills} kills (spec asks for >= ${MIN_KILLS})`);
}
console.log(problems.length ? `\nPROBLEMS:\n${problems.join('\n')}` : '\nall runs passed');
await pool.end();
process.exitCode = problems.length ? 1 : 0;

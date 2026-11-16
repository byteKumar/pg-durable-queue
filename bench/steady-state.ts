// Steady-state latency under load (spec 8c): the API plus 8 noop workers, with k6 sending
// POST /jobs at a constant rate for 60 s per rate. Wait latency comes from the jobs table.
//
//   node bench/steady-state.ts [--rates 200,500,1000] [--duration 60s]
//
// Uses DATABASE_URL (the dev database) and truncates the jobs tables before every rate.
import { spawn } from 'node:child_process';
import { mkdirSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { createPool } from '../src/db.ts';
import { migrate } from '../src/migrate.ts';
import { count, fmt3, latency, machineInfo, sleep, startProcess, waitForDrain } from './common.ts';

const WORKERS = 8;
const CONCURRENCY = 4;
const PORT = 3100;
const BASE_URL = `http://localhost:${PORT}`;
const WARMUP = { rate: 100, duration: '10s' };

const { values: args } = parseArgs({
  options: {
    rates: { type: 'string', default: '200,500,1000' },
    duration: { type: 'string', default: '60s' },
  },
});
const rates = args.rates.split(',').map(Number);

const K6_SCRIPT = fileURLToPath(new URL('./enqueue.k6.js', import.meta.url));
const K6_DIR = fileURLToPath(new URL('./results/k6/', import.meta.url));
mkdirSync(K6_DIR, { recursive: true });

const pool = createPool(undefined, 2);

interface K6Summary {
  metrics: Record<string, { values: Record<string, number> } | undefined>;
}

function runK6(rate: number, duration: string, summaryFile: string): Promise<K6Summary> {
  return new Promise((resolve, reject) => {
    const k6 = spawn(
      'k6',
      ['run', '--quiet', '-e', `RATE=${rate}`, '-e', `DURATION=${duration}`,
       '-e', `BASE_URL=${BASE_URL}`, '-e', `SUMMARY_FILE=${summaryFile}`, K6_SCRIPT],
      { stdio: ['ignore', 'inherit', 'inherit'] },
    );
    k6.once('close', (code) => {
      // k6 exits non-zero when a threshold fails; we set none, so any non-zero is an error.
      if (code !== 0) return reject(new Error(`k6 exited with ${code}`));
      resolve(JSON.parse(readFileSync(summaryFile, 'utf8')) as K6Summary);
    });
  });
}

async function waitForHealth() {
  for (let i = 0; i < 100; i++) {
    try {
      if ((await fetch(`${BASE_URL}/health`)).ok) return;
    } catch {
      // not listening yet
    }
    await sleep(100);
  }
  throw new Error('API did not become healthy');
}

await migrate(pool);
console.log(await machineInfo(pool));
console.log(
  `steady state: API (PORT ${PORT}, LOG_LEVEL=warn, default pool of 10) + ${WORKERS} workers x ` +
    `${CONCURRENCY} concurrency (noop); k6 constant-arrival-rate POST /jobs for ${args.duration} per rate.\n` +
    `k6, the API, the workers and Postgres all share this one machine. ` +
    `Warm-up first: ${WARMUP.rate} req/s for ${WARMUP.duration} (not reported).\n` +
    `Latencies in ms. Wait = started_at - created_at; end-to-end = finished_at - created_at.\n`,
);

await pool.query('TRUNCATE jobs, job_effects RESTART IDENTITY');
const api = startProcess('api', { PORT: String(PORT), LOG_LEVEL: 'warn' });
const workers = Array.from({ length: WORKERS }, (_, i) =>
  startProcess('worker', { WORKER_ID: `steady-w${i + 1}`, CONCURRENCY: String(CONCURRENCY) }),
);
await waitForHealth();
await sleep(500); // let the workers connect and LISTEN

try {
  await runK6(WARMUP.rate, WARMUP.duration, `${K6_DIR}warmup.json`);
  await waitForDrain(pool, 60_000);

  console.log(
    'rate req/s | requests | achieved req/s | enqueue p95/p99 | error % | dropped iterations | ' +
      'jobs done | wait p50/p95/p99 | end-to-end p50/p95/p99',
  );
  const passing: number[] = [];
  for (const rate of rates) {
    await pool.query('TRUNCATE jobs, job_effects RESTART IDENTITY');
    const summary = await runK6(rate, args.duration, `${K6_DIR}rate-${rate}.json`);
    await waitForDrain(pool, 5 * 60_000);

    const m = summary.metrics;
    const duration = m.http_req_duration!.values;
    const lat = await latency(pool);
    const notDone = await count(pool, `SELECT count(*) AS n FROM jobs WHERE status <> 'done'`);
    console.log(
      [
        rate,
        m.http_reqs!.values.count,
        m.http_reqs!.values.rate!.toFixed(1),
        `${duration['p(95)']!.toFixed(2)} / ${duration['p(99)']!.toFixed(2)}`,
        (m.http_req_failed!.values.rate! * 100).toFixed(3),
        m.dropped_iterations?.values.count ?? 0,
        lat.jobs + (notDone ? ` (${notDone} NOT DONE)` : ''),
        fmt3(lat.waitMs),
        fmt3(lat.e2eMs),
      ].join(' | '),
    );
    if (lat.waitMs[2] < 100) passing.push(rate);
  }
  console.log(
    `\nhighest tested rate with wait p99 < 100 ms: ${passing.length ? Math.max(...passing) : 'none'}`,
  );
} finally {
  api.child.kill('SIGTERM');
  for (const w of workers) w.child.kill('SIGTERM');
  await Promise.all([api.closed, ...workers.map((w) => w.closed)]);
  await pool.end();
}

const errors = [api, ...workers]
  .flatMap((p) => p.stderr().split('\n'))
  .filter((line) => /failed|lost|error/i.test(line));
if (errors.length) console.log(`process log problems:\n${errors.slice(0, 10).join('\n')}`);

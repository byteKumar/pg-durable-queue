// k6 load test for POST /jobs at a fixed arrival rate (requests start on schedule whether or
// not earlier ones have finished, so a slow server shows up as latency, not a lower rate).
//
//   k6 run -e RATE=500 -e DURATION=60s bench/enqueue.k6.js
//
// bench/steady-state.ts runs this at several rates and reads the JSON summary it writes.
import http from 'k6/http';
import { check } from 'k6';

const RATE = Number(__ENV.RATE || 200);
const DURATION = __ENV.DURATION || '60s';
const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';
const SUMMARY_FILE = __ENV.SUMMARY_FILE || 'k6-summary.json';

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    enqueue: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.max(20, Math.ceil(RATE / 10)),
      maxVUs: Math.max(200, RATE),
    },
  },
};

const body = JSON.stringify({ type: 'noop' });
const params = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const res = http.post(`${BASE_URL}/jobs`, body, params);
  check(res, { 'status is 201': (r) => r.status === 201 });
}

export function handleSummary(data) {
  return { [SUMMARY_FILE]: JSON.stringify(data, null, 2) };
}

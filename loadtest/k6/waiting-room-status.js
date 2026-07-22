import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const statusLatency = new Trend('waiting_room_status_duration', true);
const admitted = new Counter('waiting_room_status_admitted');
const unexpected5xx = new Counter('unexpected_5xx');

function required(name) {
  const value = (__ENV[name] || '').trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function entries() {
  const values = required('ENTRY_IDS').split(',').map((value) => value.trim()).filter(Boolean);
  if (values.length === 0) throw new Error('ENTRY_IDS must contain at least one entry');
  return values;
}

export const options = {
  vus: Number.parseInt(__ENV.VUS || '10', 10),
  duration: (__ENV.DURATION || '30s').trim(),
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    checks: ['rate>0.99'],
    unexpected_5xx: ['count==0'],
    waiting_room_status_duration: ['p(95)<750'],
  },
};

export function setup() {
  const config = {
    baseURL: required('BASE_URL').replace(/\/$/, ''),
    customerToken: required('CUSTOMER_TOKEN'),
    entryIDs: entries(),
  };
  const response = http.get(`${config.baseURL}/livez`, { tags: { operation: 'setup_livez' } });
  if (response.status !== 200) throw new Error('setup contract failed: /livez must return 200');
  return config;
}

export default function (config) {
  const index = exec.scenario.iterationInTest % config.entryIDs.length;
  const response = http.get(
    `${config.baseURL}/api/v1/waiting-room/entries/${encodeURIComponent(config.entryIDs[index])}`,
    {
      headers: { Authorization: `Bearer ${config.customerToken}` },
      tags: { operation: 'waiting_room_status' },
    },
  );
  statusLatency.add(response.timings.duration);
  if (response.status >= 500) unexpected5xx.add(1);
  if (response.status === 200 && response.json('status') === 'admitted') admitted.add(1);
  check(response, {
    'status is available or entry is terminal': (r) => [200, 404, 410].includes(r.status),
    'successful response disables credential caching': (r) =>
      r.status !== 200 || (r.headers['Cache-Control'] || '').includes('no-store'),
  });
  // Keep the latency sample bounded instead of turning it into a capacity run.
  sleep(1);
}

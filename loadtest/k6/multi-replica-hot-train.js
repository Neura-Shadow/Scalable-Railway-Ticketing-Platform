import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const apiReplicaHits = new Counter('api_replica_hits');
const stableDuplicates = new Counter('stable_duplicate_joins');
const unexpected5xx = new Counter('unexpected_5xx');
const joinLatency = new Trend('multi_replica_join_duration', true);

function required(name) {
  const value = (__ENV[name] || '').trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function positive(name, fallback) {
  const value = Number.parseInt(__ENV[name] || `${fallback}`, 10);
  if (!Number.isInteger(value) || value <= 0) throw new Error(`${name} must be positive`);
  return value;
}

function sharedQueueScenario() {
  const vus = positive('VUS', 30);
  const duration = (__ENV.DURATION || '').trim();
  if (duration) {
    return {
      executor: 'constant-vus',
      vus,
      duration,
      gracefulStop: (__ENV.GRACEFUL_STOP || '10s').trim(),
    };
  }
  return {
    executor: 'per-vu-iterations',
    vus,
    iterations: positive('ITERATIONS_PER_VU', 5),
    maxDuration: (__ENV.MAX_DURATION || '2m').trim(),
  };
}

export const options = {
  scenarios: {
    shared_queue: sharedQueueScenario(),
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    checks: ['rate>0.99'],
    unexpected_5xx: ['count==0'],
  },
};

export function setup() {
  const tokens = required('CUSTOMER_TOKENS').split(',').map((value) => value.trim()).filter(Boolean);
  const config = {
    baseURL: required('BASE_URL').replace(/\/$/, ''),
    tokens,
    trainRunID: required('TRAIN_RUN_ID'),
    origin: required('ORIGIN_CODE').toUpperCase(),
    destination: required('DESTINATION_CODE').toUpperCase(),
    seatClass: required('SEAT_CLASS').toLowerCase(),
    passengerCount: positive('PASSENGER_COUNT', 1),
  };
  if (tokens.length < positive('VUS', 30)) {
    throw new Error('setup contract failed: CUSTOMER_TOKENS must provide one distinct identity per VU');
  }
  const response = http.get(`${config.baseURL}/readyz`, { tags: { operation: 'setup_readyz' } });
  if (response.status !== 200 || !response.headers['X-Upstream-Addr']) {
    throw new Error('setup contract failed: use docker-compose.multi-replica.yml load-balancer URL');
  }
  return config;
}

export default function (config) {
  const customerToken = config.tokens[exec.vu.idInTest - 1];
  const request = () => http.post(
    `${config.baseURL}/api/v1/waiting-room/entries`,
    JSON.stringify({
      train_run_id: config.trainRunID,
      origin_station_code: config.origin,
      destination_station_code: config.destination,
      seat_class: config.seatClass,
      passenger_count: config.passengerCount,
    }),
    {
      headers: { Authorization: `Bearer ${customerToken}`, 'Content-Type': 'application/json' },
      tags: { operation: 'multi_replica_waiting_room_join' },
    },
  );

  const first = request();
  joinLatency.add(first.timings.duration);
  const second = request();
  joinLatency.add(second.timings.duration);
  for (const response of [first, second]) {
    if (response.status >= 500) unexpected5xx.add(1);
    const upstream = response.headers['X-Upstream-Addr'] || 'missing';
    apiReplicaHits.add(1, { upstream });
  }

  let firstID = '';
  let secondID = '';
  try {
    firstID = first.json('entry_id') || '';
    secondID = second.json('entry_id') || '';
  } catch (_) {
    firstID = '';
    secondID = '';
  }
  if (first.status === 201 && second.status === 201 && firstID === secondID) stableDuplicates.add(1);
  check({ first, second }, {
    'both replicas observe a successful bounded join': ({ first: a, second: b }) =>
      a.status === 201 && b.status === 201,
    'duplicate join returns one shared entry': () => firstID.length > 0 && firstID === secondID,
    'load balancer exposes bounded local evidence header': ({ first: a, second: b }) =>
      Boolean(a.headers['X-Upstream-Addr']) && Boolean(b.headers['X-Upstream-Addr']),
  });
}

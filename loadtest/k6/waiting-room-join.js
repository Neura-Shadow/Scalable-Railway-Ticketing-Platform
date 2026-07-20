import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const joins = new Counter('waiting_room_join_attempts');
const duplicates = new Counter('waiting_room_duplicate_observations');
const queueFull = new Counter('waiting_room_queue_full_observations');
const unexpected5xx = new Counter('unexpected_5xx');
const joinLatency = new Trend('waiting_room_join_duration', true);

function required(name) {
  const value = (__ENV[name] || '').trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function positive(name, fallback) {
  const value = Number.parseInt((__ENV[name] || `${fallback}`).trim(), 10);
  if (!Number.isInteger(value) || value <= 0) throw new Error(`${name} must be positive`);
  return value;
}

function contract() {
  return {
    baseURL: required('BASE_URL').replace(/\/$/, ''),
    customerToken: required('CUSTOMER_TOKEN'),
    trainRunID: required('TRAIN_RUN_ID'),
    origin: required('ORIGIN_CODE').toUpperCase(),
    destination: required('DESTINATION_CODE').toUpperCase(),
    seatClass: required('SEAT_CLASS').toLowerCase(),
    passengerCount: positive('PASSENGER_COUNT', 1),
  };
}

export const options = {
  scenarios: {
    joins: {
      executor: 'constant-vus',
      vus: positive('VUS', 10),
      duration: (__ENV.DURATION || '30s').trim(),
    },
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    checks: ['rate>0.99'],
    unexpected_5xx: ['count==0'],
    waiting_room_join_duration: ['p(95)<1000'],
  },
};

export function setup() {
  const config = contract();
  const response = http.get(`${config.baseURL}/livez`, { tags: { operation: 'setup_livez' } });
  if (response.status !== 200) throw new Error('setup contract failed: /livez must return 200');
  return config;
}

export default function (config) {
  const response = http.post(
    `${config.baseURL}/api/v1/waiting-room/entries`,
    JSON.stringify({
      train_run_id: config.trainRunID,
      origin_station_code: config.origin,
      destination_station_code: config.destination,
      seat_class: config.seatClass,
      passenger_count: config.passengerCount,
    }),
    {
      headers: {
        Authorization: `Bearer ${config.customerToken}`,
        'Content-Type': 'application/json',
      },
      tags: { operation: 'waiting_room_join' },
    },
  );
  joins.add(1);
  joinLatency.add(response.timings.duration);
  if (response.status >= 500) unexpected5xx.add(1);
  if (response.status === 429) queueFull.add(1);

  let entryID = '';
  try {
    entryID = response.json('entry_id') || '';
  } catch (_) {
    entryID = '';
  }
  if (response.status === 201 && entryID) duplicates.add(1, { result: 'accepted_or_duplicate' });
  check(response, {
    'join returns entry or bounded rejection': (r) => [201, 409, 429].includes(r.status),
    'successful join has opaque entry id': () => response.status !== 201 || entryID.length > 0,
    'retry-after is bounded when present': (r) => {
      const raw = r.headers['Retry-After'];
      return !raw || (Number(raw) >= 1 && Number(raw) <= 60);
    },
  });
}

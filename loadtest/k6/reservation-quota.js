import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const created = new Counter('reservation_holds_created');
const quotaRejected = new Counter('reservation_quota_rejected');
const unexpected5xx = new Counter('unexpected_5xx');

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

function quotaCases() {
  let parsed;
  try {
    parsed = JSON.parse(required('RESERVATION_CASES_JSON'));
  } catch (_) {
    throw new Error('RESERVATION_CASES_JSON must be valid JSON');
  }
  if (!Array.isArray(parsed) || parsed.length === 0) throw new Error('RESERVATION_CASES_JSON must be non-empty');
  for (const item of parsed) {
    if (!item.customer_token || !item.admission_token || !item.idempotency_key ||
        !Array.isArray(item.passenger_ids) || item.passenger_ids.length === 0) {
      throw new Error('each quota case must contain customer/admission tokens, idempotency key, and passenger IDs');
    }
  }
  return parsed;
}

const maxActiveHolds = positive('MAX_ACTIVE_HOLDS', 3);

export const options = {
  scenarios: {
    quota_race: {
      executor: 'per-vu-iterations',
      vus: positive('VUS', 100),
      iterations: 1,
      maxDuration: (__ENV.MAX_DURATION || '2m').trim(),
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
    unexpected_5xx: ['count==0'],
    reservation_holds_created: [`count<=${maxActiveHolds}`],
  },
};

export function setup() {
  const config = {
    baseURL: required('BASE_URL').replace(/\/$/, ''),
    trainRunID: required('TRAIN_RUN_ID'),
    origin: required('ORIGIN_CODE').toUpperCase(),
    destination: required('DESTINATION_CODE').toUpperCase(),
    seatClass: required('SEAT_CLASS').toLowerCase(),
    cases: quotaCases(),
  };
  if (config.cases.length < positive('VUS', 100)) {
    throw new Error('setup contract failed: each VU needs a separately admitted token and idempotency key');
  }
  return config;
}

export default function (config) {
  const item = config.cases[exec.vu.idInTest - 1];
  const response = http.post(
    `${config.baseURL}/api/v1/reservations`,
    JSON.stringify({
      train_run_id: config.trainRunID,
      origin_station_code: config.origin,
      destination_station_code: config.destination,
      seat_class: config.seatClass,
      passenger_ids: item.passenger_ids,
    }),
    {
      headers: {
        Authorization: `Bearer ${item.customer_token}`,
        'Content-Type': 'application/json',
        'Idempotency-Key': item.idempotency_key,
        'X-Admission-Token': item.admission_token,
      },
      tags: { operation: 'reservation_quota_race' },
    },
  );
  if (response.status === 201) created.add(1);
  if (response.status === 429 && response.json('error.code') === 'reservation_quota_exceeded') {
    quotaRejected.add(1);
  }
  if (response.status >= 500) unexpected5xx.add(1);
  check(response, {
    'quota attempt creates or rejects without internal error': (r) => [201, 409, 429].includes(r.status),
    'quota rejection includes bounded retry guidance': (r) =>
      r.status !== 429 || (Number(r.headers['Retry-After']) >= 1 && Number(r.headers['Retry-After']) <= 60),
  });
}

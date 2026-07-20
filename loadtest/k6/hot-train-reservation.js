import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const reservationLatency = new Trend('hot_train_reservation_duration', true);
const quotaRejects = new Counter('reservation_quota_rejects');
const backpressureRejects = new Counter('reservation_backpressure_rejects');
const seatConflicts = new Counter('seat_conflicts');
const unexpected5xx = new Counter('unexpected_5xx');

function required(name) {
  const value = (__ENV[name] || '').trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function cases() {
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
      throw new Error('each reservation case must contain token, admission token, idempotency key, and passenger IDs');
    }
  }
  return parsed;
}

export const options = {
  scenarios: {
    reservations: {
      executor: 'per-vu-iterations',
      vus: Number.parseInt(__ENV.VUS || '10', 10),
      iterations: 1,
      maxDuration: (__ENV.MAX_DURATION || '2m').trim(),
    },
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    checks: ['rate>0.99'],
    unexpected_5xx: ['count==0'],
  },
};

export function setup() {
  const config = {
    baseURL: required('BASE_URL').replace(/\/$/, ''),
    trainRunID: required('TRAIN_RUN_ID'),
    origin: required('ORIGIN_CODE').toUpperCase(),
    destination: required('DESTINATION_CODE').toUpperCase(),
    seatClass: required('SEAT_CLASS').toLowerCase(),
    cases: cases(),
  };
  if (config.cases.length < Number.parseInt(__ENV.VUS || '10', 10)) {
    throw new Error('setup contract failed: provide at least one pre-admitted case per VU');
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
      tags: { operation: 'hot_train_reservation' },
    },
  );
  reservationLatency.add(response.timings.duration);
  if (response.status === 409) seatConflicts.add(1);
  if (response.status === 429 && response.json('error.code') === 'reservation_quota_exceeded') quotaRejects.add(1);
  if (response.status === 503) backpressureRejects.add(1);
  if (response.status >= 500 && response.status !== 503) unexpected5xx.add(1);
  check(response, {
    'reservation returns durable result or bounded contention': (r) => [201, 409, 429, 503].includes(r.status),
    'successful reservation has id': (r) => r.status !== 201 || Boolean(r.json('id')),
    'bounded overload includes Retry-After': (r) =>
      ![429, 503].includes(r.status) || (Number(r.headers['Retry-After']) >= 1 && Number(r.headers['Retry-After']) <= 60),
  });
}

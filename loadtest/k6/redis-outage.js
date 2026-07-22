import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const hotFailClosed = new Counter('hot_redis_outage_fail_closed');
const nonHotReachedBooking = new Counter('non_hot_redis_outage_booking_path');
const unexpected5xx = new Counter('unexpected_5xx');

function required(name) {
  const value = (__ENV[name] || '').trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function passengerIDs() {
  const values = required('NON_HOT_PASSENGER_IDS').split(',').map((value) => value.trim()).filter(Boolean);
  if (values.length === 0) throw new Error('NON_HOT_PASSENGER_IDS must be non-empty');
  return values;
}

export const options = {
  scenarios: {
    outage: {
      executor: 'shared-iterations',
      vus: Number.parseInt(__ENV.VUS || '10', 10),
      iterations: Number.parseInt(__ENV.ITERATIONS || '100', 10),
      maxDuration: (__ENV.MAX_DURATION || '2m').trim(),
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
    unexpected_5xx: ['count==0'],
  },
};

export function setup() {
  const config = {
    baseURL: required('BASE_URL').replace(/\/$/, ''),
    customerToken: required('CUSTOMER_TOKEN'),
    hotTrainRunID: required('HOT_TRAIN_RUN_ID'),
    nonHotTrainRunID: required('NON_HOT_TRAIN_RUN_ID'),
    origin: required('ORIGIN_CODE').toUpperCase(),
    destination: required('DESTINATION_CODE').toUpperCase(),
    seatClass: required('SEAT_CLASS').toLowerCase(),
    passengerIDs: passengerIDs(),
    keyPrefix: required('IDEMPOTENCY_KEY_PREFIX'),
  };
  const response = http.get(`${config.baseURL}/livez`);
  if (response.status !== 200) throw new Error('setup contract failed: API must be live before the operator stops Redis');
  if ((__ENV.CONFIRM_REDIS_IS_DOWN || '').trim() !== 'yes') {
    throw new Error('setup contract failed: stop Redis externally, then set CONFIRM_REDIS_IS_DOWN=yes');
  }
  return config;
}

export default function (config) {
  const hot = http.post(
    `${config.baseURL}/api/v1/waiting-room/entries`,
    JSON.stringify({
      train_run_id: config.hotTrainRunID,
      origin_station_code: config.origin,
      destination_station_code: config.destination,
      seat_class: config.seatClass,
      passenger_count: config.passengerIDs.length,
    }),
    {
      headers: { Authorization: `Bearer ${config.customerToken}`, 'Content-Type': 'application/json' },
      tags: { operation: 'hot_join_redis_outage' },
    },
  );
  if (hot.status === 503) hotFailClosed.add(1);
  const hotRetryAfter = Number(hot.headers['Retry-After']);
  const hotFailedClosed =
    hot.status === 503 && hotRetryAfter >= 1 && hotRetryAfter <= 60;
  if (!hotFailedClosed) {
    let publicCode = 'unparseable';
    try {
      publicCode = hot.json('error.code') || 'missing';
    } catch (_) {
      // Keep diagnostics bounded and never print response bodies or credentials.
    }
    console.error(
      `hot fail-closed mismatch status=${hot.status} retry_after=${hot.headers['Retry-After'] || 'missing'} code=${publicCode}`,
    );
  }
  check(hot, {
    'hot waiting room fails closed': () => hotFailedClosed,
  });

  const nonHot = http.post(
    `${config.baseURL}/api/v1/reservations`,
    JSON.stringify({
      train_run_id: config.nonHotTrainRunID,
      origin_station_code: config.origin,
      destination_station_code: config.destination,
      seat_class: config.seatClass,
      passenger_ids: config.passengerIDs,
    }),
    {
      headers: {
        Authorization: `Bearer ${config.customerToken}`,
        'Content-Type': 'application/json',
        'Idempotency-Key': `${config.keyPrefix}-${exec.scenario.iterationInTest}`,
      },
      tags: { operation: 'non_hot_reservation_redis_outage' },
    },
  );
  if ([201, 409, 429].includes(nonHot.status)) nonHotReachedBooking.add(1);
  if (nonHot.status >= 500) unexpected5xx.add(1);
  check(nonHot, {
    'non-hot request preserves bounded PostgreSQL booking path': (r) => [201, 409, 429].includes(r.status),
  });
}

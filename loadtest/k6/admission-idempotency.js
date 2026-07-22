import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const replayed = new Counter('admission_idempotent_replays');
const mismatches = new Counter('admission_idempotency_mismatches');
const inProgress = new Counter('admission_idempotency_in_progress');
const unexpected5xx = new Counter('unexpected_5xx');

function required(name) {
  const value = (__ENV[name] || '').trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function passengerIDs() {
  const values = required('PASSENGER_IDS').split(',').map((value) => value.trim()).filter(Boolean);
  if (values.length === 0 || new Set(values).size !== values.length) {
    throw new Error('PASSENGER_IDS must contain unique IDs');
  }
  return values;
}

export const options = {
  scenarios: {
    same_token_replay: {
      executor: 'shared-iterations',
      vus: Number.parseInt(__ENV.VUS || '20', 10),
      iterations: Number.parseInt(__ENV.ITERATIONS || '100', 10),
      maxDuration: (__ENV.MAX_DURATION || '2m').trim(),
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
    unexpected_5xx: ['count==0'],
    admission_idempotency_mismatches: ['count==0'],
  },
};

export function setup() {
  const config = {
    baseURL: required('BASE_URL').replace(/\/$/, ''),
    customerToken: required('CUSTOMER_TOKEN'),
    admissionToken: required('ADMISSION_TOKEN'),
    idempotencyKey: required('IDEMPOTENCY_KEY'),
    expectedReservationID: required('EXPECTED_RESERVATION_ID'),
    trainRunID: required('TRAIN_RUN_ID'),
    origin: required('ORIGIN_CODE').toUpperCase(),
    destination: required('DESTINATION_CODE').toUpperCase(),
    seatClass: required('SEAT_CLASS').toLowerCase(),
    passengerIDs: passengerIDs(),
  };
  const response = http.get(
    `${config.baseURL}/api/v1/reservations/${encodeURIComponent(config.expectedReservationID)}`,
    { headers: { Authorization: `Bearer ${config.customerToken}` }, tags: { operation: 'setup_reservation_lookup' } },
  );
  if (response.status !== 200) {
    throw new Error('setup contract failed: EXPECTED_RESERVATION_ID must name the already committed replay target');
  }
  return config;
}

export default function (config) {
  const response = http.post(
    `${config.baseURL}/api/v1/reservations`,
    JSON.stringify({
      train_run_id: config.trainRunID,
      origin_station_code: config.origin,
      destination_station_code: config.destination,
      seat_class: config.seatClass,
      passenger_ids: config.passengerIDs,
    }),
    {
      headers: {
        Authorization: `Bearer ${config.customerToken}`,
        'Content-Type': 'application/json',
        'Idempotency-Key': config.idempotencyKey,
        'X-Admission-Token': config.admissionToken,
      },
      tags: { operation: 'admission_idempotency_replay' },
    },
  );
  if (response.status >= 500) unexpected5xx.add(1);
  if (response.status === 409 && response.json('error.code') === 'admission_in_progress') inProgress.add(1);
  let reservationID = '';
  try {
    reservationID = response.json('id') || '';
  } catch (_) {
    reservationID = '';
  }
  if (response.status === 201 && reservationID === config.expectedReservationID) replayed.add(1);
  if (response.status === 201 && reservationID !== config.expectedReservationID) mismatches.add(1);
  check(response, {
    'replay converges or reports bounded in-progress': (r) => [201, 409].includes(r.status),
    'completed replay returns original reservation': () =>
      response.status !== 201 || reservationID === config.expectedReservationID,
    'in-progress retry is bounded': (r) =>
      r.status !== 409 || (Number(r.headers['Retry-After']) >= 1 && Number(r.headers['Retry-After']) <= 60),
  });
}

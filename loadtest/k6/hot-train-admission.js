import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const issued = new Counter('admission_tokens_issued');
const conflicts = new Counter('admission_conflicts');
const unexpected5xx = new Counter('unexpected_5xx');
const admissionWait = new Trend('admission_wait_duration', true);

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

export const options = {
  scenarios: {
    admission: {
      executor: 'per-vu-iterations',
      vus: positive('VUS', 10),
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
  const tokens = required('CUSTOMER_TOKENS').split(',').map((value) => value.trim()).filter(Boolean);
  const config = {
    baseURL: required('BASE_URL').replace(/\/$/, ''),
    tokens,
    trainRunID: required('TRAIN_RUN_ID'),
    origin: required('ORIGIN_CODE').toUpperCase(),
    destination: required('DESTINATION_CODE').toUpperCase(),
    seatClass: required('SEAT_CLASS').toLowerCase(),
    passengerCount: positive('PASSENGER_COUNT', 1),
    pollSeconds: Number(__ENV.POLL_SECONDS || '0.25'),
    maxPolls: positive('MAX_POLLS', 240),
  };
  if (tokens.length < positive('VUS', 10)) {
    throw new Error('setup contract failed: CUSTOMER_TOKENS must provide at least one distinct token per VU');
  }
  const response = http.get(`${config.baseURL}/livez`, { tags: { operation: 'setup_livez' } });
  if (response.status !== 200) throw new Error('setup contract failed: /livez must return 200');
  return config;
}

export default function (config) {
  const customerToken = config.tokens[__VU - 1];
  const started = Date.now();
  const join = http.post(
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
      tags: { operation: 'waiting_room_join' },
    },
  );
  if (join.status >= 500) unexpected5xx.add(1);
  if (join.status === 409) conflicts.add(1);
  let entryID = '';
  try {
    entryID = join.json('entry_id') || '';
  } catch (_) {
    entryID = '';
  }
  const joinOK = check(join, {
    'join accepted': (r) => r.status === 201 && entryID.length > 0,
  });
  if (!joinOK) return;

  for (let attempt = 0; attempt < config.maxPolls; attempt += 1) {
    const status = http.get(
      `${config.baseURL}/api/v1/waiting-room/entries/${encodeURIComponent(entryID)}`,
      {
        headers: { Authorization: `Bearer ${customerToken}` },
        tags: { operation: 'waiting_room_status' },
      },
    );
    if (status.status >= 500) unexpected5xx.add(1);
    if (status.status === 200 && status.json('status') === 'admitted') {
      const rawToken = status.headers['X-Admission-Token'] || '';
      admissionWait.add(Date.now() - started);
      if (rawToken) issued.add(1);
      check(status, {
        'admission token is header-only and non-empty': (r) =>
          rawToken.length > 0 && !String(r.body).includes(rawToken),
        'admission response is not cacheable': (r) => (r.headers['Cache-Control'] || '').includes('no-store'),
      });
      return;
    }
    if ([404, 410].includes(status.status)) return;
    sleep(config.pollSeconds);
  }
  check(null, { 'entry admitted within bounded polling window': () => false });
}

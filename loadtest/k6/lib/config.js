function required(name) {
  const value = (__ENV[name] || '').trim();
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function positiveInteger(name, fallback) {
  const raw = (__ENV[name] || `${fallback}`).trim();
  const value = Number.parseInt(raw, 10);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

export const baseURL = () => required('BASE_URL').replace(/\/$/, '');
export const customerToken = () => required('CUSTOMER_TOKEN');
export const adminToken = () => required('ADMIN_TOKEN');
export const operatorToken = () => required('OPERATOR_TOKEN');
export const trainRunID = () => required('TRAIN_RUN_ID');
export const originCode = () => required('ORIGIN_CODE').toUpperCase();
export const destinationCode = () => required('DESTINATION_CODE').toUpperCase();
export const seatClass = () => required('SEAT_CLASS').toLowerCase();
export const serviceDate = () => required('SERVICE_DATE');
export const idempotencyKey = () => required('IDEMPOTENCY_KEY');
export const duration = () => (__ENV.DURATION || '30s').trim();
export const virtualUsers = () => positiveInteger('VUS', 1);
export const expectedSeatCount = () => {
  const value = Number.parseInt(required('EXPECTED_SEAT_COUNT'), 10);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error('EXPECTED_SEAT_COUNT must be a positive integer');
  }
  return value;
};
export const expirationWaitSeconds = () => positiveInteger('EXPIRATION_WAIT_SECONDS', 20);

export function passengerIDs() {
  const values = required('PASSENGER_IDS')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  if (values.length === 0 || new Set(values).size !== values.length) {
    throw new Error('PASSENGER_IDS must contain unique comma-separated IDs');
  }
  return values;
}

export function baseOptions(extraThresholds = {}) {
  return {
    vus: virtualUsers(),
    duration: duration(),
    summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
    thresholds: {
      unexpected_5xx: ['count==0'],
      checks: ['rate>0.95'],
      ...extraThresholds,
    },
  };
}

export function oneAttemptPerVUOptions(extraThresholds = {}, minimumDurationSeconds = 0) {
  return {
    scenarios: {
      one_attempt_per_vu: {
        executor: 'per-vu-iterations',
        vus: virtualUsers(),
        iterations: 1,
        maxDuration: minimumDurationSeconds > 0 ? `${minimumDurationSeconds}s` : duration(),
      },
    },
    summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
    thresholds: {
      unexpected_5xx: ['count==0'],
      checks: ['rate>0.95'],
      ...extraThresholds,
    },
  };
}

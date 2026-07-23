import http from 'k6/http';
import exec from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';

export const unexpected5xx = new Counter('unexpected_5xx');
export const expectedRebalancing503 = new Counter('expected_rebalancing_503');
export const expectedOutage503 = new Counter('expected_outage_503');
export const allocationConflicts = new Counter('shard_allocation_conflicts');
export const rateLimited = new Counter('shard_rate_limited');
export const routingSuccess = new Counter('shard_routing_success');
export const identityMismatches = new Counter('duplicate_identity_mismatches');
export const partialShardResults = new Counter('partial_shard_results');
export const healthyShardSuccess = new Counter('healthy_shard_success');
export const requestDuration = new Trend('shard_request_duration', true);
export const shardADuration = new Trend('shard_a_duration', true);
export const shardBDuration = new Trend('shard_b_duration', true);
export const legacyDuration = new Trend('legacy_shard_duration', true);
export const schemaDuration = new Trend('schema_shard_duration', true);

export function required(name) {
  const value = (__ENV[name] || '').trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

export function positiveInteger(name, fallback) {
  const raw = (__ENV[name] || `${fallback}`).trim();
  const value = Number.parseInt(raw, 10);
  if (!Number.isInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
  return value;
}

export function enabled(name, fallback = false) {
  const value = (__ENV[name] || (fallback ? 'yes' : 'no')).trim().toLowerCase();
  if (!['yes', 'no'].includes(value)) throw new Error(`${name} must be yes or no`);
  return value === 'yes';
}

export function list(name, minimum = 1) {
  const values = required(name).split(',').map((value) => value.trim()).filter(Boolean);
  if (values.length < minimum) throw new Error(`${name} must contain at least ${minimum} comma-separated values`);
  return values;
}

export function boundedOptions(scenarios, extraThresholds = {}) {
  return {
    scenarios,
    summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
    thresholds: {
      checks: ['rate>0.95'],
      unexpected_5xx: ['count==0'],
      duplicate_identity_mismatches: ['count==0'],
      shard_request_duration: ['p(95)<2000', 'p(99)<5000'],
      ...extraThresholds,
    },
  };
}

export function baseURL() {
  return required('BASE_URL').replace(/\/$/, '');
}

export function twoTrainRunIDs() {
  const values = list('TRAIN_RUN_IDS', 2);
  return [values[0], values[1]];
}

export function bearerHeaders(token) {
  return { Authorization: `Bearer ${token}`, Accept: 'application/json' };
}

export function bookingHeaders(token, key) {
  return {
    ...bearerHeaders(token),
    'Content-Type': 'application/json',
    'Idempotency-Key': key,
  };
}

export function customerForVU() {
  const tokens = list('CUSTOMER_TOKENS');
  const passengers = list('PASSENGER_IDS');
  const index = (exec.vu.idInTest - 1) % Math.min(tokens.length, passengers.length);
  return { token: tokens[index], passengerID: passengers[index] };
}

export function iterationKey(prefix) {
  return `${prefix}-${exec.scenario.name}-${exec.vu.idInTest}-${exec.scenario.iterationInTest}`;
}

export function publicErrorCode(response) {
  try {
    const value = response.json('error.code');
    return typeof value === 'string' ? value : 'missing';
  } catch (_) {
    return 'unparseable';
  }
}

export function recordResponse(response, options = {}) {
  requestDuration.add(response.timings.duration);
  if (options.trend) options.trend.add(response.timings.duration);
  if (response.status === 409) allocationConflicts.add(1);
  if (response.status === 429) rateLimited.add(1);
  if (response.status >= 500) {
    const code = publicErrorCode(response);
    if (response.status === 503 && options.allowRebalancing && code === 'service_temporarily_rebalancing') {
      expectedRebalancing503.add(1);
      return { expected503: true, code };
    }
    if (response.status === 503 && options.allowOutage && code === 'unavailable') {
      expectedOutage503.add(1);
      return { expected503: true, code };
    }
    unexpected5xx.add(1);
    return { expected503: false, code };
  }
  return { expected503: false, code: 'none' };
}

export function availabilityURL(url, trainRunID) {
  const query = [
    `origin_station_code=${encodeURIComponent(required('ORIGIN_CODE').toUpperCase())}`,
    `destination_station_code=${encodeURIComponent(required('DESTINATION_CODE').toUpperCase())}`,
    `seat_class=${encodeURIComponent(required('SEAT_CLASS').toLowerCase())}`,
  ].join('&');
  return `${url}/api/v1/train-runs/${encodeURIComponent(trainRunID)}/availability?${query}`;
}

export function readAvailability(url, trainRunID, options = {}) {
  const response = http.get(availabilityURL(url, trainRunID), {
    tags: { operation: options.operation || 'shard_availability' },
  });
  recordResponse(response, options);
  if (response.status === 200) routingSuccess.add(1);
  return response;
}

export function createHold(url, trainRunID, customer, key, options = {}) {
  const response = http.post(
    `${url}/api/v1/reservations`,
    JSON.stringify({
      train_run_id: trainRunID,
      origin_station_code: required('ORIGIN_CODE').toUpperCase(),
      destination_station_code: required('DESTINATION_CODE').toUpperCase(),
      seat_class: required('SEAT_CLASS').toLowerCase(),
      passenger_ids: [customer.passengerID],
    }),
    {
      headers: bookingHeaders(customer.token, key),
      tags: { operation: options.operation || 'shard_reservation_create' },
    },
  );
  recordResponse(response, options);
  if (response.status === 201) routingSuccess.add(1);
  return response;
}

export function reservationID(response) {
  if (response.status !== 200 && response.status !== 201) return '';
  try {
    return response.json('id') || '';
  } catch (_) {
    return '';
  }
}

export function getReservation(url, reservationIDValue, token, options = {}) {
  const response = http.get(`${url}/api/v1/reservations/${encodeURIComponent(reservationIDValue)}`, {
    headers: bearerHeaders(token),
    tags: { operation: options.operation || 'shard_reservation_get' },
  });
  recordResponse(response, options);
  if (response.status === 200) routingSuccess.add(1);
  return response;
}

export function listTicketOrders(url, token, options = {}) {
  const response = http.get(`${url}/api/v1/ticket-orders?page=1&limit=100&sort=-created_at`, {
    headers: bearerHeaders(token),
    tags: { operation: options.operation || 'cross_shard_ticket_orders' },
  });
  recordResponse(response, options);
  return response;
}

export function bookingStatusAllowed(response, allowRebalancing = false) {
  if ([201, 409, 429].includes(response.status)) return true;
  return allowRebalancing && response.status === 503 && publicErrorCode(response) === 'service_temporarily_rebalancing';
}

export function availabilityCountIsValid(response) {
  if (response.status !== 200) return false;
  try {
    const value = Number(response.json('available_seat_count'));
    return Number.isInteger(value) && value >= 0;
  } catch (_) {
    return false;
  }
}


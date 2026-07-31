import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

import {
  baseURL,
  bookingHeaders,
  boundedOptions,
  customerForVU,
  positiveInteger,
  publicErrorCode,
  requestDuration,
  required,
} from './lib/milestone4.js';

export const unexpected5xx = new Counter('unexpected_5xx');
export const commandRecoverySuccess = new Counter('command_recovery_success');
export const duplicateCommandObservations = new Counter('duplicate_command_observations');
export const commandRepairDuration = new Trend('command_repair_duration', true);

const maximumRepairMilliseconds = positiveInteger('MAX_REPAIR_MS', 30000);

export const options = boundedOptions({
  command_recovery: {
    executor: 'per-vu-iterations',
    vus: 1,
    iterations: 1,
    maxDuration: (__ENV.MAX_DURATION || '45s').trim(),
  },
}, {
  checks: ['rate==1'],
  command_recovery_success: ['count==1'],
  duplicate_command_observations: ['count==0'],
  command_repair_duration: [`p(95)<${maximumRepairMilliseconds}`],
});

function intervalSeconds() {
  const value = Number.parseFloat((__ENV.REPLAY_INTERVAL_SECONDS || '0.25').trim());
  if (!Number.isFinite(value) || value <= 0 || value > 5) {
    throw new Error('REPLAY_INTERVAL_SECONDS must be greater than 0 and at most 5');
  }
  return value;
}

function reservationID(response) {
  if (response.status !== 200 && response.status !== 201) return '';
  try {
    return response.json('id') || '';
  } catch (_) {
    return '';
  }
}

function post(config) {
  return http.post(
    `${config.url}/api/v1/reservations`,
    JSON.stringify(config.payload),
    {
      headers: bookingHeaders(config.customer.token, config.key),
      tags: { operation: 'booking_command_recovery' },
      timeout: '10s',
    },
  );
}

function isExpectedDeferred(response, code) {
  return response.status === 503 && publicErrorCode(response) === code;
}

export default function () {
  const customer = customerForVU();
  const deferredCode = (__ENV.DEFERRED_ERROR_CODE || 'service_temporarily_rebalancing').trim();
  const config = {
    url: baseURL(),
    customer,
    key: required('IDEMPOTENCY_KEY'),
    payload: {
      train_run_id: required('TRAIN_RUN_ID'),
      origin_station_code: required('ORIGIN_CODE').toUpperCase(),
      destination_station_code: required('DESTINATION_CODE').toUpperCase(),
      seat_class: required('SEAT_CLASS').toLowerCase(),
      passenger_ids: [customer.passengerID],
    },
  };
  const startedAt = Date.now();
  const first = post(config);
  requestDuration.add(first.timings.duration);
  if (first.status >= 500 && !isExpectedDeferred(first, deferredCode)) unexpected5xx.add(1);
  check(first, {
    'failure hook exposes the bounded deferred-finalization contract': (value) =>
      isExpectedDeferred(value, deferredCode),
  });

  let recoveredID = '';
  const attempts = positiveInteger('MAX_REPLAY_ATTEMPTS', 20);
  for (let attempt = 0; attempt < attempts && !recoveredID; attempt += 1) {
    sleep(intervalSeconds());
    const replay = post(config);
    requestDuration.add(replay.timings.duration);
    if (replay.status >= 500 && !isExpectedDeferred(replay, deferredCode)) unexpected5xx.add(1);
    const currentID = reservationID(replay);
    if (recoveredID && currentID && recoveredID !== currentID) duplicateCommandObservations.add(1);
    if (currentID) recoveredID = currentID;
  }

  if (recoveredID) {
    commandRecoverySuccess.add(1);
    commandRepairDuration.add(Date.now() - startedAt);
  }
  const finalReplay = post(config);
  requestDuration.add(finalReplay.timings.duration);
  if (finalReplay.status >= 500 && !isExpectedDeferred(finalReplay, deferredCode)) unexpected5xx.add(1);
  const finalID = reservationID(finalReplay);
  if (recoveredID && finalID && recoveredID !== finalID) duplicateCommandObservations.add(1);
  check({ recoveredID, finalReplay, finalID }, {
    'bounded replay converges to one reservation identity': (value) => value.recoveredID.length > 0,
    'post-repair replay preserves the recovered identity': (value) =>
      value.finalReplay.status === 201 && value.finalID === value.recoveredID,
  });
}

// The controller must hold and release the finalization failure hook. Post-run
// control/shard reconciliation proves one local receipt and one directory row.

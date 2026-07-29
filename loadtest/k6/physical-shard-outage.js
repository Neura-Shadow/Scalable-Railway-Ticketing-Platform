import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

import {
  baseURL,
  bookingStatusAllowed,
  boundedOptions,
  createHold,
  customerForVU,
  expectedOutage503,
  healthyShardSuccess,
  iterationKey,
  positiveInteger,
  publicErrorCode,
  required,
} from './lib/milestone4.js';

export const outageFallbackWriterObservations = new Counter('outage_fallback_writer_observations');

const duration = (__ENV.DURATION || '15s').trim();
const vus = positiveInteger('VUS_PER_SHARD', 3);

export const options = boundedOptions({
  unavailable_physical_shard: {
    executor: 'constant-vus', exec: 'unavailableShard', vus, duration, gracefulStop: '5s',
  },
  healthy_physical_shard: {
    executor: 'constant-vus', exec: 'healthyShard', vus, duration, gracefulStop: '5s',
  },
}, {
  checks: ['rate==1'],
  expected_outage_503: ['count>0'],
  healthy_shard_success: ['count>0'],
  outage_fallback_writer_observations: ['count==0'],
});

export function unavailableShard() {
  const response = createHold(
    baseURL(),
    required('OUTAGE_TRAIN_RUN_ID'),
    customerForVU(),
    iterationKey('m5-outage'),
    { operation: 'physical_shard_outage', allowOutage: true },
  );
  if (response.status === 201) outageFallbackWriterObservations.add(1);
  check(response, {
    'assigned failed shard returns the bounded unavailable contract': (value) =>
      value.status === 503 && publicErrorCode(value) === 'unavailable',
  });
  sleep(0.1);
}

export function healthyShard() {
  const response = createHold(
    baseURL(),
    required('HEALTHY_TRAIN_RUN_ID'),
    customerForVU(),
    iterationKey('m5-healthy-peer'),
    { operation: 'physical_shard_healthy_peer' },
  );
  if (bookingStatusAllowed(response, false)) healthyShardSuccess.add(1);
  check(response, {
    'healthy physical shard remains bounded during peer failure': (value) =>
      bookingStatusAllowed(value, false),
  });
  sleep(0.1);
}

void expectedOutage503;

// A successful unavailable-shard write is a client-visible fallback symptom.
// Database fence and dual-writer reconciliation remain mandatory after the run.

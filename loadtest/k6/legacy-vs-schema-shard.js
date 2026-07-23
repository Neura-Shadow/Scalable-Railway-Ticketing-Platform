import { check, sleep } from 'k6';

import {
  availabilityCountIsValid,
  baseURL,
  boundedOptions,
  legacyDuration,
  positiveInteger,
  readAvailability,
  required,
  schemaDuration,
} from './lib/milestone4.js';

const vus = positiveInteger('VUS_PER_SHARD', 4);
const duration = (__ENV.DURATION || '15s').trim();

export const options = boundedOptions({
  legacy: { executor: 'constant-vus', exec: 'legacy', vus, duration, gracefulStop: '5s' },
  schema: { executor: 'constant-vus', exec: 'schema', vus, duration, gracefulStop: '5s' },
}, {
  legacy_shard_duration: ['p(95)<2000', 'p(99)<5000'],
  schema_shard_duration: ['p(95)<2000', 'p(99)<5000'],
});

export function legacy() {
  const response = readAvailability(baseURL(), required('LEGACY_TRAIN_RUN_ID'), {
    operation: 'legacy_shard_availability', trend: legacyDuration,
  });
  check(response, { 'legacy shard remains healthy': availabilityCountIsValid });
  sleep(0.1);
}

export function schema() {
  const response = readAvailability(baseURL(), required('SCHEMA_TRAIN_RUN_ID'), {
    operation: 'schema_shard_availability', trend: schemaDuration,
  });
  check(response, { 'schema shard remains healthy': availabilityCountIsValid });
  sleep(0.1);
}


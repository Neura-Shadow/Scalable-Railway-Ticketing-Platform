import { check } from 'k6';

import {
  baseURL,
  boundedOptions,
  createHold,
  customerForVU,
  iterationKey,
  required,
} from './lib/milestone4.js';

export const options = boundedOptions({
  prewarm: {
    executor: 'shared-iterations',
    vus: 1,
    iterations: 1,
    maxDuration: '30s',
  },
}, {
  shard_routing_success: ['count==1'],
});

export default function () {
  const response = createHold(
    baseURL(),
    required('TRAIN_RUN_ID'),
    customerForVU(),
    iterationKey('m4-prewarm'),
    { operation: 'route_cache_prewarm' },
  );
  check(response, {
    'dedicated prewarm request creates a routed hold': (value) => value.status === 201,
  });
}

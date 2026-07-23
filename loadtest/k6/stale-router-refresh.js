import { check, sleep } from 'k6';

import {
  bookingStatusAllowed,
  boundedOptions,
  createHold,
  customerForVU,
  iterationKey,
  list,
  positiveInteger,
  required,
} from './lib/milestone4.js';

export const options = boundedOptions({
  stale_refresh: {
    executor: 'shared-iterations',
    vus: positiveInteger('VUS', 9),
    iterations: positiveInteger('ITERATIONS', 36),
    maxDuration: (__ENV.MAX_DURATION || '90s').trim(),
  },
});

export default function () {
  const urls = list('API_URLS', 3).map((value) => value.replace(/\/$/, ''));
  const url = urls[(__VU - 1) % urls.length];
  const response = createHold(url, required('TRAIN_RUN_ID'), customerForVU(), iterationKey('m4-stale'), {
    operation: 'stale_router_refresh',
  });
  check(response, {
    'stale router refresh is transparent to the customer': (value) => bookingStatusAllowed(value, false),
    'stale router refresh does not leak a rebalancing 503': (value) => value.status !== 503,
  });
  sleep(0.1);
}


import { check, sleep } from 'k6';

import {
  baseURL,
  bookingStatusAllowed,
  boundedOptions,
  createHold,
  customerForVU,
  iterationKey,
  positiveInteger,
  required,
} from './lib/milestone4.js';

export const options = boundedOptions({
  cutover_overlap: {
    executor: 'constant-vus',
    vus: positiveInteger('VUS', 8),
    duration: (__ENV.DURATION || '20s').trim(),
    gracefulStop: '5s',
  },
});

export default function () {
  const response = createHold(baseURL(), required('TRAIN_RUN_ID'), customerForVU(), iterationKey('m4-cutover'), {
    operation: 'cutover_overlap_create', allowRebalancing: true,
  });
  check(response, {
    'cutover workload succeeds or receives explicit rebalancing response': (value) => bookingStatusAllowed(value, true),
  });
  sleep(0.1);
}


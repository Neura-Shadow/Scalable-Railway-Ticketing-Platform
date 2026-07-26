import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { Trend } from 'k6/metrics';

import {
  baseURL,
  bookingStatusAllowed,
  boundedOptions,
  createHold,
  customerForVU,
  iterationKey,
  positiveInteger,
  publicErrorCode,
  required,
} from './lib/milestone4.js';

export const cutoverRejectionElapsed = new Trend('cutover_rejection_elapsed_ms', true);

export const options = boundedOptions({
  cutover_overlap: {
    executor: 'constant-vus',
    vus: positiveInteger('VUS', 8),
    duration: (__ENV.DURATION || '20s').trim(),
    gracefulStop: '5s',
  },
}, {
  shard_routing_success: ['count>0'],
  booking_success_duration: ['p(95)<2000', 'p(99)<5000'],
});

export default function () {
  const response = createHold(baseURL(), required('TRAIN_RUN_ID'), customerForVU(), iterationKey('m4-cutover'), {
    operation: 'cutover_overlap_create', allowRebalancing: true,
  });
  if (response.status === 503 && publicErrorCode(response) === 'service_temporarily_rebalancing') {
    cutoverRejectionElapsed.add(exec.instance.currentTestRunDuration);
  }
  check(response, {
    'cutover workload succeeds or receives explicit rebalancing response': (value) => bookingStatusAllowed(value, true),
  });
  // Keep each synthetic identity below the one-minute booking-rate ceiling
  // long enough for the independently launched cutover transaction to finish.
  sleep(1);
}

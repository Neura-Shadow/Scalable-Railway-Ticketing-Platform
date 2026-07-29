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
export const cutoverPauseObservations = new Counter('cutover_pause_observations');
export const postCutoverSuccess = new Counter('post_cutover_success');
export const cutoverSplitBrainObservations = new Counter('cutover_split_brain_observations');
export const physicalCutoverPauseDuration = new Trend('physical_cutover_pause_duration', true);

const maximumPauseMilliseconds = positiveInteger('MAX_PAUSE_MS', 30000);

export const options = boundedOptions({
  cutover_overlap: {
    executor: 'per-vu-iterations',
    vus: positiveInteger('VUS', 6),
    iterations: positiveInteger('ITERATIONS_PER_VU', 8),
    maxDuration: (__ENV.DURATION || '30s').trim(),
    gracefulStop: '5s',
  },
}, {
  cutover_pause_observations: ['count>0'],
  post_cutover_success: ['count>0'],
  cutover_split_brain_observations: ['count==0'],
  physical_cutover_pause_duration: [`p(95)<${maximumPauseMilliseconds}`],
});

let pauseStartedAt = 0;
let observedReservationID = '';

function responseID(response) {
  if (response.status !== 200 && response.status !== 201) return '';
  try {
    return response.json('id') || '';
  } catch (_) {
    return '';
  }
}

export default function () {
  const customer = customerForVU();
  const response = http.post(
    `${baseURL()}/api/v1/reservations`,
    JSON.stringify({
      train_run_id: required('TRAIN_RUN_ID'),
      origin_station_code: required('ORIGIN_CODE').toUpperCase(),
      destination_station_code: required('DESTINATION_CODE').toUpperCase(),
      seat_class: required('SEAT_CLASS').toLowerCase(),
      passenger_ids: [customer.passengerID],
    }),
    {
      headers: bookingHeaders(customer.token, `m5-cutover-${__VU}`),
      tags: { operation: 'physical_cutover_overlap' },
      timeout: '10s',
    },
  );
  requestDuration.add(response.timings.duration);
  const rebalancing = response.status === 503
    && publicErrorCode(response) === 'service_temporarily_rebalancing';
  if (response.status >= 500 && !rebalancing) unexpected5xx.add(1);

  if (rebalancing && pauseStartedAt === 0) pauseStartedAt = Date.now();
  const currentID = responseID(response);
  if (observedReservationID && currentID && observedReservationID !== currentID) {
    cutoverSplitBrainObservations.add(1);
  }
  if (currentID) observedReservationID = currentID;
  if (pauseStartedAt > 0 && currentID) {
    physicalCutoverPauseDuration.add(Date.now() - pauseStartedAt);
    cutoverPauseObservations.add(1);
    postCutoverSuccess.add(1);
    pauseStartedAt = 0;
  }

  check(response, {
    'cutover request succeeds or receives explicit rebalancing pause': (value) =>
      value.status === 201 || (value.status === 503
        && publicErrorCode(value) === 'service_temporarily_rebalancing'),
  });
  sleep(1.5);
}

// The external controller proves source-disable precedes target-enable. This
// client trend starts on explicit 503 and stops on the first successful replay.

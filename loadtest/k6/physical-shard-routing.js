import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

import {
  baseURL,
  bookingHeaders,
  boundedOptions,
  createHold,
  customerForVU,
  identityMismatches,
  iterationKey,
  list,
  positiveInteger,
  publicErrorCode,
  required,
  reservationID,
  twoTrainRunIDs,
} from './lib/milestone4.js';

export const physicalRouteSuccess = new Counter('physical_route_success');
export const physicalRouteConflicts = new Counter('physical_route_conflicts');
export const clientSplitBrainObservations = new Counter('client_split_brain_observations');
export const sameIdempotencyRequests = new Counter('same_idempotency_requests');
export const sameIdempotencyTerminalResponses = new Counter('same_idempotency_terminal_responses');
export const sameIdempotencyDeferredResponses = new Counter('same_idempotency_deferred_responses');
export const sameIdempotencyConvergedAPIs = new Counter('same_idempotency_converged_apis');
export const sameIdempotencyIdentityMismatches = new Counter('same_idempotency_identity_mismatches');
export const sameIdempotencyUnexpectedResponses = new Counter('same_idempotency_unexpected_responses');

const routingIterations = positiveInteger('ITERATIONS', 48);
const minimumRouteSuccess = positiveInteger('MIN_ROUTE_SUCCESS', 2);
if (minimumRouteSuccess > routingIterations) throw new Error('MIN_ROUTE_SUCCESS cannot exceed ITERATIONS');

const routingOptions = boundedOptions({
  physical_routing: {
    executor: 'shared-iterations',
    vus: positiveInteger('VUS', 8),
    iterations: routingIterations,
    maxDuration: (__ENV.MAX_DURATION || '2m').trim(),
  },
  same_idempotency_physical: {
    executor: 'per-vu-iterations',
    vus: 1,
    iterations: 1,
    maxDuration: (__ENV.MAX_DURATION || '2m').trim(),
  },
}, {
  physical_route_success: [`count>=${minimumRouteSuccess}`],
  physical_route_conflicts: [`count<=${routingIterations - minimumRouteSuccess}`],
  shard_rate_limited: ['count==0'],
  client_split_brain_observations: ['count==0'],
  same_idempotency_requests: ['count==100'],
  same_idempotency_terminal_responses: ['count>=1'],
  same_idempotency_converged_apis: ['count==3'],
  same_idempotency_identity_mismatches: ['count==0'],
  same_idempotency_unexpected_responses: ['count==0'],
  booking_success_duration: ['p(95)<2000', 'p(99)<5000'],
});
routingOptions.batch = 100;
routingOptions.batchPerHost = 100;
export const options = routingOptions;

function concurrentPhysicalIdentity() {
  const apiURLs = list('API_URLS', 3).slice(0, 3).map((value) => value.replace(/\/$/, ''));
  const token = required('CONCURRENT_CUSTOMER_TOKEN');
  const passengerID = required('CONCURRENT_PASSENGER_ID');
  const trainRunID = required('CONCURRENT_TRAIN_RUN_ID');
  const key = required('CONCURRENT_IDEMPOTENCY_KEY');
  const payload = JSON.stringify({
    train_run_id: trainRunID,
    origin_station_code: required('ORIGIN_CODE').toUpperCase(),
    destination_station_code: required('DESTINATION_CODE').toUpperCase(),
    seat_class: required('SEAT_CLASS').toLowerCase(),
    passenger_ids: [passengerID],
  });
  const requests = Array.from({ length: 100 }, (_, index) => ({
    method: 'POST',
    url: `${apiURLs[index % apiURLs.length]}/api/v1/reservations`,
    body: payload,
    params: {
      headers: bookingHeaders(token, key),
      tags: { operation: 'same_idempotency_physical_batch' },
      timeout: '10s',
    },
  }));
  const responses = http.batch(requests);
  sameIdempotencyRequests.add(responses.length);
  const observedIDs = new Set();
  for (const response of responses) {
    const id = reservationID(response);
    if (response.status === 201 && id) {
      observedIDs.add(id);
      sameIdempotencyTerminalResponses.add(1);
      continue;
    }
    const boundedDeferred = (response.status === 409 && publicErrorCode(response) === 'conflict')
      || (response.status === 503 && publicErrorCode(response) === 'unavailable');
    if (boundedDeferred) sameIdempotencyDeferredResponses.add(1);
    else sameIdempotencyUnexpectedResponses.add(1);
  }
  if (observedIDs.size > 1) sameIdempotencyIdentityMismatches.add(observedIDs.size - 1);

  let authoritativeID = observedIDs.size === 1 ? [...observedIDs][0] : '';
  for (const apiURL of apiURLs) {
    let recoveredID = '';
    for (let attempt = 0; attempt < 20 && !recoveredID; attempt += 1) {
      const response = http.post(`${apiURL}/api/v1/reservations`, payload, {
        headers: bookingHeaders(token, key),
        tags: { operation: 'same_idempotency_physical_convergence' },
        timeout: '10s',
      });
      recoveredID = reservationID(response);
      if (!recoveredID) sleep(0.1);
    }
    if (recoveredID) {
      if (authoritativeID && recoveredID !== authoritativeID) sameIdempotencyIdentityMismatches.add(1);
      if (!authoritativeID) authoritativeID = recoveredID;
      sameIdempotencyConvergedAPIs.add(1);
    }
  }
  check({ responses, authoritativeID }, {
    'exactly 100 same-idempotency requests reached the three physical API replicas': (value) =>
      value.responses.length === 100,
    'all three replicas converge on one physical reservation identity': (value) =>
      value.authoritativeID.length > 0,
  });
}

export default function () {
  if (exec.scenario.name === 'same_idempotency_physical') {
    concurrentPhysicalIdentity();
    return;
  }
  const trainRuns = twoTrainRunIDs();
  const trainRunID = trainRuns[__ITER % trainRuns.length];
  const customer = customerForVU();
  const key = iterationKey('m5-physical-route');
  const first = createHold(baseURL(), trainRunID, customer, key, {
    operation: 'physical_route_create',
  });
  const replay = createHold(baseURL(), trainRunID, customer, key, {
    operation: 'physical_route_replay',
  });
  const firstID = reservationID(first);
  const replayID = reservationID(replay);

  if (firstID && replayID && firstID !== replayID) {
    identityMismatches.add(1);
    clientSplitBrainObservations.add(1);
  }
  const succeeded = first.status === 201 && replay.status === 201 && firstID && firstID === replayID;
  const conflicted = first.status === 409 && replay.status === 409
    && publicErrorCode(first) === 'conflict' && publicErrorCode(replay) === 'conflict';
  if (succeeded) {
    physicalRouteSuccess.add(1);
  }
  if (conflicted) physicalRouteConflicts.add(1);

  check({ first, replay, firstID, replayID }, {
    'physical route produces a committed replay or one typed conflict': () => succeeded || conflicted,
    'successful physical replay preserves one reservation identity': (value) =>
      !value.firstID || !value.replayID || value.firstID === value.replayID,
  });
  sleep(0.1);
}

// Database-local fences and post-run source/target reconciliation remain the
// authoritative split-brain proof; this metric is deliberately client-visible only.

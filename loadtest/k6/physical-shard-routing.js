import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

import {
  baseURL,
  boundedOptions,
  createHold,
  customerForVU,
  identityMismatches,
  iterationKey,
  positiveInteger,
  publicErrorCode,
  reservationID,
  twoTrainRunIDs,
} from './lib/milestone4.js';

export const physicalRouteSuccess = new Counter('physical_route_success');
export const physicalRouteConflicts = new Counter('physical_route_conflicts');
export const clientSplitBrainObservations = new Counter('client_split_brain_observations');

const routingIterations = positiveInteger('ITERATIONS', 48);
const minimumRouteSuccess = positiveInteger('MIN_ROUTE_SUCCESS', 2);
if (minimumRouteSuccess > routingIterations) throw new Error('MIN_ROUTE_SUCCESS cannot exceed ITERATIONS');

export const options = boundedOptions({
  physical_routing: {
    executor: 'shared-iterations',
    vus: positiveInteger('VUS', 8),
    iterations: routingIterations,
    maxDuration: (__ENV.MAX_DURATION || '2m').trim(),
  },
}, {
  physical_route_success: [`count>=${minimumRouteSuccess}`],
  physical_route_conflicts: [`count<=${routingIterations - minimumRouteSuccess}`],
  shard_rate_limited: ['count==0'],
  client_split_brain_observations: ['count==0'],
  booking_success_duration: ['p(95)<2000', 'p(99)<5000'],
});

export default function () {
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

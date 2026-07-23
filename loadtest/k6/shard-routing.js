import { check, sleep } from 'k6';

import {
  baseURL,
  bookingStatusAllowed,
  boundedOptions,
  createHold,
  customerForVU,
  enabled,
  identityMismatches,
  iterationKey,
  positiveInteger,
  reservationID,
  twoTrainRunIDs,
} from './lib/milestone4.js';

export const options = boundedOptions({
  routing: {
    executor: 'shared-iterations',
    vus: positiveInteger('VUS', 8),
    iterations: positiveInteger('ITERATIONS', 48),
    maxDuration: (__ENV.MAX_DURATION || '2m').trim(),
  },
});

export default function () {
  const runs = twoTrainRunIDs();
  const trainRunID = runs[__ITER % runs.length];
  const customer = customerForVU();
  const key = iterationKey('m4-route');
  const allowRebalancing = enabled('ALLOW_REBALANCING_503');
  const first = createHold(baseURL(), trainRunID, customer, key, {
    operation: 'shard_route_create', allowRebalancing,
  });
  const replay = createHold(baseURL(), trainRunID, customer, key, {
    operation: 'shard_route_replay', allowRebalancing,
  });
  const firstID = reservationID(first);
  const replayID = reservationID(replay);
  if (firstID && replayID && firstID !== replayID) identityMismatches.add(1);
  check({ first, replay }, {
    'routed booking returns a bounded public outcome': ({ first: value }) => bookingStatusAllowed(value, allowRebalancing),
    'idempotent replay returns a bounded public outcome': ({ replay: value }) => bookingStatusAllowed(value, allowRebalancing),
    'successful replay preserves reservation identity': () => !firstID || !replayID || firstID === replayID,
  });
  sleep(0.1);
}

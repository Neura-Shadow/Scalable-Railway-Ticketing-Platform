import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

import {
  availabilityCountIsValid,
  baseURL,
  boundedOptions,
  createHold,
  customerForVU,
  identityMismatches,
  positiveInteger,
  readAvailability,
  required,
  reservationID,
} from './lib/milestone4.js';

export const baseCopySourceSuccess = new Counter('base_copy_source_success');
export const baseCopyDuplicateObservations = new Counter('base_copy_duplicate_observations');
export const baseCopySourceDuration = new Trend('base_copy_source_duration', true);

export const options = boundedOptions({
  source_while_copying: {
    executor: 'per-vu-iterations',
    vus: positiveInteger('VUS', 4),
    iterations: positiveInteger('ITERATIONS_PER_VU', 2),
    maxDuration: (__ENV.DURATION || '20s').trim(),
    gracefulStop: '5s',
  },
}, {
  base_copy_source_success: ['count>=2'],
  base_copy_duplicate_observations: ['count==0'],
  base_copy_source_duration: ['p(95)<2000', 'p(99)<5000'],
});

export default function () {
  const url = baseURL();
  const trainRunID = required('TRAIN_RUN_ID');
  const customer = customerForVU();
  const availability = readAvailability(url, trainRunID, {
    operation: 'base_copy_source_availability', trend: baseCopySourceDuration,
  });
  const key = `m5-base-copy-${__VU}`;
  const created = createHold(url, trainRunID, customer, key, {
    operation: 'base_copy_source_create', trend: baseCopySourceDuration,
  });
  const replay = createHold(url, trainRunID, customer, key, {
    operation: 'base_copy_source_replay', trend: baseCopySourceDuration,
  });
  const createdID = reservationID(created);
  const replayID = reservationID(replay);
  if (createdID && replayID && createdID !== replayID) {
    identityMismatches.add(1);
    baseCopyDuplicateObservations.add(1);
  }
  const committedReplay = created.status === 201 && replay.status === 201
    && createdID.length > 0 && createdID === replayID;
  if (availability.status === 200 && committedReplay) baseCopySourceSuccess.add(1);

  check({ availability, created, replay, createdID, replayID }, {
    'source availability remains readable during base copy': (value) => availabilityCountIsValid(value.availability),
    'source mutation commits while copy advances': (value) => value.created.status === 201,
    'source mutation replay commits while copy advances': (value) => value.replay.status === 201,
    'base-copy workload observes one idempotent identity': (value) =>
      value.createdID.length > 0 && value.createdID === value.replayID,
  });
  sleep(0.5);
}

// The external controller owns snapshot/copy checkpoints and proves progress;
// this client only measures source service while that bounded copy is active.

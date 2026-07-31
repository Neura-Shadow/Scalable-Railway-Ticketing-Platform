import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

import {
  availabilityCountIsValid,
  baseURL,
  boundedOptions,
  createHold,
  identityMismatches,
  list,
  positiveInteger,
  readAvailability,
  reservationID,
  required,
} from './lib/milestone4.js';

export const legacyPathSuccess = new Counter('legacy_path_success');
export const physicalPathSuccess = new Counter('physical_path_success');
export const comparisonDuplicateObservations = new Counter('comparison_duplicate_observations');
export const legacyPathDuration = new Trend('legacy_path_duration', true);
export const physicalPathDuration = new Trend('physical_path_duration', true);

const vus = positiveInteger('VUS_PER_PATH', 4);
const maxDuration = (__ENV.DURATION || '15s').trim();
const iterations = positiveInteger('ITERATIONS_PER_VU', 2);

export const options = boundedOptions({
  legacy_path: { executor: 'per-vu-iterations', exec: 'legacy', vus, iterations, maxDuration, gracefulStop: '5s' },
  physical_path: { executor: 'per-vu-iterations', exec: 'physical', vus, iterations, maxDuration, gracefulStop: '5s' },
}, {
  legacy_path_success: ['count>=2'],
  physical_path_success: ['count>=2'],
  comparison_duplicate_observations: ['count==0'],
  legacy_path_duration: ['p(95)<2000', 'p(99)<5000'],
  physical_path_duration: ['p(95)<2000', 'p(99)<5000'],
});

function customer(prefix) {
  const tokens = list(`${prefix}_CUSTOMER_TOKENS`);
  const passengers = list(`${prefix}_PASSENGER_IDS`);
  const index = (__VU - 1) % Math.min(tokens.length, passengers.length);
  return { token: tokens[index], passengerID: passengers[index] };
}

function exercise(label, trainRunID, participant, trend, successCounter) {
  const url = baseURL();
  const availability = readAvailability(url, trainRunID, {
    operation: `${label}_availability`, trend,
  });
  const key = `m5-${label}-${__VU}`;
  const first = createHold(url, trainRunID, participant, key, {
    operation: `${label}_create`, trend,
  });
  const replay = createHold(url, trainRunID, participant, key, {
    operation: `${label}_replay`, trend,
  });
  const firstID = reservationID(first);
  const replayID = reservationID(replay);
  if (firstID && replayID && firstID !== replayID) {
    identityMismatches.add(1);
    comparisonDuplicateObservations.add(1);
  }
  const committedReplay = first.status === 201 && replay.status === 201
    && firstID.length > 0 && firstID === replayID;
  // Each VU contributes at most one committed identity. Repeated replay loops
  // cannot satisfy the cross-path minimum by themselves.
  if (__ITER === 0 && availability.status === 200 && committedReplay) successCounter.add(1);

  check({ availability, first, replay, firstID, replayID }, {
    [`${label} availability remains valid`]: (value) => availabilityCountIsValid(value.availability),
    [`${label} booking commits successfully`]: (value) => value.first.status === 201,
    [`${label} replay commits successfully`]: (value) => value.replay.status === 201,
    [`${label} successful replay preserves identity`]: (value) =>
      value.firstID.length > 0 && value.firstID === value.replayID,
  });
  sleep(0.1);
}

export function legacy() {
  exercise(
    'legacy_path', required('LEGACY_TRAIN_RUN_ID'), customer('LEGACY'), legacyPathDuration, legacyPathSuccess,
  );
}

export function physical() {
  exercise(
    'physical_path', required('PHYSICAL_TRAIN_RUN_ID'), customer('PHYSICAL'), physicalPathDuration, physicalPathSuccess,
  );
}

// These separate distributions are compatibility observations, not a capacity
// comparison or an extrapolation to a production topology.

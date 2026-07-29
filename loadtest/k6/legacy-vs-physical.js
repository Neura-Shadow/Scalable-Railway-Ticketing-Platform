import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

import {
  availabilityCountIsValid,
  baseURL,
  bookingStatusAllowed,
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
const duration = (__ENV.DURATION || '15s').trim();

export const options = boundedOptions({
  legacy_path: { executor: 'constant-vus', exec: 'legacy', vus, duration, gracefulStop: '5s' },
  physical_path: { executor: 'constant-vus', exec: 'physical', vus, duration, gracefulStop: '5s' },
}, {
  legacy_path_success: ['count>0'],
  physical_path_success: ['count>0'],
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
  const key = `m5-${label}-${__VU}-${__ITER}`;
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
  if (availability.status === 200 && bookingStatusAllowed(first, false)) successCounter.add(1);

  check({ availability, first, replay, firstID, replayID }, {
    [`${label} availability remains valid`]: (value) => availabilityCountIsValid(value.availability),
    [`${label} booking has a bounded outcome`]: (value) => bookingStatusAllowed(value.first, false),
    [`${label} replay has a bounded outcome`]: (value) => bookingStatusAllowed(value.replay, false),
    [`${label} successful replay preserves identity`]: (value) =>
      !value.firstID || !value.replayID || value.firstID === value.replayID,
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

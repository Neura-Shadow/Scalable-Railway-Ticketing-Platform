import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

import {
  baseURL,
  boundedOptions,
  createHold,
  getReservation,
  identityMismatches,
  list,
  required,
  reservationID,
} from './lib/milestone4.js';

export const reverseMigrationPreserved = new Counter('reverse_migration_preserved');
export const reverseMigrationDuplicateObservations = new Counter('reverse_migration_duplicate_observations');
export const reverseMigrationRecoveryDuration = new Trend('reverse_migration_recovery_duration', true);

export const options = boundedOptions({
  verify_target_era_write: {
    executor: 'per-vu-iterations', vus: 1, iterations: 1, maxDuration: '30s',
  },
}, {
  checks: ['rate==1'],
  reverse_migration_preserved: ['count==1'],
  reverse_migration_duplicate_observations: ['count==0'],
  reverse_migration_recovery_duration: ['p(95)<5000'],
});

export default function () {
  const url = baseURL();
  const expectedID = required('TARGET_ERA_RESERVATION_ID');
  const customer = {
    token: list('CUSTOMER_TOKENS')[0],
    passengerID: list('PASSENGER_IDS')[0],
  };
  const replay = createHold(url, required('TRAIN_RUN_ID'), customer, required('IDEMPOTENCY_KEY'), {
    operation: 'reverse_migration_target_era_replay', trend: reverseMigrationRecoveryDuration,
  });
  const fetched = getReservation(url, expectedID, customer.token, {
    operation: 'reverse_migration_target_era_read', trend: reverseMigrationRecoveryDuration,
  });
  const replayID = reservationID(replay);
  if (replayID && replayID !== expectedID) {
    identityMismatches.add(1);
    reverseMigrationDuplicateObservations.add(1);
  }
  const preserved = replay.status === 201 && replayID === expectedID && fetched.status === 200;
  if (preserved) reverseMigrationPreserved.add(1);

  check({ replay, fetched, replayID, expectedID }, {
    'target-era command replays after reverse migration': (value) => value.replay.status === 201,
    'target-era idempotency identity survives reverse migration': (value) =>
      value.replayID.length > 0 && value.replayID === value.expectedID,
    'target-era reservation remains readable after reverse migration': (value) => {
      if (value.fetched.status !== 200) return false;
      try {
        return value.fetched.json('id') === value.expectedID;
      } catch (_) {
        return false;
      }
    },
  });
}

// TARGET_ERA_RESERVATION_ID is captured before the externally coordinated
// reverse migration; database reconciliation proves the newer generation.

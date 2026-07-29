import { check, fail } from 'k6';
import { Counter, Trend } from 'k6/metrics';

import {
  boundedOptions,
  createHold,
  identityMismatches,
  list,
  required,
  reservationID,
} from './lib/milestone4.js';

export const physicalStaleRefreshSuccess = new Counter('physical_stale_refresh_success');
export const staleRouterSplitBrainObservations = new Counter('stale_router_split_brain_observations');
export const physicalStaleRefreshDuration = new Trend('physical_stale_refresh_duration', true);

export const options = boundedOptions({
  stale_replica_1: { executor: 'per-vu-iterations', exec: 'replica1', vus: 1, iterations: 1, maxDuration: '30s' },
  stale_replica_2: { executor: 'per-vu-iterations', exec: 'replica2', vus: 1, iterations: 1, maxDuration: '30s' },
  stale_replica_3: { executor: 'per-vu-iterations', exec: 'replica3', vus: 1, iterations: 1, maxDuration: '30s' },
}, {
  checks: ['rate==1'],
  physical_stale_refresh_success: ['count==3'],
  stale_router_split_brain_observations: ['count==0'],
  physical_stale_refresh_duration: ['p(95)<2000', 'p(99)<5000'],
});

function exerciseReplica(index) {
  const urls = list('API_URLS', 3).map((value) => value.replace(/\/$/, ''));
  const tokens = list('CUSTOMER_TOKENS', 3);
  const passengers = list('PASSENGER_IDS', 3);
  const customer = { token: tokens[index], passengerID: passengers[index] };
  const key = `m5-stale-physical-${index + 1}`;
  const first = createHold(urls[index], required('TRAIN_RUN_ID'), customer, key, {
    operation: `physical_stale_refresh_replica_${index + 1}`,
    trend: physicalStaleRefreshDuration,
  });
  const replay = createHold(urls[index], required('TRAIN_RUN_ID'), customer, key, {
    operation: `physical_stale_refresh_replay_${index + 1}`,
    trend: physicalStaleRefreshDuration,
  });
  const firstID = reservationID(first);
  const replayID = reservationID(replay);
  if (firstID && replayID && firstID !== replayID) {
    identityMismatches.add(1);
    staleRouterSplitBrainObservations.add(1);
  }
  const passed = check({ first, replay, firstID, replayID }, {
    [`replica ${index + 1} refreshes stale physical route and commits`]: (value) =>
      value.first.status === 201,
    [`replica ${index + 1} replay preserves one target identity`]: (value) =>
      value.replay.status === 201 && value.firstID.length > 0 && value.firstID === value.replayID,
  });
  if (!passed) fail(`replica ${index + 1} did not refresh the stale physical route safely`);
  physicalStaleRefreshSuccess.add(1);
}

export function replica1() { exerciseReplica(0); }
export function replica2() { exerciseReplica(1); }
export function replica3() { exerciseReplica(2); }

// Prewarming and database stale-fence/source-write deltas are controller gates;
// the public API deliberately does not disclose physical topology headers.

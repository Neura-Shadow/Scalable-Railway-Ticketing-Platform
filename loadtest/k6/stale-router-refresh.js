import { check, fail } from 'k6';
import { Counter } from 'k6/metrics';

import {
  boundedOptions,
  createHold,
  list,
  required,
} from './lib/milestone4.js';

export const staleRefreshSuccess = new Counter('stale_refresh_success');

export const options = boundedOptions({
  replica_1: { executor: 'per-vu-iterations', exec: 'replica1', vus: 1, iterations: 1, maxDuration: '30s' },
  replica_2: { executor: 'per-vu-iterations', exec: 'replica2', vus: 1, iterations: 1, maxDuration: '30s' },
  replica_3: { executor: 'per-vu-iterations', exec: 'replica3', vus: 1, iterations: 1, maxDuration: '30s' },
}, {
  checks: ['rate==1'],
  stale_refresh_success: ['count==3'],
  shard_routing_success: ['count==3'],
  booking_success_duration: ['p(95)<2000', 'p(99)<5000'],
});

function exerciseReplica(index) {
  const urls = list('API_URLS', 3).map((value) => value.replace(/\/$/, ''));
  const tokens = list('CUSTOMER_TOKENS', 3);
  const passengers = list('PASSENGER_IDS', 3);
  const response = createHold(urls[index], required('TRAIN_RUN_ID'), {
    token: tokens[index],
    passengerID: passengers[index],
  }, `m4-stale-replica-${index + 1}`, {
    operation: `stale_router_refresh_replica_${index + 1}`,
  });
  const passed = check(response, {
    [`replica ${index + 1} transparently refreshes and commits on target`]: (value) => value.status === 201,
    [`replica ${index + 1} returns a reservation identity`]: (value) => {
      try {
        const id = value.json('id');
        return typeof id === 'string' && id.length > 0;
      } catch (_) {
        return false;
      }
    },
  });
  if (!passed) fail(`replica ${index + 1} stale-route refresh did not commit exactly once`);
  staleRefreshSuccess.add(1);
}

export function replica1() { exerciseReplica(0); }
export function replica2() { exerciseReplica(1); }
export function replica3() { exerciseReplica(2); }

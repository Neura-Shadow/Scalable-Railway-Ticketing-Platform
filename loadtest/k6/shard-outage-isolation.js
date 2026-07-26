import { check, sleep } from 'k6';

import {
  baseURL,
  boundedOptions,
  expectedOutage503,
  getReservation,
  healthyShardSuccess,
  list,
  publicErrorCode,
  required,
  shardADuration,
  shardBDuration,
} from './lib/milestone4.js';

const duration = (__ENV.DURATION || '15s').trim();

export const options = boundedOptions({
  unavailable_shard: { executor: 'constant-vus', exec: 'unavailableShard', vus: 3, duration, gracefulStop: '5s' },
  healthy_shard: { executor: 'constant-vus', exec: 'healthyShard', vus: 3, duration, gracefulStop: '5s' },
}, {
  checks: ['rate==1'],
  expected_outage_503: ['count>0'],
  healthy_shard_success: ['count>0'],
});

export function unavailableShard() {
  const response = getReservation(baseURL(), list('RESERVATION_IDS', 2)[0], required('CUSTOMER_TOKEN'), {
    operation: 'isolated_unavailable_shard', allowOutage: true, trend: shardADuration,
  });
  check(response, {
    'isolated shard fails with bounded unavailable contract': (value) => value.status === 503 && publicErrorCode(value) === 'unavailable',
  });
  sleep(0.1);
}

export function healthyShard() {
  const response = getReservation(baseURL(), list('RESERVATION_IDS', 2)[1], required('CUSTOMER_TOKEN'), {
    operation: 'isolated_healthy_shard', trend: shardBDuration,
  });
  if (response.status === 200) healthyShardSuccess.add(1);
  check(response, {
    'healthy shard remains available during peer isolation': (value) => value.status === 200,
  });
  sleep(0.1);
}

// Keep the imported counter in the static module graph so k6 inspect verifies
// the exact threshold metric exported by the shared helper.
void expectedOutage503;

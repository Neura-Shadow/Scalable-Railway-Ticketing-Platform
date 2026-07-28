import { check, sleep } from 'k6';

import {
  baseURL,
  boundedOptions,
  createHold,
  customerForVU,
  identityMismatches,
  positiveInteger,
  reservationID,
  required,
} from './lib/milestone4.js';

export const options = boundedOptions({
  cache_replay: {
    executor: 'constant-vus',
    vus: positiveInteger('VUS', 6),
    duration: (__ENV.DURATION || '15s').trim(),
    gracefulStop: '5s',
  },
}, {
  shard_routing_success: ['count>0'],
  booking_success_duration: ['p(95)<2000', 'p(99)<5000'],
});

let observedID = '';

export default function () {
  const customer = customerForVU();
  const response = createHold(baseURL(), required('TRAIN_RUN_ID'), customer, `m4-cache-${__VU}`, {
    operation: 'route_cache_replay',
  });
  const currentID = reservationID(response);
  if (observedID && currentID && observedID !== currentID) identityMismatches.add(1);
  if (currentID) observedID = currentID;
  check(response, {
    'cached route replay is successful or capacity bounded': (value) => [201, 409, 429].includes(value.status),
    'idempotent cache replay keeps one reservation identity': () => !observedID || !currentID || observedID === currentID,
  });
  sleep(0.2);
}

import http from 'k6/http';
import { check, sleep } from 'k6';

import {
  baseURL,
  bearerHeaders,
  boundedOptions,
  enabled,
  list,
  partialShardResults,
  recordResponse,
  required,
} from './lib/milestone4.js';

const expectPartialRun = enabled('EXPECT_PARTIAL');

export const options = boundedOptions({
  bounded_cross_shard_probe: {
    executor: 'constant-vus',
    vus: 2,
    duration: (__ENV.DURATION || '10s').trim(),
    gracefulStop: '5s',
  },
}, expectPartialRun ? { partial_shard_results: ['count>0'] } : {});

export default function () {
  const url = baseURL();
  const reservations = list('RESERVATION_IDS', 2);
  const requestOptions = {
    headers: bearerHeaders(required('CUSTOMER_TOKEN')),
  };
  const responses = http.batch([
    ['GET', `${url}/api/v1/reservations/${encodeURIComponent(reservations[0])}`, null,
      { ...requestOptions, tags: { operation: 'cross_shard_a' } }],
    ['GET', `${url}/api/v1/reservations/${encodeURIComponent(reservations[1])}`, null,
      { ...requestOptions, tags: { operation: 'cross_shard_b' } }],
  ]);
  const expectPartial = expectPartialRun;
  const firstHealthy = responses[0].status === 200;
  const secondHealthy = responses[1].status === 200;
  recordResponse(responses[0], { allowOutage: expectPartial });
  recordResponse(responses[1], { allowOutage: expectPartial });
  if (firstHealthy !== secondHealthy) partialShardResults.add(1);
  check(responses, {
    'bounded cross-shard probe is complete or explicitly partial': () => expectPartial
      ? firstHealthy !== secondHealthy
      : firstHealthy && secondHealthy,
  });
  sleep(0.1);
}

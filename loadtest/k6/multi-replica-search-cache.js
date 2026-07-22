import { check } from 'k6';

import { baseOptions, baseURL } from './lib/config.js';
import { readSearch } from './lib/read-model.js';

export const options = baseOptions({ checks: ['rate>0.99'] });

export function setup() {
  const response = readSearch('multi_replica_setup');
  if (response.status !== 200 || !response.headers['X-Upstream-Addr']) {
    throw new Error('use the docker-compose.multi-replica.yml load-balancer URL');
  }
  return { base: baseURL() };
}

export default function () {
  const response = readSearch('multi_replica_search_cache');
  check(response, {
    'load balancer exposes upstream evidence': (result) => Boolean(result.headers['X-Upstream-Addr']),
  });
}

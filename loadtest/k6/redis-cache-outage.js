import exec from 'k6/execution';

import { baseOptions } from './lib/config.js';
import { readAvailability, readSearch } from './lib/read-model.js';

// Restart Redis from the disposable test harness during this read-only run.
export const options = baseOptions();

export default function () {
  if (exec.scenario.iterationInTest % 2 === 0) {
    readSearch('search_during_redis_outage', { chaos: 'external_redis_restart' });
  } else {
    readAvailability('availability_during_redis_outage', { chaos: 'external_redis_restart' });
  }
}

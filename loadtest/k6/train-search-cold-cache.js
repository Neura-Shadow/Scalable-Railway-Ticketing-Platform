import { readSearch } from './lib/read-model.js';

// Rotate railway:cache:search:version immediately before this bounded run.
// A single request keeps the result an honest cold-observation rather than
// mixing the first miss with subsequent warm hits.
export const options = {
  scenarios: {
    cold_observation: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 1,
      maxDuration: (__ENV.MAX_DURATION || '30s').trim(),
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: { unexpected_5xx: ['count==0'], checks: ['rate==1'] },
};

export default function () {
  readSearch('train_search_cold', { cache_state: 'cold_precondition' });
}

import http from 'k6/http';

import { baseOptions } from './lib/config.js';
import { readSearch, searchURL } from './lib/read-model.js';

export const options = baseOptions();

export function setup() {
  const response = http.get(searchURL(), { tags: { operation: 'train_search_warm_setup' } });
  if (response.status !== 200) throw new Error(`warm-cache setup failed with HTTP ${response.status}`);
}

export default function () {
  readSearch('train_search_warm', { cache_state: 'warm_precondition' });
}

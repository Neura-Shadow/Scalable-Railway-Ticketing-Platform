import exec from 'k6/execution';

import { baseOptions } from './lib/config.js';
import { readJSON, searchURL } from './lib/read-model.js';

// Run while a disposable-environment controller emits bounded synthetic
// offering updates. This script intentionally has no privileged mutation API.
export const options = baseOptions();

export default function () {
  const page = (exec.scenario.iterationInTest % 3) + 1;
  readJSON(
    searchURL(page),
    'search_during_invalidation_storm',
    (response) => Array.isArray(response.json('items')),
    { chaos: 'bounded_external_invalidation' },
  );
}

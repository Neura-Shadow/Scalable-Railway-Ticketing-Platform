import { baseOptions } from './lib/config.js';
import { readSearch } from './lib/read-model.js';

// Pause and resume the read-model worker externally. Search remains available
// through the current projection/cache or authoritative PostgreSQL fallback.
export const options = baseOptions();

export default function () {
  readSearch('search_during_read_model_pause', { chaos: 'external_worker_pause' });
}

import { baseOptions } from './lib/config.js';
import { readAvailability } from './lib/read-model.js';

export const options = baseOptions();

export default function () {
  readAvailability('availability_cache_read');
}

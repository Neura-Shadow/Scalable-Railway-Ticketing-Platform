import { baseOptions, baseURL } from './lib/config.js';
import { readJSON } from './lib/read-model.js';

export const options = baseOptions();

export default function () {
  readJSON(
    `${baseURL()}/api/v1/stations?page=1&limit=100&sort=code`,
    'station_cache_read',
    (response) => Array.isArray(response.json('items')),
  );
}

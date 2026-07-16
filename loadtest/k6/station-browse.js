import { check } from 'k6';
import http from 'k6/http';

import { baseOptions, baseURL } from './lib/config.js';
import { recordResponse } from './lib/metrics.js';

export const options = baseOptions();

export default function () {
  const response = http.get(`${baseURL()}/api/v1/stations?page=1&limit=100&sort=code`, {
    tags: { operation: 'station_browse' },
  });
  recordResponse(response);
  check(response, {
    'station browse returns 200': (result) => result.status === 200,
    'station browse returns an items array': (result) => Array.isArray(result.json('items')),
  });
}

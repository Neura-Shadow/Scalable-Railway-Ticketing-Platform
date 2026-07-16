import { check } from 'k6';
import http from 'k6/http';

import { baseOptions, baseURL, destinationCode, originCode, seatClass, serviceDate } from './lib/config.js';
import { recordResponse } from './lib/metrics.js';

export const options = baseOptions();

export default function () {
  const query = [
    `origin_station_code=${encodeURIComponent(originCode())}`,
    `destination_station_code=${encodeURIComponent(destinationCode())}`,
    `service_date=${encodeURIComponent(serviceDate())}`,
    `seat_class=${encodeURIComponent(seatClass())}`,
    'page=1',
    'limit=100',
    'sort=departure_at',
  ].join('&');
  const response = http.get(`${baseURL()}/api/v1/train-runs/search?${query}`, {
    tags: { operation: 'train_search' },
  });
  recordResponse(response);
  check(response, {
    'train search returns 200': (result) => result.status === 200,
    'train search returns an items array': (result) => Array.isArray(result.json('items')),
  });
}

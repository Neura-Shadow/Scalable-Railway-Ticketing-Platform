import { check } from 'k6';
import http from 'k6/http';

import { baseOptions, baseURL, destinationCode, originCode, seatClass, trainRunID } from './lib/config.js';
import { recordResponse } from './lib/metrics.js';

export const options = baseOptions();

export default function () {
  const query = [
    `origin_station_code=${encodeURIComponent(originCode())}`,
    `destination_station_code=${encodeURIComponent(destinationCode())}`,
    `seat_class=${encodeURIComponent(seatClass())}`,
  ].join('&');
  const response = http.get(
    `${baseURL()}/api/v1/train-runs/${encodeURIComponent(trainRunID())}/availability?${query}`,
    { tags: { operation: 'availability_read' } },
  );
  recordResponse(response);
  check(response, {
    'availability returns 200': (result) => result.status === 200,
    'availability returns a non-negative count': (result) => Number(result.json('available_seat_count')) >= 0,
  });
}

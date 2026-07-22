import { check } from 'k6';
import http from 'k6/http';
import { Counter, Trend } from 'k6/metrics';

import {
  baseURL,
  destinationCode,
  originCode,
  seatClass,
  serviceDate,
  trainRunID,
} from './config.js';
import { recordResponse } from './metrics.js';

export const readRequests = new Counter('read_model_load_requests');
export const readLatency = new Trend('read_model_load_duration', true);
export const upstreamObservations = new Counter('read_model_upstream_observations');

export function searchURL(page = 1, sort = 'departure_at') {
  const query = [
    `origin_station_code=${encodeURIComponent(originCode())}`,
    `destination_station_code=${encodeURIComponent(destinationCode())}`,
    `service_date=${encodeURIComponent(serviceDate())}`,
    `seat_class=${encodeURIComponent(seatClass())}`,
    `page=${page}`,
    'limit=100',
    `sort=${encodeURIComponent(sort)}`,
  ].join('&');
  return `${baseURL()}/api/v1/train-runs/search?${query}`;
}

export function availabilityURL() {
  const query = [
    `origin_station_code=${encodeURIComponent(originCode())}`,
    `destination_station_code=${encodeURIComponent(destinationCode())}`,
    `seat_class=${encodeURIComponent(seatClass())}`,
  ].join('&');
  return `${baseURL()}/api/v1/train-runs/${encodeURIComponent(trainRunID())}/availability?${query}`;
}

export function readJSON(url, operation, validator, extraTags = {}) {
  const response = http.get(url, { tags: { operation, ...extraTags } });
  recordResponse(response);
  readRequests.add(1, { operation });
  readLatency.add(response.timings.duration, { operation });
  const upstream = response.headers['X-Upstream-Addr'];
  if (upstream) upstreamObservations.add(1, { upstream });
  check(response, {
    [`${operation} returns 200`]: (result) => result.status === 200,
    [`${operation} returns the expected schema`]: (result) => {
      try {
        return validator(result);
      } catch (_) {
        return false;
      }
    },
  });
  return response;
}

export function readSearch(operation = 'train_search', extraTags = {}) {
  return readJSON(
    searchURL(),
    operation,
    (response) => Array.isArray(response.json('items')),
    extraTags,
  );
}

export function readAvailability(operation = 'availability_read', extraTags = {}) {
  return readJSON(
    availabilityURL(),
    operation,
    (response) => Number(response.json('available_seat_count')) >= 0,
    extraTags,
  );
}

import { check, sleep } from 'k6';
import http from 'k6/http';

import {
  availabilityCountIsValid,
  baseURL,
  bearerHeaders,
  boundedOptions,
  customerForVU,
  positiveInteger,
  readAvailability,
  recordResponse,
  shardADuration,
  shardBDuration,
  twoTrainRunIDs,
} from './lib/milestone4.js';

const vus = positiveInteger('VUS_PER_SHARD', 5);
const duration = (__ENV.DURATION || '20s').trim();

export const options = boundedOptions({
  shard_a: { executor: 'constant-vus', exec: 'shardA', vus, duration, gracefulStop: '5s' },
  shard_b: { executor: 'constant-vus', exec: 'shardB', vus, duration, gracefulStop: '5s' },
}, {
  shard_a_duration: ['p(95)<2000', 'p(99)<5000'],
  shard_b_duration: ['p(95)<2000', 'p(99)<5000'],
});

function read(index, trend, operation) {
  const url = baseURL();
  const trainRunID = twoTrainRunIDs()[index];
  const response = readAvailability(url, trainRunID, { trend, operation });
  const customer = customerForVU();
  const join = http.post(
    `${url}/api/v1/waiting-room/entries`,
    JSON.stringify({
      train_run_id: trainRunID,
      origin_station_code: (__ENV.ORIGIN_CODE || '').trim().toUpperCase(),
      destination_station_code: (__ENV.DESTINATION_CODE || '').trim().toUpperCase(),
      seat_class: (__ENV.SEAT_CLASS || '').trim().toLowerCase(),
      passenger_count: 1,
    }),
    {
      headers: { ...bearerHeaders(customer.token), 'Content-Type': 'application/json' },
      tags: { operation: `${operation}_waiting_room` },
    },
  );
  recordResponse(join, { trend });
  check({ response, join }, {
    [`${operation} returns shard-local availability`]: (values) => availabilityCountIsValid(values.response),
    [`${operation} hot waiting room accepts a bounded join`]: (values) => [201, 429].includes(values.join.status),
  });
  sleep(0.1);
}

export function shardA() { read(0, shardADuration, 'hot_shard_a_availability'); }
export function shardB() { read(1, shardBDuration, 'hot_shard_b_availability'); }

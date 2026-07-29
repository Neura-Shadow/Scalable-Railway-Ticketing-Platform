import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

import {
  baseURL,
  bookingHeaders,
  boundedOptions,
  list,
  positiveInteger,
  publicErrorCode,
  recordResponse,
  required,
  twoTrainRunIDs,
} from './lib/milestone4.js';

export const globalQuotaHoldsCreated = new Counter('global_quota_holds_created');
export const globalQuotaRejections = new Counter('global_quota_rejections');

const vus = positiveInteger('VUS', 8);
const maximumActivePerCustomer = positiveInteger('MAX_ACTIVE_HOLDS_PER_CUSTOMER', 1);
if (maximumActivePerCustomer !== 1) {
  throw new Error('cross-shard quota evidence requires MAX_ACTIVE_HOLDS_PER_CUSTOMER=1');
}

export const options = boundedOptions({
  cross_shard_quota_race: {
    executor: 'per-vu-iterations',
    vus,
    iterations: 1,
    maxDuration: (__ENV.MAX_DURATION || '2m').trim(),
  },
}, {
  checks: ['rate==1'],
  global_quota_holds_created: [`count==${vus}`],
  global_quota_rejections: [`count==${vus}`],
});

export function setup() {
  const tokens = list('CUSTOMER_TOKENS');
  const passengers = list('PASSENGER_IDS');
  if (tokens.length < vus || passengers.length < vus * 2) {
    throw new Error('cross-shard quota requires one token and two distinct synthetic passengers per VU');
  }
  return {
    url: baseURL(),
    trainRuns: twoTrainRunIDs(),
    origin: required('ORIGIN_CODE').toUpperCase(),
    destination: required('DESTINATION_CODE').toUpperCase(),
    seatClass: required('SEAT_CLASS').toLowerCase(),
    tokens,
    passengers,
  };
}

export default function (config) {
  const index = __VU - 1;
  const customerToken = config.tokens[index];
  const requests = config.trainRuns.map((trainRunID, index) => [
    'POST',
    `${config.url}/api/v1/reservations`,
    JSON.stringify({
      train_run_id: trainRunID,
      origin_station_code: config.origin,
      destination_station_code: config.destination,
      seat_class: config.seatClass,
      passenger_ids: [config.passengers[((__VU - 1) * 2) + index]],
    }),
    {
      headers: bookingHeaders(customerToken, `m5-global-quota-${__VU}-${index}`),
      tags: { operation: `cross_shard_global_quota_${index}` },
      timeout: '10s',
    },
  ]);
  const responses = http.batch(requests);

  for (const response of responses) {
    recordResponse(response);
    if (response.status === 201) globalQuotaHoldsCreated.add(1);
    if (response.status === 429 && publicErrorCode(response) === 'reservation_quota_exceeded') {
      globalQuotaRejections.add(1);
    }
  }

  check(responses, {
    'cross-shard quota race creates one final allowed hold per customer': (values) =>
      values.filter((value) => value.status === 201).length === 1,
    'cross-shard quota race rejects exactly one excess hold per customer': (values) =>
      values.filter((value) => value.status === 429).length === 1,
    'quota rejection is typed and carries bounded retry guidance': (values) =>
      values.every((value) => value.status !== 429 || (
        publicErrorCode(value) === 'reservation_quota_exceeded'
        && Number(value.headers['Retry-After']) >= 1
        && Number(value.headers['Retry-After']) <= 60
      )),
  });
}

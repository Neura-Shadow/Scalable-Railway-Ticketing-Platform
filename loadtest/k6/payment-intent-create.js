import { check } from 'k6';

import {
  apiURLs,
  boundedScenario,
  createIntent,
  fixture,
  intentView,
  iterationKey,
  optional,
} from './lib/payment.js';

export const options = boundedScenario('paymentIntentCreate', {
  payment_accepted: ['count>0'],
});

export function paymentIntentCreate() {
  const replicas = apiURLs();
  if (replicas.length < 3) throw new Error('BASE_URLS must contain three API replica URLs');
  const actor = fixture();
  const base = replicas[(__VU - 1 + __ITER) % replicas.length];
  const response = createIntent(
    base, actor.reservationID, actor.token, iterationKey('m6-intent-create'),
  );
  const intent = intentView(response);
  const expectedAmount = optional('EXPECTED_AMOUNT_MINOR');
  const expectedCurrency = optional('EXPECTED_CURRENCY').toUpperCase();
  check(response, {
    'intent create is accepted with bounded customer view': () => Boolean(
      response.status === 202 && intent && intent.reservation_id === actor.reservationID
      && Number.isInteger(intent.amount_minor) && intent.amount_minor > 0
      && typeof intent.currency === 'string' && /^[A-Z]{3}$/.test(intent.currency)
      && (!expectedAmount || intent.amount_minor === Number.parseInt(expectedAmount, 10))
      && (!expectedCurrency || intent.currency === expectedCurrency)
    ),
  });
}

// External post-run checks must prove one active intent/saga per reservation
// and equality with the immutable shard reservation amount/currency.

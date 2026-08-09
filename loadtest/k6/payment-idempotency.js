import { check } from 'k6';

import {
  apiURL,
  assertStableIdentity,
  boundedScenario,
  createIntent,
  iterationKey,
  pairedFixture,
  publicErrorCode,
} from './lib/payment.js';

export const options = boundedScenario('paymentIdempotency');

export function paymentIdempotency() {
  const actor = pairedFixture();
  const key = iterationKey('m6-payment-idempotency');
  const first = createIntent(apiURL(), actor.firstReservationID, actor.token, key);
  const replay = createIntent(apiURL(), actor.firstReservationID, actor.token, key);
  const changedFingerprint = createIntent(
    apiURL(), actor.secondReservationID, actor.token, key, [409],
  );
  check(null, {
    'same key and reservation replay one intent': () => assertStableIdentity(first, replay),
    'same key on another reservation conflicts': () => (
      changedFingerprint.status === 409
      && publicErrorCode(changedFingerprint) === 'payment_intent_conflict'
    ),
  });
}

// A database snapshot must separately prove that only the key hash persists;
// the client cannot inspect storage or provider-operation uniqueness.

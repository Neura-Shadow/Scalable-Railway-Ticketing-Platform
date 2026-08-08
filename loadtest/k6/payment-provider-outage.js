import { check } from 'k6';

import {
  apiURL,
  boundedScenario,
  createIntent,
  fixture,
  intentView,
  iterationKey,
  optionalList,
  pollIntent,
  positiveInteger,
  queueFault,
} from './lib/payment.js';

export const options = boundedScenario('paymentProviderOutage', {
  payment_fault_hooks_armed: ['count>0'],
});

export function paymentProviderOutage() {
  const actor = fixture();
  const faults = optionalList('PROVIDER_FAULT_KINDS', 'outage,invalid_response,oversized_response');
  const kind = faults[__ITER % faults.length];
  const armed = queueFault('create_checkout', kind);
  const created = createIntent(apiURL(), actor.reservationID, actor.token, iterationKey('m6-provider-outage'));
  const initial = intentView(created);
  const observed = initial && pollIntent(
    apiURL(), initial.id, actor.token,
    ['awaiting_customer', 'manual_review', 'failed', 'checkout_pending'],
    positiveInteger('OUTAGE_POLL_ATTEMPTS', 40, 120),
  );
  check(null, {
    'provider fault is armed': () => armed.status === 204,
    'intent remains observable during provider fault': () => Boolean(
      initial && observed
      && ['checkout_pending', 'awaiting_customer', 'manual_review', 'failed'].includes(observed.state)
    ),
  });
}

// Backoff, exhausted attempts, leases, goroutines and absence of blind retries
// are external worker/metrics/database assertions, not HTTP-client claims.

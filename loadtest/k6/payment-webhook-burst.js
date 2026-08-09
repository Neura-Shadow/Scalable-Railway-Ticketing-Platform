import { check } from 'k6';

import {
  apiURL,
  authorizeHosted,
  boundedScenario,
  createIntent,
  deliverDrainedWebhooks,
  fixture,
  intentView,
  iterationKey,
  queueFault,
  waitForHosted,
} from './lib/payment.js';

export const options = boundedScenario('paymentWebhookBurst');

export function paymentWebhookBurst() {
  const actor = fixture();
  const fault = __ITER % 2 === 0 ? 'duplicate_webhook' : 'out_of_order_webhook';
  const created = createIntent(apiURL(), actor.reservationID, actor.token, iterationKey('m6-webhook'));
  const initial = intentView(created);
  const hosted = initial && waitForHosted(apiURL(), initial.id, actor.token);
  const queued = hosted ? queueFault('authorize', fault) : null;
  const authorized = hosted && authorizeHosted(hosted.hosted_session_ref);
  const deliveries = deliverDrainedWebhooks(apiURL());
  check(null, {
    'deterministic webhook fault is armed': () => Boolean(queued && queued.status === 204),
    'hosted authorization is accepted': () => Boolean(authorized && authorized.status === 202),
    'bounded drained webhook burst is acknowledged': () => (
      deliveries.length > 0 && deliveries.every((response) => [200, 202].includes(response.status))
    ),
  });
}

// Inbox identity/hash consistency, changed-hash conflicts, event ordering and
// absence of inline financial effects require the external invariant bundle.

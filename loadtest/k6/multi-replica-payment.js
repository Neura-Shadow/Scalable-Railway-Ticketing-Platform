import { check } from 'k6';

import {
  apiURLs,
  assertStableIdentity,
  authorizeHosted,
  boundedScenario,
  createIntent,
  deliverDrainedWebhooks,
  fixture,
  intentView,
  iterationKey,
  listTicketOrders,
  pollIntent,
  positiveInteger,
  ticketOrderForReservation,
  waitForHosted,
} from './lib/payment.js';

export const options = boundedScenario('multiReplicaPayment');

export function multiReplicaPayment() {
  const replicas = apiURLs();
  if (replicas.length < 3) throw new Error('BASE_URLS must contain three API replica URLs');
  const actor = fixture();
  const key = iterationKey('m6-multi-replica');
  const first = createIntent(replicas[0], actor.reservationID, actor.token, key);
  const replayTwo = createIntent(replicas[1], actor.reservationID, actor.token, key);
  const replayThree = createIntent(replicas[2], actor.reservationID, actor.token, key);
  const initial = intentView(first);
  const hosted = initial && waitForHosted(replicas[1], initial.id, actor.token);
  const authorized = hosted && authorizeHosted(hosted.hosted_session_ref);
  if (authorized) deliverDrainedWebhooks(replicas[2]);
  const final = initial && pollIntent(
    replicas[2], initial.id, actor.token, ['completed', 'manual_review', 'failed'],
    positiveInteger('MULTI_REPLICA_POLL_ATTEMPTS', 100, 120),
  );
  const order = ticketOrderForReservation(listTicketOrders(replicas[0], actor.token), actor.reservationID);
  check(null, {
    'three replicas replay one payment identity': () => (
      assertStableIdentity(first, replayTwo) && assertStableIdentity(first, replayThree)
    ),
    'multi-replica journey converges to one visible order': () => Boolean(
      authorized && authorized.status === 202 && final && final.state === 'completed'
      && order && order.status === 'issued'
    ),
  });
}

// Provider operation, worker lease, issuance receipt and refund uniqueness are
// established only by the external invariant and reconciliation snapshots.

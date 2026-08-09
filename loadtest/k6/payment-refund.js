import { check } from 'k6';

import {
  apiURL,
  authorizeHosted,
  boundedScenario,
  cancelIntent,
  createIntent,
  deliverDrainedWebhooks,
  fixture,
  intentView,
  iterationKey,
  listTicketOrders,
  optional,
  pollIntent,
  positiveInteger,
  queueFault,
  ticketOrderForReservation,
  waitForHosted,
} from './lib/payment.js';

export const options = boundedScenario('paymentRefund', {
  payment_fault_hooks_armed: ['count>0'],
});

export function paymentRefund() {
  const actor = fixture();
  const created = createIntent(apiURL(), actor.reservationID, actor.token, iterationKey('m6-refund-create'));
  const initial = intentView(created);
  const hosted = initial && waitForHosted(apiURL(), initial.id, actor.token);
  const authorized = hosted && authorizeHosted(hosted.hosted_session_ref);
  if (authorized) deliverDrainedWebhooks(apiURL());
  const completed = initial && pollIntent(
    apiURL(), initial.id, actor.token, ['completed', 'manual_review', 'failed'],
    positiveInteger('PAYMENT_POLL_ATTEMPTS', 80, 120),
  );
  const refundFault = optional('REFUND_FAULT_KIND', 'refund_transient');
  const armed = completed && completed.state === 'completed'
    ? queueFault('refund', refundFault)
    : null;
  const cancellation = completed && completed.state === 'completed'
    ? cancelIntent(apiURL(), initial.id, actor.token, iterationKey('m6-refund-cancel'))
    : null;
  const refunded = cancellation && pollIntent(
    apiURL(), initial.id, actor.token, ['cancelled', 'manual_review', 'failed'],
    positiveInteger('REFUND_POLL_ATTEMPTS', 80, 120),
  );
  const order = ticketOrderForReservation(listTicketOrders(apiURL(), actor.token), actor.reservationID);
  check(null, {
    'captured journey reaches customer-visible completion': () => Boolean(
      authorized && authorized.status === 202 && completed && completed.state === 'completed'
    ),
    'refund request is accepted': () => Boolean(cancellation && cancellation.status === 202),
    'refund retry fault is armed': () => Boolean(armed && armed.status === 204),
    'refund converges without active tickets': () => Boolean(
      refunded && refunded.state === 'cancelled' && order
      && ['refunded', 'cancelled'].includes(order.status)
      && (!Array.isArray(order.tickets) || order.tickets.every((ticket) => ticket.status !== 'active'))
    ),
  });
}

// Exact full-refund totals, provider refund uniqueness and seat release timing
// must be proven from provider/control/shard post-run snapshots.

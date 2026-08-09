import { check } from 'k6';

import {
  apiURLs,
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

export const options = boundedScenario('ticketIssuance');

export function ticketIssuance() {
  const replicas = apiURLs();
  if (replicas.length < 3) throw new Error('BASE_URLS must contain three API replica URLs');
  const actor = fixture();
  const before = ticketOrderForReservation(listTicketOrders(replicas[0], actor.token), actor.reservationID);
  const created = createIntent(replicas[0], actor.reservationID, actor.token, iterationKey('m6-ticket-issuance'));
  const initial = intentView(created);
  const hosted = initial && waitForHosted(replicas[1], initial.id, actor.token);
  const authorized = hosted && authorizeHosted(hosted.hosted_session_ref);
  if (authorized) deliverDrainedWebhooks(replicas[2]);
  const final = initial && pollIntent(
    replicas[2], initial.id, actor.token, ['completed', 'manual_review', 'failed'],
    positiveInteger('ISSUANCE_POLL_ATTEMPTS', 80, 120),
  );
  const order = ticketOrderForReservation(listTicketOrders(replicas[0], actor.token), actor.reservationID);
  check(null, {
    'no ticket order is visible before customer authorization': () => before === null,
    'hosted authorization is accepted': () => Boolean(authorized && authorized.status === 202),
    'completed payment exposes one issued order with active tickets': () => Boolean(
      final && final.state === 'completed' && order && order.status === 'issued'
      && Array.isArray(order.tickets) && order.tickets.length > 0
      && order.tickets.every((ticket) => ticket.status === 'active')
    ),
  });
}

// The control/shard crash hooks are external orchestration steps. Issuance
// receipt uniqueness, one ticket per reservation seat and absence of duplicate
// ticket codes remain mandatory external database assertions.

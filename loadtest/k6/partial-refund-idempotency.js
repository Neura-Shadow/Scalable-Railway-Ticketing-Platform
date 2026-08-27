import { check } from 'k6';
import http from 'k6/http';

import { apiURLs, iterationKey, paymentHeaders, required } from './lib/payment.js';
import { jsonID, m7Scenario, partialRefundDuplicates, ticketFixture } from './lib/milestone7.js';

export const options = {
  ...m7Scenario('partialRefundIdempotency'),
  batch: 100,
  batchPerHost: 100,
};

export function partialRefundIdempotency() {
  const fixture = ticketFixture();
  const bases = apiURLs();
  if (bases.length < 3) throw new Error('BASE_URLS must address all three API replicas');
  const conflictTicketID = required('CONFLICT_TICKET_ID');
  if (fixture.ticketIDs.includes(conflictTicketID)) throw new Error('CONFLICT_TICKET_ID must differ from the selected ticket');

  // A single batch is the deterministic release barrier: all 100 requests are
  // constructed before k6 dispatches them concurrently across all replicas.
  const key = iterationKey('m7-refund-replay');
  const body = JSON.stringify({ ticket_ids: fixture.ticketIDs });
  const requests = Array.from({ length: 100 }, (_, index) => ({
    method: 'POST',
    url: `${bases[index % bases.length]}/api/v1/ticket-orders/${encodeURIComponent(fixture.orderID)}/refunds`,
    body,
    params: { headers: paymentHeaders(fixture.token, key), tags: { operation: 'ticket_partial_refund_100_way' } },
  }));
  const responses = http.batch(requests);
  const ids = responses.map(jsonID);
  const identity = ids[0] || '';
  const stable = identity !== '' && responses.every((response, index) => response.status === 202 && ids[index] === identity);
  if (!stable) partialRefundDuplicates.add(1);
  check(null, { '100 concurrent retries return one exact request identity': () => stable });

  const conflict = http.post(
    `${bases[1]}/api/v1/ticket-orders/${encodeURIComponent(fixture.orderID)}/refunds`,
    JSON.stringify({ ticket_ids: [conflictTicketID] }),
    { headers: paymentHeaders(fixture.token, key), tags: { operation: 'ticket_partial_refund_idempotency_conflict' } },
  );
  check(conflict, {
    'same key with different ticket selection is rejected': (response) => response.status === 409 && response.json('error.code') === 'conflict',
  });
}

// External assertions prove one request, one provider refund, one compensation
// receipt, and one seat release after the 100-way barrier and conflict probe.

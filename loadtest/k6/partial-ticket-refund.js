import { check } from 'k6';
import { apiURL, iterationKey } from './lib/payment.js';
import { getRefund, jsonID, m7Scenario, requestRefund, ticketFixture } from './lib/milestone7.js';

export const options = m7Scenario('partialTicketRefund');

export function partialTicketRefund() {
  const fixture = ticketFixture();
  const response = requestRefund(apiURL(), fixture, iterationKey('m7-ticket-refund'));
  const requestID = jsonID(response);
  const view = requestID ? getRefund(apiURL(), fixture.token, requestID) : null;
  check(null, {
    'server accepts only selected whole tickets': () => response.status === 202,
    'accepted refund remains owner-addressable': () => Boolean(view && view.status === 200),
  });
}

// Shard receipts, exact seat-mask release, provider refund count, cumulative
// money, unselected tickets and ledger balance are mandatory external checks.

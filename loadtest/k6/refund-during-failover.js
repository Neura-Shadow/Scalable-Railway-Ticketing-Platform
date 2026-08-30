import { check } from 'k6';
import { apiURL, iterationKey } from './lib/payment.js';
import { jsonID, m7Scenario, requestRefund, ticketFixture } from './lib/milestone7.js';

export const options = m7Scenario('refundDuringFailover');

export function refundDuringFailover() {
  const fixture = ticketFixture();
  const response = requestRefund(apiURL(), fixture, iterationKey('m7-failover-refund'));
  check(response, {
    'refund has bounded failover behavior': (value) => [202, 503].includes(value.status),
    'accepted refund has stable identity': (value) => value.status === 503 || jsonID(value) !== '',
  });
}

// External assertions require query-before-retry, one provider refund, exact
// selected-seat release, ledger evidence and no unselected-ticket mutation.

import { check } from 'k6';
import { apiURL, createIntent, fixture, intentView, iterationKey } from './lib/payment.js';
import { m7Scenario } from './lib/milestone7.js';

export const options = m7Scenario('paymentDuringFailover');

export function paymentDuringFailover() {
  const actor = fixture();
  const response = createIntent(apiURL(), actor.reservationID, actor.token, iterationKey('m7-failover-payment'), [202, 503]);
  const view = intentView(response);
  check(null, {
    'failover payment response is bounded': () => [202, 503].includes(response.status),
    'accepted payment has durable identity': () => response.status === 503 || Boolean(view && view.id),
  });
}

// Final assertions require one capture, resumed saga, one issued order, ticket
// retrieval, and clean reconciliation after the route switch.

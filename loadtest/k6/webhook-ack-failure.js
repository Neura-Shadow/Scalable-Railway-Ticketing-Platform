import http from 'k6/http';
import { check } from 'k6';
import { m7Scenario, optional, required, webhookAckDuration, webhookRetries } from './lib/milestone7.js';

export const options = m7Scenario('webhookAckFailure');

export function webhookAckFailure() {
  const started = Date.now();
  const headers = {'Content-Type': 'application/json'};
  headers[optional('WEBHOOK_SIGNATURE_HEADER', 'Stripe-Signature')] = required('WEBHOOK_SIGNATURE');
  if (optional('WEBHOOK_KEY_ID')) headers['X-Payment-Key-ID'] = optional('WEBHOOK_KEY_ID');
  if (optional('WEBHOOK_TIMESTAMP')) headers['X-Payment-Timestamp'] = optional('WEBHOOK_TIMESTAMP');
  const response = http.post(required('WEBHOOK_URL'), required('WEBHOOK_BODY'), {
    headers,
    tags: { operation: 'webhook_durable_ack' },
  });
  webhookAckDuration.add(Date.now() - started);
  const expected = Number.parseInt(optional('EXPECTED_WEBHOOK_STATUS', '500'), 10);
  if (response.status >= 500) webhookRetries.add(1);
  check(response, { 'ack matches configured persistence outcome': (value) => value.status === expected });
}

// The driver controls the control-DB fault and proves 2xx only after durable
// inbox commit; k6 never decides whether persistence actually committed.

import http from 'k6/http';
import { check } from 'k6';
import { m7Scenario, optional, required } from './lib/milestone7.js';

export const options = m7Scenario('webhookKeyRotation');

function deliver(suffix) {
  const headers = {'Content-Type': 'application/json'};
  headers[optional('WEBHOOK_SIGNATURE_HEADER', 'Stripe-Signature')] = required(`WEBHOOK_${suffix}_SIGNATURE`);
  if (optional(`WEBHOOK_${suffix}_KEY_ID`)) headers['X-Payment-Key-ID'] = optional(`WEBHOOK_${suffix}_KEY_ID`);
  if (optional(`WEBHOOK_${suffix}_TIMESTAMP`)) headers['X-Payment-Timestamp'] = optional(`WEBHOOK_${suffix}_TIMESTAMP`);
  return http.post(required('WEBHOOK_URL'), required(`WEBHOOK_${suffix}_BODY`), {
    headers,
    tags: { operation: 'webhook_key_rotation', key_generation: suffix.toLowerCase() },
  });
}

export function webhookKeyRotation() {
  const previous = deliver('PREVIOUS');
  const current = deliver('CURRENT');
  check(null, {
    'previous key remains valid during overlap': () => [200, 202].includes(previous.status),
    'current key is accepted': () => [200, 202].includes(current.status),
  });
}

// The evidence driver removes the previous key after the bounded overlap and
// requires that a new previous-key signature is rejected without inbox write.

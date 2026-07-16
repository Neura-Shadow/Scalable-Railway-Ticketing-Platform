import { check } from 'k6';

import { baseOptions, idempotencyKey } from './lib/config.js';
import { idempotencyMismatches } from './lib/metrics.js';
import { createHold, reservationID } from './lib/requests.js';

export const options = baseOptions({ idempotency_mismatches: ['count==0'] });
const sharedKey = idempotencyKey();

export default function () {
  const first = createHold(sharedKey, { attempt: 'first' });
  const second = createHold(sharedKey, { attempt: 'replay' });
  const firstID = reservationID(first);
  const secondID = reservationID(second);

  const stable = first.status === 201 && second.status === 201 && firstID && firstID === secondID;
  if (!stable) {
    idempotencyMismatches.add(1);
  }
  check(null, { 'same idempotency request returns one reservation': () => Boolean(stable) });
}

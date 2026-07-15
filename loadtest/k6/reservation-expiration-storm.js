import { check, sleep } from 'k6';

import { expirationWaitSeconds, oneAttemptPerVUOptions } from './lib/config.js';
import { expiredHolds } from './lib/metrics.js';
import { createHold, getReservation, iterationKey, reservationID } from './lib/requests.js';

export const options = oneAttemptPerVUOptions({}, expirationWaitSeconds() + 30);

export default function () {
  const hold = createHold(iterationKey('expiration-storm'));
  check(hold, {
    'expiration setup returns success or bounded contention': (result) => [201, 409, 429].includes(result.status),
  });
  const id = reservationID(hold);
  if (!id) {
    return;
  }

  sleep(expirationWaitSeconds());
  const loaded = getReservation(id);
  let status = '';
  try {
    status = loaded.json('status') || '';
  } catch (_) {
    status = '';
  }
  if (status === 'expired') {
    expiredHolds.add(1);
  }
  check(loaded, { 'hold expires after worker window': () => loaded.status === 200 && status === 'expired' });
}

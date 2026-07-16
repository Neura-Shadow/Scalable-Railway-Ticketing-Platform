import { check } from 'k6';

import { baseOptions } from './lib/config.js';
import { cancelReservation, confirmReservation, createHold, iterationKey, reservationID } from './lib/requests.js';

export const options = baseOptions();

export default function () {
  const hold = createHold(iterationKey('confirm-hold'));
  check(hold, {
    'confirmation setup returns success or bounded contention': (result) => [201, 409, 429].includes(result.status),
  });
  const id = reservationID(hold);
  if (!id) {
    return;
  }

  const confirmed = confirmReservation(id, iterationKey('confirm'));
  check(confirmed, { 'held reservation confirms': (result) => result.status === 200 });
  if (confirmed.status === 200) {
    const cancelled = cancelReservation(id, iterationKey('confirm-cleanup'));
    check(cancelled, { 'confirmed reservation cleanup succeeds': (result) => result.status === 200 });
  }
}

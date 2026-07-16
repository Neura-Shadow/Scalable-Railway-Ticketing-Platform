import { check } from 'k6';

import { baseOptions } from './lib/config.js';
import { cancelReservation, createHold, getReservation, iterationKey, reservationID } from './lib/requests.js';

export const options = baseOptions();

export default function () {
  const hold = createHold(iterationKey('normal-hold'));
  check(hold, {
    'normal hold returns success or bounded contention': (result) => [201, 409, 429].includes(result.status),
  });
  const id = reservationID(hold);
  if (!id) {
    return;
  }

  const loaded = getReservation(id);
  check(loaded, { 'created hold can be read': (result) => result.status === 200 });
  const cancelled = cancelReservation(id, iterationKey('normal-cancel'));
  check(cancelled, { 'normal hold cleanup succeeds': (result) => result.status === 200 });
}

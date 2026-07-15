import { check } from 'k6';

import { baseOptions } from './lib/config.js';
import { cancelReservation, createHold, iterationKey, reservationID } from './lib/requests.js';

export const options = baseOptions();

export default function () {
  const hold = createHold(iterationKey('rate-limit'));
  check(hold, {
    'rate-limit probe has a bounded response': (result) => [201, 409, 429].includes(result.status),
  });
  const id = reservationID(hold);
  if (id) {
    cancelReservation(id, iterationKey('rate-limit-cleanup'));
  }
}

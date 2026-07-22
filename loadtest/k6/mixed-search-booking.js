import { check } from 'k6';
import exec from 'k6/execution';

import { baseOptions } from './lib/config.js';
import { cancelReservation, createHold, iterationKey, reservationID } from './lib/requests.js';
import { readSearch } from './lib/read-model.js';

export const options = baseOptions();

export default function () {
  readSearch('mixed_train_search');
  const every = Number.parseInt(__ENV.BOOKING_EVERY || '10', 10);
  if (!Number.isInteger(every) || every <= 0) throw new Error('BOOKING_EVERY must be positive');
  if (exec.scenario.iterationInTest % every !== 0) return;

  const hold = createHold(iterationKey('m3-mixed-hold'), { workload: 'mixed_search_booking' });
  check(hold, {
    'booking returns created or an expected contention response': (response) =>
      response.status === 201 || response.status === 409 || response.status === 429,
  });
  const id = reservationID(hold);
  if (id) cancelReservation(id, iterationKey('m3-mixed-cancel'));
}

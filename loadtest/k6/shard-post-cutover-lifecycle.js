import { check, fail } from 'k6';
import http from 'k6/http';
import { Counter } from 'k6/metrics';

import {
  baseURL,
  bearerHeaders,
  bookingHeaders,
  boundedOptions,
  createHold,
  getReservation,
  identityMismatches,
  iterationKey,
  list,
  listTicketOrders,
  recordResponse,
  required,
  reservationID,
} from './lib/milestone4.js';

export const lifecycleSuccess = new Counter('post_cutover_lifecycle_success');
export const ticketOrderReadSuccess = new Counter('post_cutover_ticket_order_read_success');

export const options = boundedOptions({
  post_cutover_lifecycle: {
    executor: 'shared-iterations',
    vus: 1,
    iterations: 1,
    maxDuration: '60s',
    gracefulStop: '0s',
  },
}, {
  checks: ['rate==1'],
  post_cutover_lifecycle_success: ['count==1'],
  post_cutover_ticket_order_read_success: ['count==1'],
  booking_success_duration: ['p(95)<2000', 'p(99)<5000'],
});

function lifecycleCustomers() {
  const tokens = list('CUSTOMER_TOKENS', 2);
  const passengers = list('PASSENGER_IDS', 2);
  return [
    { token: tokens[0], passengerID: passengers[0] },
    { token: tokens[1], passengerID: passengers[1] },
  ];
}

function requireCheck(value, name, predicate) {
  if (!check(value, { [name]: predicate })) fail(name);
}

function requireReservation(response, expectedStatus, expectedID, label) {
  requireCheck(response, `${label} returns the exact successful status`, (value) => value.status === 200);
  requireCheck(response, `${label} returns the expected reservation identity`, (value) => {
    try {
      return value.json('id') === expectedID;
    } catch (_) {
      return false;
    }
  });
  requireCheck(response, `${label} returns reservation status ${expectedStatus}`, (value) => {
    try {
      return value.json('status') === expectedStatus;
    } catch (_) {
      return false;
    }
  });
}

function createAndReplay(url, trainRunID, customer, key, label) {
  const created = createHold(url, trainRunID, customer, key, {
    operation: `${label}_create`,
  });
  requireCheck(created, `${label} create returns 201`, (value) => value.status === 201);
  const createdID = reservationID(created);
  requireCheck(createdID, `${label} create returns a reservation identity`, (value) => value.length > 0);

  const replayed = createHold(url, trainRunID, customer, key, {
    operation: `${label}_create_replay`,
  });
  requireCheck(replayed, `${label} idempotent create replay returns 201`, (value) => value.status === 201);
  const replayedID = reservationID(replayed);
  if (createdID !== replayedID) identityMismatches.add(1);
  requireCheck(
    { createdID, replayedID },
    `${label} idempotent create replay preserves reservation identity`,
    (value) => value.createdID.length > 0 && value.createdID === value.replayedID,
  );
  return createdID;
}

function mutateReservation(url, reservationIDValue, customer, key, action, operation) {
  const response = http.post(
    `${url}/api/v1/reservations/${encodeURIComponent(reservationIDValue)}/${action}`,
    null,
    {
      headers: bookingHeaders(customer.token, key),
      tags: { operation },
      timeout: '10s',
    },
  );
  recordResponse(response);
  return response;
}

function ticketOrderForReservation(response, reservationIDValue) {
  try {
    const items = response.json('items');
    if (!Array.isArray(items)) return [];
    return items.filter((item) => item && item.reservation_id === reservationIDValue);
  } catch (_) {
    return [];
  }
}

function verifyTicketOrder(url, customer, reservationIDValue) {
  const listed = listTicketOrders(url, customer.token, {
    operation: 'post_cutover_ticket_order_list',
  });
  requireCheck(listed, 'post-cutover ticket-order list returns 200', (value) => value.status === 200);
  const matches = ticketOrderForReservation(listed, reservationIDValue);
  requireCheck(
    matches,
    'confirmed reservation has exactly one bounded ticket order',
    (value) => value.length === 1 && value[0].status === 'confirmed' && typeof value[0].id === 'string' && value[0].id.length > 0,
  );

  const orderID = matches[0].id;
  const fetched = http.get(
    `${url}/api/v1/ticket-orders/${encodeURIComponent(orderID)}`,
    {
      headers: bearerHeaders(customer.token),
      tags: { operation: 'post_cutover_ticket_order_get' },
      timeout: '10s',
    },
  );
  recordResponse(fetched);
  requireCheck(fetched, 'post-cutover ticket-order read returns 200', (value) => value.status === 200);
  requireCheck(fetched, 'post-cutover ticket-order read remains bound to the reservation', (value) => {
    try {
      return value.json('id') === orderID
        && value.json('reservation_id') === reservationIDValue
        && value.json('status') === 'confirmed';
    } catch (_) {
      return false;
    }
  });
  requireCheck(fetched, 'post-cutover ticket-order read returns one active ticket', (value) => {
    try {
      const tickets = value.json('tickets');
      return Array.isArray(tickets)
        && tickets.length === 1
        && tickets[0].status === 'active'
        && typeof tickets[0].id === 'string'
        && tickets[0].id.length > 0;
    } catch (_) {
      return false;
    }
  });
  ticketOrderReadSuccess.add(1);
}

export default function () {
  const url = baseURL();
  const trainRunID = required('TRAIN_RUN_ID');
  const [confirmCustomer, cancelCustomer] = lifecycleCustomers();

  const confirmReservationID = createAndReplay(
    url,
    trainRunID,
    confirmCustomer,
    iterationKey('m4-post-cutover-confirm-create'),
    'post-cutover confirm path',
  );
  const held = getReservation(url, confirmReservationID, confirmCustomer.token, {
    operation: 'post_cutover_confirm_path_get_held',
  });
  requireReservation(held, 'held', confirmReservationID, 'post-cutover held read');

  const confirmKey = iterationKey('m4-post-cutover-confirm');
  const confirmed = mutateReservation(
    url,
    confirmReservationID,
    confirmCustomer,
    confirmKey,
    'confirm',
    'post_cutover_confirm',
  );
  requireReservation(confirmed, 'confirmed', confirmReservationID, 'post-cutover confirm');
  const confirmReplay = mutateReservation(
    url,
    confirmReservationID,
    confirmCustomer,
    confirmKey,
    'confirm',
    'post_cutover_confirm_replay',
  );
  requireReservation(confirmReplay, 'confirmed', confirmReservationID, 'post-cutover confirm replay');
  verifyTicketOrder(url, confirmCustomer, confirmReservationID);

  const cancelReservationID = createAndReplay(
    url,
    trainRunID,
    cancelCustomer,
    iterationKey('m4-post-cutover-cancel-create'),
    'post-cutover cancel path',
  );
  const cancelHeld = getReservation(url, cancelReservationID, cancelCustomer.token, {
    operation: 'post_cutover_cancel_path_get_held',
  });
  requireReservation(cancelHeld, 'held', cancelReservationID, 'post-cutover cancel-path held read');

  const cancelKey = iterationKey('m4-post-cutover-cancel');
  const cancelled = mutateReservation(
    url,
    cancelReservationID,
    cancelCustomer,
    cancelKey,
    'cancel',
    'post_cutover_cancel',
  );
  requireReservation(cancelled, 'cancelled', cancelReservationID, 'post-cutover cancel');
  const cancelReplay = mutateReservation(
    url,
    cancelReservationID,
    cancelCustomer,
    cancelKey,
    'cancel',
    'post_cutover_cancel_replay',
  );
  requireReservation(cancelReplay, 'cancelled', cancelReservationID, 'post-cutover cancel replay');
  const cancelledRead = getReservation(url, cancelReservationID, cancelCustomer.token, {
    operation: 'post_cutover_cancel_path_get_cancelled',
  });
  requireReservation(cancelledRead, 'cancelled', cancelReservationID, 'post-cutover cancelled read');

  lifecycleSuccess.add(1);
}

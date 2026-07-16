import http from 'k6/http';
import exec from 'k6/execution';

import {
  baseURL,
  customerToken,
  destinationCode,
  originCode,
  passengerIDs,
  seatClass,
  trainRunID,
} from './config.js';
import {
  cancelledHolds,
  confirmedReservations,
  recordResponse,
  reservationLatency,
  successfulHolds,
} from './metrics.js';

export function bearerHeaders(token) {
  return {
    Authorization: `Bearer ${token}`,
    Accept: 'application/json',
  };
}

export function reservationHeaders(idempotencyKey) {
  return {
    ...bearerHeaders(customerToken()),
    'Content-Type': 'application/json',
    'Idempotency-Key': idempotencyKey,
  };
}

export function reservationPayload() {
  return {
    train_run_id: trainRunID(),
    origin_station_code: originCode(),
    destination_station_code: destinationCode(),
    seat_class: seatClass(),
    passenger_ids: passengerIDs(),
  };
}

export function iterationKey(prefix) {
  return `${prefix}-${exec.scenario.name}-${exec.vu.idInTest}-${exec.scenario.iterationInTest}`;
}

export function createHold(idempotencyKey, tags = {}) {
  const response = http.post(
    `${baseURL()}/api/v1/reservations`,
    JSON.stringify(reservationPayload()),
    { headers: reservationHeaders(idempotencyKey), tags: { operation: 'reservation_create', ...tags } },
  );
  reservationLatency.add(response.timings.duration);
  recordResponse(response);
  if (response.status === 201) {
    successfulHolds.add(1);
  }
  return response;
}

export function reservationID(response) {
  if (response.status !== 200 && response.status !== 201) {
    return '';
  }
  try {
    return response.json('id') || '';
  } catch (_) {
    return '';
  }
}

export function getReservation(id) {
  const response = http.get(`${baseURL()}/api/v1/reservations/${encodeURIComponent(id)}`, {
    headers: bearerHeaders(customerToken()),
    tags: { operation: 'reservation_get' },
  });
  recordResponse(response);
  return response;
}

export function confirmReservation(id, idempotencyKey) {
  const response = http.post(`${baseURL()}/api/v1/reservations/${encodeURIComponent(id)}/confirm`, null, {
    headers: reservationHeaders(idempotencyKey),
    tags: { operation: 'reservation_confirm' },
  });
  recordResponse(response);
  if (response.status === 200) {
    confirmedReservations.add(1);
  }
  return response;
}

export function cancelReservation(id, idempotencyKey) {
  const response = http.post(`${baseURL()}/api/v1/reservations/${encodeURIComponent(id)}/cancel`, null, {
    headers: reservationHeaders(idempotencyKey),
    tags: { operation: 'reservation_cancel' },
  });
  recordResponse(response);
  if (response.status === 200) {
    cancelledHolds.add(1);
  }
  return response;
}

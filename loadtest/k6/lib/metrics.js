import { Counter, Trend } from 'k6/metrics';

export const http4xx = new Counter('http_4xx');
export const http5xx = new Counter('http_5xx');
export const unexpected5xx = new Counter('unexpected_5xx');
export const allocationConflicts = new Counter('allocation_conflicts');
export const successfulHolds = new Counter('successful_holds');
export const confirmedReservations = new Counter('confirmed_reservations');
export const expiredHolds = new Counter('expired_holds');
export const cancelledHolds = new Counter('cancelled_holds');
export const rateLimited = new Counter('rate_limited');
export const idempotencyMismatches = new Counter('idempotency_mismatches');
export const reservationLatency = new Trend('reservation_duration', true);

export function recordResponse(response) {
  if (response.status >= 400 && response.status < 500) {
    http4xx.add(1);
  }
  if (response.status >= 500) {
    http5xx.add(1);
    unexpected5xx.add(1);
  }
  if (response.status === 409) {
    allocationConflicts.add(1);
  }
  if (response.status === 429) {
    rateLimited.add(1);
  }
}

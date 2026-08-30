import http from 'k6/http';
import { Counter, Trend } from 'k6/metrics';

import { bearerHeaders, boundedScenario, optional, paymentHeaders, positiveInteger, record, required } from './payment.js';

export { boundedScenario, optional, positiveInteger, required };

export const partialRefundDuplicates = new Counter('partial_refund_duplicate_count');
export const regionalWriteRejections = new Counter('regional_write_rejections');
export const webhookRetries = new Counter('webhook_retry_count');
export const settlementMismatch = new Counter('settlement_reconciliation_mismatch_count');
export const settlementImportedRecords = new Counter('settlement_import_records_observed');
export const partialRefundDuration = new Trend('partial_refund_duration', true);
export const webhookAckDuration = new Trend('webhook_durable_ack_duration', true);
export const regionalRecoveryDuration = new Trend('regional_recovery_duration', true);
export const settlementImportRate = new Trend('settlement_import_rate_records_per_second');
export const settlementImportLag = new Trend('settlement_import_lag_seconds_observed', true);

export function m7Scenario(execName) {
  return boundedScenario(execName, {
    partial_refund_duplicate_count: ['count==0'],
    settlement_reconciliation_mismatch_count: ['count==0'],
  });
}

export function settlementScenario(execName) {
  return boundedScenario(execName, {
    partial_refund_duplicate_count: ['count==0'],
    settlement_import_records_observed: ['count>0'],
    settlement_import_rate_records_per_second: ['avg>0'],
    settlement_import_lag_seconds_observed: ['min>=0'],
  });
}

export function ticketFixture() {
  const token = required('CUSTOMER_TOKEN');
  const orderID = required('TICKET_ORDER_ID');
  const ticketIDs = required('TICKET_IDS').split(',').map((value) => value.trim()).filter(Boolean);
  if (ticketIDs.length < 1 || ticketIDs.length > 100) throw new Error('TICKET_IDS is out of bounds');
  return { token, orderID, ticketIDs };
}

export function requestRefund(base, fixture, key) {
  const started = Date.now();
  const response = http.post(
    `${base.replace(/\/$/, '')}/api/v1/ticket-orders/${encodeURIComponent(fixture.orderID)}/refunds`,
    JSON.stringify({ ticket_ids: fixture.ticketIDs }),
    { headers: paymentHeaders(fixture.token, key), tags: { operation: 'ticket_partial_refund' } },
  );
  partialRefundDuration.add(Date.now() - started);
  record(response, [202], ['refund_processing', 'payment_under_review'], 'ticket_partial_refund');
  return response;
}

export function getRefund(base, token, requestID) {
  const response = http.get(`${base.replace(/\/$/, '')}/api/v1/ticket-refunds/${encodeURIComponent(requestID)}`, {
    headers: bearerHeaders(token), tags: { operation: 'ticket_partial_refund_get' },
  });
  record(response, [200], ['refund_processing', 'payment_under_review'], 'ticket_partial_refund_get');
  return response;
}

export function jsonID(response) {
  try {
    const value = response.json();
    return value && typeof value.id === 'string' ? value.id : '';
  } catch (_) {
    return '';
  }
}

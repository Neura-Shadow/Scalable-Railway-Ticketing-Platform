import encoding from 'k6/encoding';
import exec from 'k6/execution';
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

export const unexpectedResponses = new Counter('payment_unexpected_responses');
export const expectedFaultResponses = new Counter('payment_expected_fault_responses');
export const faultHooksArmed = new Counter('payment_fault_hooks_armed');
export const identityMismatches = new Counter('payment_identity_mismatches');
export const paymentHTTPRequests = new Counter('payment_http_requests');
export const paymentIntentRequests = new Counter('payment_intent_requests');
export const paymentAccepted = new Counter('payment_accepted');
export const paymentHTTPRequestDuration = new Trend('payment_http_request_duration', true);
export const paymentConvergenceDuration = new Trend('payment_convergence_duration', true);

export function required(name) {
  const value = (__ENV[name] || '').trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

export function optional(name, fallback = '') {
  const value = (__ENV[name] || '').trim();
  return value || fallback;
}

export function list(name, minimum = 1) {
  const values = required(name).split(',').map((value) => value.trim()).filter(Boolean);
  if (values.length < minimum) throw new Error(`${name} must contain at least ${minimum} values`);
  return values;
}

export function optionalList(name, fallback) {
  const raw = optional(name, fallback);
  const values = raw.split(',').map((value) => value.trim()).filter(Boolean);
  if (values.length === 0) throw new Error(`${name} must not be empty`);
  return values;
}

export function positiveInteger(name, fallback, maximum = 1000) {
  const value = Number.parseInt(optional(name, `${fallback}`), 10);
  if (!Number.isInteger(value) || value < 1 || value > maximum) {
    throw new Error(`${name} must be between 1 and ${maximum}`);
  }
  return value;
}

export function boundedDuration(name, fallback, maximumMilliseconds = 10 * 60 * 1000) {
  const value = optional(name, fallback);
  const match = /^(\d+)(ms|s|m)$/.exec(value);
  if (!match) throw new Error(`${name} must use ms, s or m units`);
  const multiplier = match[2] === 'm' ? 60000 : match[2] === 's' ? 1000 : 1;
  const milliseconds = Number.parseInt(match[1], 10) * multiplier;
  if (milliseconds < 1 || milliseconds > maximumMilliseconds) {
    throw new Error(`${name} exceeds the bounded maximum`);
  }
  return value;
}

export function boundedScenario(execName = 'default', extraThresholds = {}) {
  const vus = positiveInteger('VUS', 1, 100);
  const iterations = positiveInteger('ITERATIONS_PER_VU', 1, 100);
  return {
    scenarios: {
      payment: {
        executor: 'per-vu-iterations',
        exec: execName,
        vus,
        iterations,
        maxDuration: boundedDuration('DURATION', '2m'),
        gracefulStop: '5s',
      },
    },
    // Exclude URL/name system tags so hosts, ports and payment identifiers do
    // not enter raw metric evidence. Operation tags below are fixed allowlists.
    systemTags: ['method', 'status', 'scenario', 'expected_response'],
    summaryTrendStats: ['count', 'avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
    thresholds: {
      checks: ['rate==1'],
      payment_unexpected_responses: ['count==0'],
      payment_identity_mismatches: ['count==0'],
      payment_http_request_duration: ['p(99)<10000'],
      ...extraThresholds,
    },
  };
}

export function apiURL() {
  return required('BASE_URL').replace(/\/$/, '');
}

export function apiURLs() {
  const configured = optional('BASE_URLS');
  const values = configured
    ? configured.split(',').map((value) => value.trim()).filter(Boolean)
    : [apiURL()];
  return values.map((value) => value.replace(/\/$/, ''));
}

export function sandboxURL() {
  return required('SANDBOX_URL').replace(/\/$/, '');
}

export function bearerHeaders(token) {
  return { Authorization: `Bearer ${token}`, Accept: 'application/json' };
}

export function paymentHeaders(token, key) {
  return {
    ...bearerHeaders(token),
    'Content-Type': 'application/json',
    'Idempotency-Key': key,
  };
}

export function fixture(prefix = '') {
  const tokens = list(`${prefix}CUSTOMER_TOKENS`);
  const reservations = list(`${prefix}RESERVATION_IDS`);
  const iterations = positiveInteger('ITERATIONS_PER_VU', 1, 100);
  const slots = positiveInteger('VUS', 1, 100) * iterations;
  if (reservations.length < slots || (tokens.length !== 1 && tokens.length < slots)) {
    throw new Error(`${prefix}fixtures must cover every configured VU iteration`);
  }
  const slot = (exec.vu.idInTest - 1) * iterations + exec.vu.iterationInScenario;
  return {
    token: tokens.length === 1 ? tokens[0] : tokens[slot],
    reservationID: reservations[slot],
  };
}

export function pairedFixture() {
  const tokens = list('CUSTOMER_TOKENS');
  const reservations = list('RESERVATION_IDS', 2);
  const iterations = positiveInteger('ITERATIONS_PER_VU', 1, 100);
  const slots = positiveInteger('VUS', 1, 100) * iterations;
  if (reservations.length < slots * 2 || (tokens.length !== 1 && tokens.length < slots)) {
    throw new Error('paired fixtures must cover every configured VU iteration');
  }
  const slot = (exec.vu.idInTest - 1) * iterations + exec.vu.iterationInScenario;
  const first = slot * 2;
  return {
    token: tokens.length === 1 ? tokens[0] : tokens[slot],
    firstReservationID: reservations[first],
    secondReservationID: reservations[first + 1],
  };
}

export function iterationKey(prefix) {
  const value = `${prefix}-${exec.scenario.name}-${exec.vu.idInTest}-${exec.vu.iterationInScenario}`;
  return value.slice(0, 120);
}

export function publicErrorCode(response) {
  try {
    const value = response.json('error.code');
    return typeof value === 'string' ? value : 'missing';
  } catch (_) {
    return 'unparseable';
  }
}

export function record(response, expectedStatuses, expectedFaultCodes = [], operation = 'unknown') {
  paymentHTTPRequests.add(1);
  paymentHTTPRequestDuration.add(response.timings.duration);
  const code = publicErrorCode(response);
  const expected = expectedStatuses.includes(response.status);
  const expectedFault = expectedFaultCodes.includes(code);
  if (expectedFault) expectedFaultResponses.add(1);
  if (!expected && !expectedFault) {
    unexpectedResponses.add(1);
    console.error(JSON.stringify({ event: 'unexpected_payment_response', operation, status: response.status, code }));
  }
  return { expected: expected || expectedFault, code };
}

export function createIntent(base, reservationID, token, key, expectedStatuses = [202]) {
  paymentIntentRequests.add(1);
  const response = http.post(
    `${base}/api/v1/reservations/${encodeURIComponent(reservationID)}/payment-intents`,
    '{}',
    { headers: paymentHeaders(token, key), tags: { operation: 'payment_intent_create' } },
  );
  record(response, expectedStatuses, ['payment_processing', 'payment_provider_unavailable'], 'payment_intent_create');
  if (response.status === 202) paymentAccepted.add(1);
  return response;
}

export function intentView(response) {
  if (response.status !== 200 && response.status !== 202) return null;
  try {
    const value = response.json();
    return value && typeof value.id === 'string' ? value : null;
  } catch (_) {
    return null;
  }
}

export function getIntent(base, intentID, token) {
  const response = http.get(`${base}/api/v1/payment-intents/${encodeURIComponent(intentID)}`, {
    headers: bearerHeaders(token), tags: { operation: 'payment_intent_get' },
  });
  record(response, [200], ['payment_processing', 'payment_under_review'], 'payment_intent_get');
  return response;
}

export function cancelIntent(base, intentID, token, key) {
  const response = http.post(
    `${base}/api/v1/payment-intents/${encodeURIComponent(intentID)}/cancel`,
    '{}',
    { headers: paymentHeaders(token, key), tags: { operation: 'payment_intent_cancel' } },
  );
  record(response, [202], ['refund_processing', 'payment_processing', 'payment_under_review'], 'payment_intent_cancel');
  return response;
}

export function authorizeHosted(hostedReference) {
  const prefix = 'sandbox-checkout:';
  if (typeof hostedReference !== 'string' || !hostedReference.startsWith(prefix)) return null;
  const paymentID = hostedReference.slice(prefix.length);
  const response = http.post(
    `${sandboxURL()}/hosted/checkouts/${encodeURIComponent(paymentID)}/authorize`,
    null,
    { tags: { operation: 'sandbox_hosted_authorize' } },
  );
  record(response, [202], [], 'sandbox_hosted_authorize');
  return response;
}

export function pollIntent(base, intentID, token, terminalStates, polls = 30) {
  const boundedPolls = Math.min(Math.max(polls, 1), 120);
  const started = Date.now();
  let view = null;
  for (let attempt = 0; attempt < boundedPolls; attempt += 1) {
    const response = getIntent(base, intentID, token);
    view = intentView(response);
    if (view && terminalStates.includes(view.state)) {
      paymentConvergenceDuration.add(Date.now() - started);
      return view;
    }
    sleep(0.25);
  }
  paymentConvergenceDuration.add(Date.now() - started);
  return view;
}

export function waitForHosted(base, intentID, token) {
  const view = pollIntent(base, intentID, token, ['awaiting_customer', 'authorized', 'capture_pending', 'captured', 'ticket_issue_pending', 'completed', 'manual_review'], positiveInteger('POLL_ATTEMPTS', 40, 120));
  return view && view.hosted_session_ref ? view : null;
}

export function listTicketOrders(base, token) {
  const response = http.get(`${base}/api/v1/ticket-orders?page=1&limit=100&sort=-created_at`, {
    headers: bearerHeaders(token), tags: { operation: 'ticket_order_list' },
  });
  record(response, [200], ['ticket_issuance_processing'], 'ticket_order_list');
  return response;
}

export function ticketOrderForReservation(response, reservationID) {
  if (response.status !== 200) return null;
  try {
    const items = response.json('items');
    if (!Array.isArray(items)) return null;
    return items.find((item) => item && item.reservation_id === reservationID) || null;
  } catch (_) {
    return null;
  }
}

export function queueFault(operation, kind, delaySteps = 0) {
  const operations = ['create_checkout', 'get_payment_status', 'authorize', 'capture', 'void', 'refund'];
  const kinds = [
    'timeout_before_commit', 'timeout_after_commit', 'response_loss', 'rate_limited',
    'provider_error', 'outage', 'invalid_response', 'oversized_response',
    'duplicate_webhook', 'out_of_order_webhook', 'delayed_webhook',
    'refund_transient', 'refund_permanent',
  ];
  if (!operations.includes(operation) || !kinds.includes(kind)) {
    throw new Error('sandbox fault is not allowlisted');
  }
  const isDelayedWebhook = kind === 'delayed_webhook';
  if (!Number.isInteger(delaySteps) || (isDelayedWebhook
    ? delaySteps < 1 || delaySteps > 10000
    : delaySteps !== 0)) {
    throw new Error('sandbox fault delay is invalid');
  }
  if ((kind === 'refund_transient' || kind === 'refund_permanent') && operation !== 'refund') {
    throw new Error('refund fault requires refund operation');
  }
  if (operation === 'get_payment_status' && [
    'timeout_after_commit', 'response_loss', 'duplicate_webhook',
    'out_of_order_webhook', 'delayed_webhook',
  ].includes(kind)) {
    throw new Error('status-query fault combination is invalid');
  }
  const response = http.post(
    `${sandboxURL()}/_sandbox/faults`,
    JSON.stringify({ operation, kind, delay_steps: delaySteps }),
    {
      headers: {
        'Content-Type': 'application/json',
        'X-Sandbox-Control-Token': required('SANDBOX_CONTROL_TOKEN'),
      },
      tags: { operation: 'sandbox_fault_control', fault_kind: kind },
    },
  );
  record(response, [204], [], 'sandbox_fault_control');
  if (response.status === 204) faultHooksArmed.add(1);
  return response;
}

export function advanceSandbox(steps = 1) {
  const response = http.post(
    `${sandboxURL()}/_sandbox/advance`,
    JSON.stringify({ steps }),
    {
      headers: {
        'Content-Type': 'application/json',
        'X-Sandbox-Control-Token': required('SANDBOX_CONTROL_TOKEN'),
      },
      tags: { operation: 'sandbox_advance' },
    },
  );
  record(response, [204], [], 'sandbox_advance');
  return response;
}

export function drainWebhooks() {
  const response = http.get(`${sandboxURL()}/_sandbox/webhooks`, {
    headers: { 'X-Sandbox-Control-Token': required('SANDBOX_CONTROL_TOKEN') },
    tags: { operation: 'sandbox_webhook_drain' },
  });
  record(response, [200], [], 'sandbox_webhook_drain');
  if (response.status !== 200) return [];
  try {
    const value = response.json();
    return Array.isArray(value) ? value.slice(0, 100) : [];
  } catch (_) {
    return [];
  }
}

export function deliverWebhook(base, queued) {
  const headers = queued.Headers || queued.headers || {};
  const encodedBody = queued.Body || queued.body || '';
  const body = encoding.b64decode(encodedBody, 'std', 's');
  const response = http.post(`${base}/webhooks/payments/sandbox`, body, {
    headers: {
      'Content-Type': 'application/json',
      'X-Payment-Key-ID': headers.key_id || headers.KeyID || '',
      'X-Payment-Timestamp': headers.timestamp || headers.Timestamp || '',
      'X-Payment-Signature': headers.signature || headers.Signature || '',
    },
    tags: { operation: 'payment_webhook_ingest' },
  });
  record(response, [200, 202], ['payment_webhook_conflict'], 'payment_webhook_ingest');
  return response;
}

export function deliverDrainedWebhooks(base) {
  return drainWebhooks().map((queued) => deliverWebhook(base, queued));
}

export function assertStableIdentity(first, second) {
  const left = intentView(first);
  const right = intentView(second);
  const stable = Boolean(left && right && left.id === right.id && left.reservation_id === right.reservation_id);
  if (!stable) identityMismatches.add(1);
  return stable;
}

// Client-visible checks are deliberately incomplete. Each scenario must be
// paired with the sanitized database/provider/reconciliation snapshots listed
// in docs/milestone-6-load-testing.md before its status can become passed.

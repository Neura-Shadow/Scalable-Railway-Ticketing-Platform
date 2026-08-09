import { check } from 'k6';

import {
  apiURL,
  authorizeHosted,
  boundedScenario,
  createIntent,
  deliverDrainedWebhooks,
  fixture,
  intentView,
  iterationKey,
  optional,
  pollIntent,
  positiveInteger,
  queueFault,
  waitForHosted,
} from './lib/payment.js';

export const options = boundedScenario('paymentCaptureRecovery', {
  payment_fault_hooks_armed: ['count>0'],
});

export function paymentCaptureRecovery() {
  const actor = fixture();
  const kind = optional('CAPTURE_FAULT_KIND', 'timeout_after_commit');
  const created = createIntent(apiURL(), actor.reservationID, actor.token, iterationKey('m6-capture-recovery'));
  const initial = intentView(created);
  const hosted = initial && waitForHosted(apiURL(), initial.id, actor.token);
  const armed = hosted ? queueFault('capture', kind) : null;
  const authorized = hosted && authorizeHosted(hosted.hosted_session_ref);
  deliverDrainedWebhooks(apiURL());
  const final = initial && pollIntent(
    apiURL(), initial.id, actor.token,
    ['captured', 'ticket_issue_pending', 'completed', 'manual_review', 'failed'],
    positiveInteger('RECOVERY_POLL_ATTEMPTS', 60, 120),
  );
  check(null, {
    'capture recovery fault is armed': () => Boolean(armed && armed.status === 204),
    'customer authorization is accepted': () => Boolean(authorized && authorized.status === 202),
    'capture uncertainty remains visible or converges': () => Boolean(
      final && ['captured', 'ticket_issue_pending', 'completed', 'manual_review'].includes(final.state)
    ),
  });
}

// Provider operation rows and sandbox status snapshots must prove one capture
// and query-before-retry; a customer-visible terminal state cannot prove them.

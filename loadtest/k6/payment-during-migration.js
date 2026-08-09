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
  waitForHosted,
} from './lib/payment.js';

export const options = boundedScenario('paymentDuringMigration');

export function paymentDuringMigration() {
  // MIGRATION_PHASE is a bounded evidence tag only. Migration transitions are
  // driven by the existing operator CLI outside this customer traffic script.
  const phase = optional('MIGRATION_PHASE', 'externally_controlled');
  if (!/^[a-z][a-z0-9_-]{0,31}$/.test(phase)) throw new Error('MIGRATION_PHASE is invalid');
  const actor = fixture();
  const created = createIntent(
    apiURL(), actor.reservationID, actor.token, iterationKey(`m6-migration-${phase}`), [202, 503],
  );
  const initial = intentView(created);
  const hosted = initial && waitForHosted(apiURL(), initial.id, actor.token);
  const authorized = hosted && authorizeHosted(hosted.hosted_session_ref);
  if (authorized) deliverDrainedWebhooks(apiURL());
  const final = initial && pollIntent(
    apiURL(), initial.id, actor.token,
    ['completed', 'manual_review', 'failed', 'refund_pending', 'refunded'],
    positiveInteger('MIGRATION_POLL_ATTEMPTS', 100, 120),
  );
  check(null, {
    'migration window returns bounded create behavior': () => [202, 503].includes(created.status),
    'accepted intent remains addressable across route changes': () => Boolean(
      created.status === 503 || (initial && (authorized === null || authorized.status === 202) && final)
    ),
  });
}

// Both-shard v2 state, fences, journal lag, receipts, stale-route rejection and
// reverse-migration preservation are mandatory external post-run assertions.

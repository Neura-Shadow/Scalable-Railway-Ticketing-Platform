import { check } from 'k6';

import {
  apiURL,
  boundedScenario,
  createIntent,
  fixture,
  intentView,
  iterationKey,
  pollIntent,
  positiveInteger,
} from './lib/payment.js';

export const options = boundedScenario('paymentShardOutage');

export function paymentShardOutage() {
  // The assigned shard outage is an external Compose hook applied before this
  // bounded traffic phase; no customer endpoint is allowed to choose a shard.
  const failed = fixture('OUTAGE_');
  const healthy = fixture('HEALTHY_');
  const failedResponse = createIntent(
    apiURL(), failed.reservationID, failed.token, iterationKey('m6-shard-outage'), [202, 503],
  );
  const healthyResponse = createIntent(
    apiURL(), healthy.reservationID, healthy.token, iterationKey('m6-healthy-shard'), [202],
  );
  const failedIntent = intentView(failedResponse);
  const healthyIntent = intentView(healthyResponse);
  const failedObserved = failedIntent && pollIntent(
    apiURL(), failedIntent.id, failed.token,
    ['reservation_securing', 'checkout_pending', 'manual_review', 'failed'],
    positiveInteger('OUTAGE_POLL_ATTEMPTS', 20, 120),
  );
  check(null, {
    'failed assignment returns bounded failure or durable pending intent': () => Boolean(
      failedResponse.status === 503 || (failedIntent && failedObserved)
    ),
    'healthy shard continues accepting payment intents': () => Boolean(
      healthyResponse.status === 202 && healthyIntent
    ),
  });
}

// The external invariant bundle must prove current-route resolution, absence
// of fallback writes/cross-shard scans, and bounded healthy-shard pool use.

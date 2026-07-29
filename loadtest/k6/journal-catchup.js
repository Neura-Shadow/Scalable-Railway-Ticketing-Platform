import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

import {
  baseURL,
  boundedOptions,
  createHold,
  customerForVU,
  identityMismatches,
  iterationKey,
  positiveInteger,
  required,
  reservationID,
} from './lib/milestone4.js';

export const journalMutationSuccess = new Counter('journal_mutation_success');
export const duplicateApplyEffectObservations = new Counter('duplicate_apply_effect_observations');
export const journalClientReplayDuration = new Trend('journal_client_replay_duration', true);

export const options = boundedOptions({
  mutations_during_catchup: {
    executor: 'shared-iterations',
    vus: positiveInteger('VUS', 6),
    iterations: positiveInteger('ITERATIONS', 36),
    maxDuration: (__ENV.MAX_DURATION || '2m').trim(),
  },
}, {
  journal_mutation_success: ['count>0'],
  duplicate_apply_effect_observations: ['count==0'],
  journal_client_replay_duration: ['p(95)<2000', 'p(99)<5000'],
});

export default function () {
  const url = baseURL();
  const trainRunID = required('TRAIN_RUN_ID');
  const customer = customerForVU();
  const key = iterationKey('m5-journal-catchup');
  const created = createHold(url, trainRunID, customer, key, {
    operation: 'journal_catchup_mutation',
  });
  const replay = createHold(url, trainRunID, customer, key, {
    operation: 'journal_catchup_client_replay', trend: journalClientReplayDuration,
  });
  const createdID = reservationID(created);
  const replayID = reservationID(replay);
  if (createdID && replayID && createdID !== replayID) {
    identityMismatches.add(1);
    duplicateApplyEffectObservations.add(1);
  }
  if (created.status === 201 && replay.status === 201 && createdID === replayID) {
    journalMutationSuccess.add(1);
  }

  check({ created, replay, createdID, replayID }, {
    'journal source mutation commits successfully': (value) => value.created.status === 201,
    'journal workload replay commits successfully': (value) => value.replay.status === 201,
    'journal workload replay preserves one reservation identity': (value) =>
      value.createdID.length > 0 && value.createdID === value.replayID,
  });
  sleep(0.1);
}

// journal_client_replay_duration measures the public idempotent replay only.
// Journal lag, unique apply receipts, and target equality require DB evidence.

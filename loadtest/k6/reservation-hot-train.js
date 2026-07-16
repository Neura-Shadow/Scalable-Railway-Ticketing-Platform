import { check } from 'k6';

import { expectedSeatCount, oneAttemptPerVUOptions } from './lib/config.js';
import { createHold, iterationKey } from './lib/requests.js';

export const options = oneAttemptPerVUOptions({
  successful_holds: [`count<=${expectedSeatCount()}`],
});

export default function () {
  const response = createHold(iterationKey('hot-train'));
  check(response, {
    'hot train returns success or bounded contention': (result) => [201, 409, 429].includes(result.status),
  });
}

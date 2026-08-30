import http from 'k6/http';
import { check } from 'k6';
import { m7Scenario, regionalRecoveryDuration, required } from './lib/milestone7.js';

export const options = m7Scenario('regionalFailback');

export function regionalFailback() {
  const started = Date.now();
  const active = http.get(`${required('FAILBACK_ACTIVE_URL').replace(/\/$/, '')}/readyz`, {tags: {operation: 'failback_active_ready'}});
  const retained = http.get(`${required('RETAINED_REGION_URL').replace(/\/$/, '')}/readyz`, {tags: {operation: 'retained_region_ready'}});
  regionalRecoveryDuration.add(Date.now() - started);
  check(null, {
    'reseeded promoted region is ready': () => active.status === 200,
    'retained old region is not write-ready': () => retained.status !== 200,
  });
}

// Driver evidence must bind the reseed backup, catch-up position, higher epoch,
// reconciliation, ingress switch, and retained-source fencing.

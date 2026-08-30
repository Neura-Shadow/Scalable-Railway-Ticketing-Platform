import http from 'k6/http';
import { check } from 'k6';
import {
  required,
  settlementImportedRecords,
  settlementImportLag,
  settlementImportRate,
  settlementMismatch,
  settlementScenario,
} from './lib/milestone7.js';

export const options = settlementScenario('settlementImport');

function metricTotal(body, family, requiredLabel = '') {
  return body.split('\n').reduce((total, line) => {
    if (!(line.startsWith(`${family}{`) || line.startsWith(`${family} `)) ||
        (requiredLabel && !line.includes(requiredLabel))) return total;
    const fields = line.trim().split(/\s+/);
    const value = Number(fields[fields.length - 1]);
    return Number.isFinite(value) && value >= 0 ? total + value : total;
  }, 0);
}

export function settlementImport() {
  const workerResponse = http.get(`${required('SETTLEMENT_WORKER_URL').replace(/\/$/, '')}/metrics`, {
    tags: { operation: 'settlement_worker_metrics' },
  });
  const durableResponse = http.get(`${required('DURABLE_METRICS_URL').replace(/\/$/, '')}/metrics`, {
    tags: { operation: 'settlement_durable_metrics' },
  });
  const observationSeconds = Number(required('SETTLEMENT_OBSERVATION_SECONDS'));
  const importedRecords = metricTotal(durableResponse.body, 'settlement_import_total', 'result="success"');
  const lagSum = metricTotal(durableResponse.body, 'settlement_lag_seconds_sum');
  const lagCount = metricTotal(durableResponse.body, 'settlement_lag_seconds_count');
  const importRate = observationSeconds > 0 ? importedRecords / observationSeconds : 0;
  const averageLag = lagCount > 0 ? lagSum / lagCount : -1;
  const visibleMismatch = /settlement_reconciliation_mismatch[^\n]* [1-9]/.test(durableResponse.body);
  if (visibleMismatch) settlementMismatch.add(1);
  if (importedRecords > 0) settlementImportedRecords.add(importedRecords);
  if (importRate > 0) settlementImportRate.add(importRate);
  if (averageLag >= 0) settlementImportLag.add(averageLag);
  check({ workerResponse, durableResponse, observationSeconds, importedRecords, importRate, averageLag }, {
    'settlement worker remains observable': (value) => value.workerResponse.status === 200,
    'durable settlement metrics remain observable': (value) => value.durableResponse.status === 200,
    'settlement metrics remain bounded': (value) => value.workerResponse.body.length <= 1048576 && value.durableResponse.body.length <= 1048576,
    'settlement import work is visible': (value) => value.importedRecords > 0,
    'settlement import observation window is valid': (value) => Number.isFinite(value.observationSeconds) && value.observationSeconds > 0,
    'settlement import rate is positive': (value) => Number.isFinite(value.importRate) && value.importRate > 0,
    'settlement lag is measured': (value) => Number.isFinite(value.averageLag) && value.averageLag >= 0,
  });
}

// The evidence driver supplies the bounded end-to-end observation window and
// separately proves durable cursors, replay, interruption resume, and row counts.

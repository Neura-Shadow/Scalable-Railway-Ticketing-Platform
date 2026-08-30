import http from 'k6/http';
import { check } from 'k6';
import { Trend } from 'k6/metrics';
import { m7Scenario, required } from './lib/milestone7.js';

const scenario = m7Scenario('providerContract');
export const options = {
  ...scenario,
  thresholds: { ...scenario.thresholds, provider_contract_operation_duration: ['p(99)<10000'] },
};
const operationDuration = new Trend('provider_contract_operation_duration', true);

function contractGet(base, path, headers, operation, expectedStatus = 200) {
  const started = Date.now();
  const response = http.get(`${base}${path}`, { redirects: 0, headers, tags: { operation } });
  operationDuration.add(Date.now() - started, { operation });
  check(response, {
    [`${operation} returned ${expectedStatus}`]: (value) => value.status === expectedStatus,
    [`${operation} response is bounded`]: (value) => value.body.length <= 65536,
    [`${operation} was not redirected`]: (value) => value.url.indexOf(path.split('?')[0]) >= 0,
  });
  return response;
}

export function providerContract() {
  const base = required('PROVIDER_CONTRACT_URL').replace(/\/$/, '');
  const headers = {
    Authorization: `Bearer ${required('PROVIDER_CONTRACT_API_KEY')}`,
    'Stripe-Account': required('PROVIDER_CONTRACT_ACCOUNT_ID'),
    'Stripe-Version': required('PROVIDER_CONTRACT_API_VERSION'),
  };
  const ready = contractGet(base, '/readyz', {}, 'provider_contract_readiness');
  check(ready, {
    'contract advertises deterministic read-only mode': (value) => {
      const body = value.json();
      return body.provider === 'stripe' && body.mode === 'deterministic_test_contract' && body.mutations_enabled === false;
    },
  });

  const balance = contractGet(base, '/adapter/balance-transactions', headers, 'stripe_adapter_balance_transactions');
  check(balance, {
    'adapter normalized capture settlement evidence': (value) => value.json('adapter') === 'stripe'
      && value.json('result') === 'succeeded' && value.json('provider_record_id') === 'txn_m7_capture'
      && value.json('gross_minor') === 1000 && value.json('fee_minor') === 30
      && value.json('net_minor') === 970 && value.json('currency') === 'TWD',
  });
  const payouts = contractGet(base, '/adapter/payouts', headers, 'stripe_adapter_payouts');
  check(payouts, {
    'adapter normalized paid payout evidence': (value) => value.json('adapter') === 'stripe'
      && value.json('result') === 'succeeded' && value.json('provider_record_id') === 'po_m7_settlement'
      && value.json('amount_minor') === 670 && value.json('currency') === 'TWD'
      && value.json('status') === 'paid',
  });
  const failure = contractGet(base, '/adapter/error-classification', headers, 'stripe_adapter_error_classification');
  check(failure, {
    'adapter normalized read-only 503 classification': (value) => value.json('adapter') === 'stripe'
      && value.json('result') === 'classified' && value.json('category') === 'provider_unavailable'
      && value.json('retryable') === true && value.json('uncertain') === false,
  });

  contractGet(base, '/adapter/balance-transactions', { ...headers, Authorization: 'Bearer sk_test_invalid_contract_key' }, 'provider_auth_rejection', 401);
}

# Payment Provider Conformance

## Purpose

The conformance suite proves the normalized provider contract without teaching
the payment saga provider-specific states. It is capability-driven: required
core cases run for every adapter, while partial refunds, settlement listing,
and payout listing run only when the bounded capability profile enables them.

## Required contract cases

- stable checkout and mutation idempotency identities;
- checkout replay and status retrieval;
- authorization, capture, void, full refund, and supported partial refund;
- timeout before dispatch and uncertain outcome after dispatch;
- response loss and duplicate mutation requests;
- provider 429, 500, 502, 503, and 504 classification;
- malformed, oversized, unknown-state, and contradictory-money responses;
- signed webhook verification, duplicate delivery, and key rotation;
- settlement and payout pagination when supported.

A mutating 5xx, timeout, connection loss, or unreadable response after request
dispatch is classified `timeout_unknown`. The next worker action is a status
query, not a blind second POST. A provider state may advance the saga only when
the common observation evaluator confirms its exact amount and cumulative
captured/refunded totals.

## Test environments

The deterministic sandbox and the Stripe adapter both run the applicable
contract against controlled HTTP servers. Tests use barriers, test clocks, and
bounded fault hooks rather than sleeps. The sandbox supplies deterministic
before-commit, after-commit, response-loss, duplicate, out-of-order, restart,
and key-rotation behavior.

An optional Stripe test-mode job is separate from standard CI and is never a
substitute for deterministic conformance. Its sanitized result states whether
it ran, the pinned API version, capability profile, and cleanup result; it never
publishes object payloads, keys, signatures, hosted-session secrets, or account
identifiers.

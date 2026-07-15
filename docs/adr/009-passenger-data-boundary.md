# ADR 009: Minimize and Isolate Passenger Data

- Status: Accepted
- Date: 2026-07-15

## Context

Milestone 1 must associate one owned passenger with each allocated seat, but it does not perform national identity verification. Collecting government identifiers would add breach impact and key-management obligations without satisfying a current requirement.

## Decision

The passenger record contains only:

- opaque passenger ID;
- owning user ID;
- display name;
- creation/update timestamps.

The customer owns all passenger CRUD operations and reservation references. HTTP authorization derives the user from a validated JWT and applies ownership in repository predicates; request bodies cannot select another owner. Admin/operator roles do not receive blanket passenger-data access through operational endpoints.

Display names are validated for length and safe Unicode handling. They are not included in metrics, routine logs, cache keys, outbox payloads, or infrastructure error details. Reservation/ticket responses expose passenger data only to the owning customer through the minimum necessary representation.

Tests and k6 fixtures use synthetic data. No real documents, raw identifiers, tokens, or passenger payloads are committed.

If a later milestone introduces document identifiers, it requires a new ADR covering:

- envelope encryption at rest with managed key rotation;
- keyed hashes for exact equality lookup;
- strict redaction and partial display;
- access audit trails and retention/deletion policy;
- incident response and data-subject obligations; and
- synthetic-only non-production values.

## Consequences

- Milestone 1 cannot claim real identity verification or fraud prevention.
- Compromise still exposes display names and travel associations, so access controls, encryption in transit, database controls, and data minimization remain security-critical.
- Operational event consumers do not need passenger identity data.

## Rejected alternatives

- Government identifier fields for realism: rejected because they are not validated and increase harm.
- Passenger identity in event payloads/cache keys: rejected because downstream proliferation breaks minimization.
- Authorization by a `user_id` request field: rejected as an ownership-bypass risk.

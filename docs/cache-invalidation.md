# Cache Invalidation

Milestone 3 invalidates by rotating a collision-resistant generation token at
an exact Redis key. It never enumerates old cache entries. Redis is outside the
PostgreSQL source transaction, so invalidation is at-least-once and eventually
consistent rather than a distributed transaction.

| Event | Projection | Station generation | Search generation | Availability generation |
|---|---:|---:|---:|---:|
| `station.created/updated/disabled` | affected route runs | yes | yes | no |
| `route.created/updated/disabled` | route runs | no | yes | no |
| `train.updated` | train runs | no | yes | no |
| `coach.updated` | no | no | no | affected runs |
| `seat.updated` | no | no | no | affected runs |
| `fare.created/updated/disabled` | affected runs | no | yes | no |
| `trainrun.created/updated/cancelled` | named run | no | yes | named run |
| `reservation.held/confirmed/cancelled/expired` | no | no | no | named run |
| `ticket.created` | no | no | no | named run |

The implementation accepts only the event/aggregate pairs declared by the
migration constraint and coordinator allowlist. Impact queries are
parameterized, deterministically ordered, and capped at 100 train runs per
event.

## Failure sequencing

Projection/receipt work commits first. Required generation rotations run next.
The stream entry is acknowledged last. If Redis rotation fails, the entry
remains pending. On retry, the receipt suppresses another projection mutation
while the version rotation is attempted again. After the configured maximum,
the safe event envelope enters the bounded DLQ before ACK.

This sequence does not promise instantaneous invalidation. TTLs bound stale
read data even when delivery is delayed. Booking correctness does not depend on
delivery or rotation.

## Version loss

`GetOrCreate` is one Lua operation. A missing or malformed pointer receives a
new CSPRNG token rather than a predictable counter or a reused old token. A
complete Redis flush therefore creates fresh namespaces on demand; old values
cannot become current merely because a pointer disappeared.

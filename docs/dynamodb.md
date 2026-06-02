# DynamoDB Integration

## Event Log Table

The event log table uses:

- `pk` (`S`) as the entity ID
- `sk` (`N`) as the event sequence
- `sk = 0` as the inline snapshot row for that entity
- DynamoDB Streams with `NEW_IMAGE` when using async projectors or publishers

Event rows are append-only and protected by conditional writes.

## Entity Views Table

The view table uses:

- `pk` (`S`) as the projection-owned lookup key
- `sk` (`S`) as the view sort key, defaulting to `VIEW`
- `entityType` (`S`) for `entityType-index`
- `ttl` (`N`) when expiring derived rows is useful

Views are rebuildable cache, not source of truth.

## Local Tables

The `infra/local` Terraform stack creates emulator tables that match the test
suite. Apply it before running integration tests.

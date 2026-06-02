# Concepts

## Entity

An entity is an aggregate root whose state is reconstructed from events. It owns
business invariants and applies facts.

## Command

A command is a request to change state. It may return zero or more events plus
optional transaction groups for related writes.

## Event

An event is an immutable fact. Events are versioned so payload schemas can evolve
with explicit upcasters.

## Repository

A repository persists serialized events and snapshots. The DynamoDB repository
stores event rows under stable `pk` and `sk` keys and uses conditional writes to
protect per-entity ordering.

## Store

A store coordinates load, handle, serialize, commit, and apply operations. The
same aggregate can run with an in-memory or DynamoDB repository.

## Projector

A projector turns event-sourced state into deterministic read models. Projectors
should be idempotent and safe to replay.

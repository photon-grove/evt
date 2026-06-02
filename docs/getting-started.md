# Getting Started

## 1. Define an Aggregate

An aggregate implements `evt.Entity`: it handles commands, applies events, and
knows how to deserialize historical events back into current Go types.

Keep command validation and invariant checks in command handling. Keep state
mutation in `Apply`.

## 2. Write the First Test With `mem`

Use `mem.NewStore()` or `mem.NewStoreFromRepo()` for fast tests. This exercises
the same `evt.Store` interface as production without requiring DynamoDB.

## 3. Add Metadata

`evt.Metadata` carries optional command ID, trace context, origin, address,
region, and timestamp. A stable command ID is the practical path to safe retries.

## 4. Move to DynamoDB

Create:

- an event-log table with `pk` string and `sk` number
- an entity-views table with `pk` string, `sk` string, and `entityType-index`

Use `dynamo.NewRepository` and `snapshots.NewStore` for production writes.

## 5. Project Views Deliberately

Views are derived state. If a projection cannot be rebuilt by replaying events,
the missing state should be represented as an event first.

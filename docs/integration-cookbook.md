# Integration Cookbook

## Use Fact Tables and Projection Tables Separately

Keep event rows and view rows in separate tables. This makes table scans,
rebuilds, TTLs, and operational permissions easier to reason about.

## Rebuild Before You Patch

If a view is wrong, prefer fixing the projector and rebuilding over hand-editing
view rows. Hand edits disappear on the next rebuild and hide missing events.

## Command IDs Are Retry Keys

Set `Metadata.CommandID` from an idempotency key, request ID, job ID, or message
ID. Duplicate command attempts should fail as duplicates, not produce new facts.

## Upcast Historical Shapes

Never assume every stored payload has the latest struct shape. Add an upcaster
and a fixture test whenever event JSON changes.

## Test Against the Real DynamoDB Shape

Unit tests should use `mem`; integration tests should hit DynamoDB-compatible
tables with the exact key and index shapes used in production.

## Treat Stream Handlers as Batch Processors

Stream projectors and publishers should process records independently, report
partial failures precisely, and preserve retry safety for successful records.

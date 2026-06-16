# Public-Safe Configuration

`evt` is a public, open-source repository extracted from production patterns. The
patterns are real; the specifics are not. Nothing here — code, docs, examples,
tests, fixtures, or generated artifacts — should carry private repo names, private
hostnames, account IDs, real ARNs, secrets, customer data, internal tickets, or
environment-specific deployment names. This page is the bar to clear before
committing.

## Use neutral, local examples

Prefer placeholder names and the local emulator over anything production-shaped:

- table names like `evt-local-event-log` and `evt-local-entity-views`
- topic names like `example.events`
- local emulator credentials: `AWS_ACCESS_KEY_ID=test`,
  `AWS_SECRET_ACCESS_KEY=test`, `AWS_REGION=us-west-2`
- a local endpoint such as `AWS_ENDPOINT_URL=http://localhost:4566`
- placeholder ARNs built from documented dummy account IDs

If a useful pattern came from private work, describe the **general engineering
rule** and adapt it to this repository's public API — don't transplant the
private specifics.

## The production build tag

One safety rule is enforced by the compiler, not just review: the snapshot-unsafe
`dynamo.Delete` helper is guarded by `//go:build !prod` and is excluded from
production builds (released binaries build with `-tags prod`). It exists for
local and staging fixtures only. For any principled truncation in real systems,
use [`CompactBelow`](dynamodb.md#compaction), which refuses to delete events a
durable snapshot doesn't already cover.

## Checklist before committing

- No private repo names, hostnames, account IDs, ARNs, or deployment names.
- No real credentials or secrets — emulator placeholders only.
- No customer/user data or internal incident details in fixtures or logs.
- Screenshots and generated artifacts scrubbed of the above.
- New infrastructure examples point at the local emulator, not real AWS.

Keep real infrastructure configuration in adopter repositories, where it belongs.

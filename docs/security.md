# Public-Safe Configuration

This repository should not contain private account IDs, application hostnames,
production ARNs, secrets, or environment-specific deployment names.

Acceptable examples:

- `evt-local-event-log`
- `evt-local-entity-views`
- `example.events`
- local emulator credentials such as `AWS_ACCESS_KEY_ID=test`
- placeholder ARNs using documented dummy account IDs

When adding docs or examples, prefer neutral names and local emulator endpoints.
Keep real infrastructure configuration in adopter repositories.

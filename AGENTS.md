# Agent Rules

## Instruction File Goal

- `AGENTS.md` is for high-signal steering, not full repository documentation.
- Keep `AGENTS.md` as the single source of truth; `CLAUDE.md` should symlink to this file.
- Add rules only for repeated real failures; prefer fixing code, tooling, or tests first.
- Do not commit raw generated rule files without manual pruning.
- This is a public repository. Do not add private application names, environment details, account
  identifiers, credentials, internal incident details, or non-public infrastructure references.

## Repository Identity

- This repo is `evt`, a Go event-sourcing framework published as `github.com/photon-grove/evt`.
- It contains Go framework packages, a Vite docs app, a React diagram package, local Terraform for
  DynamoDB-compatible integration tests, and public documentation.
- The project is intentionally public-safe: examples should use neutral names, local emulator
  credentials, and no private application environment details.
- Verify the remote if uncertain: `git remote get-url origin`.

## Critical Invariants

- Never commit secrets.
- Immutable domain events are the source of truth. Views/projections are derived state and must be
  safe to wipe and rebuild by replaying events.
- Event logs are append-only and retained in full by default. Compaction
  (`evt.Compactor.CompactBelow`) may delete events only where a durable snapshot already covers them
  (`snapshot.EventSequence >= throughSequence`); it must never delete the `sk=0` snapshot. After a
  stream is compacted, rebuild it via the snapshot-aware path (`RebuildConfig.SeedEntity` /
  `evt.SnapshotStreamer`), never full-replay-from-1. See
  `docs/adr/0001-event-compaction-and-snapshot-truncation.md`.
- The raw `dynamo.Delete` is snapshot-unsafe and for local/staging fixtures only. It is guarded by
  `//go:build !prod`; production builds must use `-tags prod` (released binaries already do). Prefer
  `CompactBelow` for any principled truncation.
- Do not persist mutable decisions, external signals, publish flags, accept/reject verdicts, or
  review state directly to a view table with no backing event.
- DynamoDB event-log key patterns must stay stable. Preserve the documented `pk`/`sk`, inline
  snapshot, metadata, and serialized event formats unless the change is intentional and documented.
- Avoid table scans except for explicit migration, rebuild, or diagnostic flows.
- When event JSON changes, add or update upcasters and fixture coverage so older stored events
  remain readable.
- Update documentation when behavior changes, especially `README.md`, `BEHAVIORAL_INVARIANTS.md`,
  and relevant files under `docs/`.

## Codebase Patterns

- Structure:
  - Root Go module: core `evt` package and support packages such as `dynamo`, `postgres`, `mem`,
    `snapshots`, `projectors`, `publishers`, `viewstore`, and `test`.
  - `website/`: Vite documentation site.
  - `packages/react-flow-diagrams/`: reusable React diagram package for docs.
  - `infra/local/`: Terraform stack for local DynamoDB-compatible integration testing.
  - `infra/local-postgres/`: Terraform stack for local PostgreSQL integration testing.
  - `examples/`: public examples; keep names and scenarios neutral.
- Go:
  - Prefer small, explicit APIs that preserve existing package contracts.
  - Keep internal refactors compatible with exported framework behavior unless the breaking change is
    intentional and documented.
  - Use `gofmt -w -s` formatting.
  - Keep function bodies readable with blank lines between logical groups and before non-trivial
    returns. Prefer helper functions when spacing alone is not enough.
- TypeScript/React:
  - Use the repo's existing Vite, React, TypeScript, and package patterns.
  - Keep docs examples public, neutral, and inspectable.
  - Use explicit exports for barrels when adding them; avoid leaking server-only or implementation
    internals through public client entry points.
- Terraform:
  - `infra/local` is for local emulator resources only.
  - Do not add real cloud account details, private endpoints, or production infrastructure to this
    public repo.

## Tooling

- Package manager: `pnpm` 10.x.
- Node: 24.x, with Moon configured around Node `~24.5`.
- Go toolchain: Go 1.26.4 via `go.mod`.
- Common commands:
  - Install dependencies: `pnpm install`
  - Go tests: `go test ./...`
  - Moon tests: `pnpm exec moon run evt:test`
  - Go vet/check: `pnpm exec moon run evt:check`
  - Website typecheck/build: `pnpm exec moon run website:typecheck website:build`
  - React diagram tests: `pnpm exec moon run react-flow-diagrams:test`

## Integration Tests

- DynamoDB integration tests run against a local AWS emulator, not real AWS.
- Standard local setup:

  ```sh
  docker run -d --name evt-moto -p 4566:5000 motoserver/moto:5.1.22
  terraform -chdir=infra/local init
  terraform -chdir=infra/local apply -auto-approve
  AWS_ENDPOINT_URL=http://localhost:4566 pnpm exec moon run evt:integration
  ```

- Integration-test credentials should be local emulator placeholders only:
  `AWS_REGION=us-west-2`, `AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test`.
- If readiness fails, inspect `scripts/check-integration-readiness.sh`, Moto logs, and the local
  Terraform outputs before changing test code.

## CI and Workflow

- CI runs on ARM64 Ubuntu runners (`ubuntu-24.04-arm`).
- Use upstream `actions/cache@v5`; do not introduce archived cache forks.
- CI quality checks include:
  - `go mod tidy` with no `go.mod`/`go.sum` diff.
  - `go vet ./...`
  - `go test ./...`
  - `pnpm exec moon run evt:check evt:test website:typecheck website:build react-flow-diagrams:test`
- CI integration checks start Moto, apply `infra/local`, and run `pnpm exec moon run
  evt:integration`.
- Before staging, inspect dependency and workspace churn:

  ```sh
  git diff -- go.mod go.sum '**/package.json' pnpm-lock.yaml
  ```

- Revert unrelated churn introduced by tooling unless it is required by the change.
- Run the smallest relevant verification first, then broaden to CI-equivalent checks when touching
  shared behavior, serialization, DynamoDB contracts, docs builds, or package exports.

## Git and PR Rules

- Keep diffs focused; split unrelated changes.
- Never use `--no-verify` for commit or push.
- Never push directly to `main`.
- Prefer conventional commit shape when committing:

  ```text
  <type>(<scope>): <summary>

  Why: <rationale>
  ```

- Use `gh` for GitHub operations when needed.
- PR descriptions should be concise Markdown, start with `## Summary`, and include verification
  notes when manual evidence or non-obvious test selection matters.
- An automatic PR review agent reviews every PR and posts findings as inline review threads; treat
  them like any reviewer's — reply, then resolve before merge. The reviewer is configured outside the
  repo (provider settings), so keep guidance here agent-agnostic. What it flags is tuned by the
  `## Review guidelines` section below.

## Review guidelines

These instructions steer the automatic PR review agent (and apply to the closest `AGENTS.md` to each
changed file). Severities: **P0** = must fix before merge; **P1** = should fix; otherwise a nit.

- Focus on correctness over style; reserve **P0** for changes that break behavior or corrupt stored
  data. Style, naming, and refactor suggestions are nits at most.
- **P0** — flag mutable state (decisions, external signals, publish flags, accept/reject verdicts,
  review state) persisted directly to a view/projection table with no backing event; projections
  must stay safe to wipe and rebuild by replay.
- **P0** — flag changes to the DynamoDB event-log `pk`/`sk`, inline snapshot, metadata, or
  serialized-event formats that aren't intentional and documented, and table scans outside an
  explicit migration/rebuild/diagnostic flow.
- **P0** — flag event-JSON changes that lack a matching upcaster + fixture coverage keeping older
  stored events readable.
- **P1** — flag new behavior without test coverage, and documentation a change makes stale
  (especially `README.md`, `BEHAVIORAL_INVARIANTS.md`, and `docs/`).
- **P1** — flag accidental breaks to existing exported package contracts.
- Do not report issues in generated code or lockfiles, and do not re-flag anything CI already
  enforces (`go vet`, formatting/lint, `go mod tidy`). This is a public repo: also flag any private
  repo names, hostnames, account IDs, credentials, or non-public details that slip into docs,
  examples, or fixtures (see the Public-Safety Checklist).
- Keep reviews actionable: cap nits at roughly five and summarize the rest as a count. On re-review,
  prefer P0/P1 and suppress new nits.

## Public-Safety Checklist

- Before committing docs, examples, logs, screenshots, or generated artifacts, check that they do not
  contain private repo names, private hostnames, account IDs, real credentials, customer/user data,
  internal tickets, or non-public operational details.
- Prefer neutral examples such as `account`, `order`, `banking`, `event-log`, `views`, and local
  emulator table names.
- If a useful pattern came from private work, describe only the general engineering rule and adapt it
  to this repository's public API and documentation.

# Agent Rules

## Instruction File Goal

- `AGENTS.md` is for high-signal steering, not full repository documentation.
- Keep `AGENTS.md` as the single source of truth; `CLAUDE.md` should symlink to this file.
- Add rules only for repeated real failures; prefer fixing code, tooling, or tests first.
- Do not commit raw generated rule files without manual pruning.
- This is a public repository. Do not add private application names, environment details, account
  identifiers, credentials, internal incident details, or non-public infrastructure references.

## Agent Memory

- Maintainer agent sessions may have a cross-session memory MCP server. Its connection details
  never belong in this repo; without such a server, skip this section.
- This repo's memory group is `photon-grove/evt`; every call names it.
- **Recall first:** at the start of substantive work, `search_memory {query, group:
  "photon-grove/evt"}` for the topic (and `get_memory` for a known id).
- **Remember outcomes:** before ending substantive work, `remember` what a future session needs
  (decisions with reasons, gotchas, follow-ups) as `{id: "<kebab-topic>", group:
  "photon-grove/evt", corpus: "explicit", source: "<PR/issue/path>", text}`. Deterministic ids
  overwrite instead of duplicating.
- Session transcripts sync automatically through harness hooks, so keep `remember` for distilled
  outcomes, not raw logs. Never store secrets.

## Repository Identity

- This repo is `evt`, a Go event-sourcing framework published as `github.com/photon-grove/evt/go`.
- It contains the Go module (`go/`), native Rust crates (`crates/`) that share the Go storage
  format, a TypeScript implementation that joins them (`packages/evt`, ADR 0009), a Vite docs site
  (`website/`), a React diagram package (`packages/react-flow-diagrams`), local Terraform for
  integration tests (`infra/`), and the docs (`docs/`).
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
  stream is compacted, rebuild it via the snapshot-aware path (`RebuildConfig.SeedAggregate` /
  `evt.SnapshotStreamer`; Rust `ProjectionRebuild` / `stream_entities_from_snapshots`), never
  full-replay-from-1. See
  `docs/adr/0001-event-compaction-and-snapshot-truncation.md`.
- Do not persist mutable decisions, external signals, publish flags, accept/reject verdicts, or
  review state directly to a view table with no backing event.
- DynamoDB event-log key patterns must stay stable. Preserve the documented `pk`/`sk`, inline
  snapshot, metadata, and serialized event formats unless the change is intentional and documented.
- Avoid table scans except for explicit migration, rebuild, or diagnostic flows.
- When event JSON changes, add or update upcasters and fixture coverage so older stored events
  remain readable.
- Go and Rust share one storage format (`docs/reference/storage-format.md`), and TypeScript checks
  itself against its fixtures. Any change to it must regenerate `compat/fixtures`
  (`go -C go test ./internal/compat -run TestFixtures -update`), pass in Go and Rust, pass the
  TypeScript fixture runner (`pnpm exec moon run evt-ts:test evt-ts:runtimes`), and pass
  `scripts/compat-integration.sh dynamo|postgres`. Never hand-edit fixtures.
  The exception is a Rust-first extension's fixtures (ADR 0005): Rust generates them,
  `compat/fixtures/journal.json` with `EVT_UPDATE_FIXTURES=1 cargo test -p evt-compat --test
  journal_fixtures` and `compat/fixtures/lambda.json` with `EVT_UPDATE_FIXTURES=1 cargo test -p
  photon-evt-lambda --all-features --test fixtures`.
- Update documentation when behavior changes: the README of the unit you changed (`README.md`,
  `go/README.md`, `crates/*/README.md`), `docs/guide/`, `docs/reference/storage-format.md` (normative),
  and `docs/internal/behavioral-invariants.md`.

## Codebase Patterns

- Structure:
  - `go/`: the Go module (`github.com/photon-grove/evt/go`, package `evt`). Run Go commands from
    there. The backends are `go/backends/dynamo`, `go/backends/postgres` and `go/backends/mem`, and
    the other packages sit beside them: `snapshots`, `projectors`, `viewcodec`, `test` and so on.
  - `website/`: Vite documentation site. It renders `docs/**/*.md` except `docs/internal/`, and
    `website/src/docs.ts` (`DOC_CONFIG`) orders and groups the pages.
  - `docs/`: `guide/` (how-tos), `reference/` (storage format, feature matrix), `adr/` (decisions,
    kept as history), `internal/` (process notes, behavioral invariants). Plan and epic IDs
    (K1, C3, X2 and so on) belong only in `adr/` and `internal/`, never in user-facing prose.
  - `packages/react-flow-diagrams/`: reusable React diagram package for docs.
  - `packages/evt` (`@photon-grove/evt`, moon project `evt-ts`): the TypeScript implementation,
    built phase by phase from ADR 0009. `packages/evt-compat` (private) is its fixture runner. TS
    checks itself against `compat/fixtures` and generates none.
  - `infra/local/`: Terraform stack for local DynamoDB-compatible integration testing.
  - `infra/local-postgres/`: Terraform stack for local PostgreSQL integration testing.
  - `go/examples/`: public examples; keep names and scenarios neutral.
  - `crates/evt` (`photon-evt`): the Rust crate. `crates/evt-lambda` (`photon-evt-lambda`): its
    DynamoDB Streams / Lambda helpers (ADR 0005, X3). `crates/evt-compat` (unpublished) and
    `go/internal/compat` (Go) are the two halves of the compatibility harness. `compat/` holds the
    shared scenarios and generated fixtures.
- Rust:
  - Keep the core dependency-light (serde, serde_json, thiserror); SDKs stay behind the `dynamodb`
    and `postgres` features, and live-backend tests stay behind `evt-compat`'s `drivers` feature.
  - Encode stored JSON with `photon_evt::json`, not plain `serde_json`, so bytes match Go.
  - New repository behavior gets a case in `photon_evt::conformance`.
- Go:
  - Prefer small, explicit APIs that preserve existing package contracts.
  - Keep internal refactors compatible with exported framework behavior unless the breaking change is
    intentional and documented.
  - Use `gofmt -w -s` formatting.
  - Keep function bodies readable with blank lines between logical groups and before non-trivial
    returns. Prefer helper functions when spacing alone is not enough.
- TypeScript (`packages/evt`):
  - `src/` may use only the web-standard globals declared in `src/globals.d.ts`: no Node built-ins,
    `Buffer` or `process`. `evt-ts:check` enforces it with `types: []` and an import check.
  - ESM only, no runtime dependencies, and no top-level await in the package.
  - `packages/evt-compat` imports the package through its `exports` map, never `src/`, and stays
    framework-free so Bun, Deno and Node 22 run it unchanged.
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
- Node: 24.x, with Moon configured around Node `~24.5`. The TypeScript package also supports Node
  22.12+, Bun and Deno, pinned in `.prototools` and in CI's TypeScript job.
- Go toolchain: Go 1.26.4 via `go/go.mod`.
- Rust toolchain: 1.99.0 via `rust-toolchain.toml` (crate MSRV 1.88, `rust-version` in `Cargo.toml`,
  checked in CI without the `dynamodb` feature, which follows the AWS SDK's MSRV).
- Common commands:
  - Install dependencies: `pnpm install`
  - Go tests (from `go/`): `go test ./...`
  - Moon tests: `pnpm exec moon run evt:test`
  - Go vet/check: `pnpm exec moon run evt:check`
  - Go lint (golangci-lint, must report zero findings): `pnpm exec moon run evt:lint`
  - Website typecheck/build: `pnpm exec moon run website:typecheck website:build`
  - React diagram tests: `pnpm exec moon run react-flow-diagrams:test`
  - TypeScript (`@photon-grove/evt`): `pnpm exec moon run evt-ts:check evt-ts:test evt-ts:runtimes`
    (tsc, oxlint with `--deny-warnings`, the import check, Vitest, and the fixture runner on Node,
    Bun and Deno)
  - Rust (toolchain pinned in `rust-toolchain.toml`): `pnpm exec moon run evt-rust:check
    evt-rust:test` (fmt check, clippy `-D warnings`, tests)
  - Rust and cross-language integration: `pnpm exec moon run evt-rust:integration
    evt-rust:integration-postgres`

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
- Use upstream `actions/cache` (currently `@v6`); do not introduce archived cache forks.
- CI quality checks include:
  - From `go/`: `go mod tidy` with no `go.mod`/`go.sum` diff, `go vet ./...` and `go test ./...`.
  - `pnpm exec moon run evt:check evt:lint evt:test website:typecheck website:build react-flow-diagrams:test`
  - `pnpm exec moon run evt-rust:check evt-rust:test` (Rust job)
  - `pnpm exec moon run evt-ts:check evt-ts:test evt-ts:runtimes`, then the fixture runner on Node
    22.12 (TypeScript job)
- CI integration checks start Moto or PostgreSQL, apply the local Terraform, and run
  `evt:integration` / `evt:integration-postgres`, plus `evt-rust:integration` /
  `evt-rust:integration-postgres` when the storage format may have changed.
- A `changes` job classifies changed paths: Rust-only changes skip Go jobs, Go-only changes skip
  the Rust job, and only TypeScript, fixture and shared-config changes run the TypeScript job. Keep
  its path lists in `.github/workflows/ci.yml` in sync when adding directories. Go lives under
  `go/`, and the release tags are `go/vX.Y.Z`.
- Before staging, inspect dependency and workspace churn:

  ```sh
  git diff -- go/go.mod go/go.sum '**/package.json' pnpm-lock.yaml Cargo.lock
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
- PR bodies follow the shared
  [`/pr-description` skill](https://github.com/photon-grove/skills/blob/main/skills/pr-description/SKILL.md):
  a reviewer gets why + what in under a minute. `## Summary` (1–3 sentences, why-first, one clause
  of newcomer context) + `## Change` (1–2 sentences, key call sites, scope/safety — what isn't
  affected); optional one-line tail (related work, caveat, follow-up). Show, don't tell: link code
  at a pinned commit SHA, evidence via durable query URLs, related PRs/issues as full URLs (never
  bare `#123`). Omit routine CI/validation notes; add a one-line validation note only for
  change-specific checks CI doesn't cover (manual evidence, non-obvious test selection). Keep the
  PR template's checklist.
- An automatic PR review agent reviews every PR and posts findings as inline review threads; treat
  them like any reviewer's — reply, then resolve before merge. The reviewer is configured outside the
  repo (provider settings), so keep guidance here agent-agnostic. What it flags is tuned by the
  `## Review guidelines` section below.
- Review-agent budget (its quota is limited): run a local adversarial review and fix its findings
  before marking a PR ready. Max two agent rounds per PR: the automatic pass on ready, plus one
  re-request only if that pass raised P0/P1. Verify later fixes with a local review, reply to and
  resolve the threads, and file leftover below-P1 findings (nits, edge cases) as follow-up issues.
  Prefer smaller PRs for core changes over repeated review rounds on one.

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
- **P0** — flag storage-format changes (DynamoDB items, PostgreSQL rows, serialized JSON, stream
  envelopes, snapshot policy) that do not regenerate `compat/fixtures` and update both the Go and
  Rust implementations, or that change the documented Go/Rust differences without updating
  `docs/reference/storage-format.md`.
- **P1** — flag new behavior without test coverage, and documentation a change makes stale
  (the READMEs, `docs/guide/`, `docs/reference/`, and `docs/internal/behavioral-invariants.md`).
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

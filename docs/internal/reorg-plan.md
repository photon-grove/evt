# Reorganizing the repository

Status: proposed, waiting on review. This plan moves the Go module into `go/`, regroups the
subpackages, renames the identifiers that trip people up, and rewrites the docs front door. The
wire format does not move: stored items, key layouts, marker names, compat fixtures and
conformance semantics stay byte-for-byte identical. This is a layout and naming cleanup, and the
Rust crate stays where it is.

Three decisions are already made, so I won't relitigate them. The Go module moves into `go/`
beside `crates/` and `packages/`, its module path becomes `github.com/photon-grove/evt/go`, and
its release tags become `go/vX.Y.Z`. Public API names may change where they confuse,
as deliberate breaking changes with Go and Rust kept parallel. And nothing in this plan touches
what gets persisted.

## Where the repo stands

The root has 45 Go files in one package, thirteen Go subpackage directories (counting
`result/`, which is cut-bound anyway), and then Rust, a docs
site, a React package, Terraform, fixtures, and a native CLI all sharing the same top level. A
newcomer lands on `atomic.go` at line 1 and has no way to know that `crates/` exists, let alone
that it reads the same event log.

The 45 root files group cleanly by concern:

- Core types and contracts: `doc.go`, `types.go`, `repository.go`, `errors.go` (with
  `errors_test.go`, `entity_test.go`, `command_test.go` holding the test fixtures).
- Events, metadata and serialization: `serialization.go`, `eventjson.go`, `metadata.go`,
  `events_test.go`, `serialize_test.go`, `eventjson_test.go`, `metadata_test.go`,
  `metadata_propagation_test.go`, `upcasting_test.go`, `sort_test.go`.
- Commits and transactions: `atomic.go` (the multi-stream commit path), `transactions.go`, plus
  their tests.
- Outcomes and rejections: `outcome.go` (1,029 lines, the largest file in the repo),
  `rejection_numbers.go`, `outcome_test.go`.
- Projections and views: `projection.go` (916 lines), `views.go`, `projection_test.go`,
  `view_repo_test.go`, `viewcodec_test.go`.
- Rebuild and fence: `rebuild.go`, `rebuildfence.go`, `incremental.go` (deprecated, see the cut
  list), `rebuild_test.go`, `rebuildfence_test.go`, `replay_strictness_test.go`.
- Heads, namespaces, features: `heads.go`, `heads_migration.go`, `namespaces.go`, `features.go`,
  `namespaces_test.go`, `feature_binding_test.go`.
- Compaction: `compaction.go`, `compaction_test.go`, and `snapshot_test.go` (documents the
  serialized snapshot shape; there is no root `snapshot.go`, the type lives in `serialization.go`).

The Go subpackages, in one line each: `dynamo` (the DynamoDB backend, with `integration`,
`test`, `mock`, and a test-only `awsmeasure` alongside it), `postgres`, `mem` (backends),
`snapshots` (the snapshot-aware store decorator and its execute/retry/outcome helpers),
`projectors` (stream projector runtime), `publishers` (SNS fan-out, deprecated), `stream`
(SNS/SQS adapters and the stream decode handler), `policy` (retry classification and hooks,
deprecated), `viewstore` (typed JSON codec for view rows), `test` (the shared test aggregate),
`conformance` (the Go side of the shared suite), `internal` (compat harness, event JSON,
jsondefaults, streamheads, docexamples), `cmd/evt-doctor`, and `examples/banking`.

Outside Go: `crates/` holds `evt` (photon-evt), `evt-lambda` and `evt-compat`. `compat/` holds
the shared scenarios and generated fixtures. `conformance/` is Go. `website/` is the Vite docs
site and reads every `docs/**/*.md` at build time. `packages/react-flow-diagrams` is the diagram
toolkit. `infra/local` and `infra/local-postgres` stand up the emulators. `scripts/` has the
readiness checks and `compat-integration.sh`. `docs/` is markdown plus the spec and ADRs. The
root markdown files: README (36KB and carrying quick start, package table, deprecations, core
discipline and local dev all at once), AGENTS.md (CLAUDE.md symlinks to it),
BEHAVIORAL_INVARIANTS.md (71KB of agent-facing contract), CONTRIBUTING, SECURITY, CHANGELOG.

Places that hardcode paths, so the move PR knows its full blast radius:

- `go.mod` (module path), and the import lines in 230 Go files (108 of them non-test sources,
  the rest tests).
- `.moon/workspace.yml` (project roots) and `.moon/tasks/go.yml` (task args like
  `./dynamo/integration` and `scripts/` paths); `moon.yml` in `crates/`, `packages/`, `website/`.
- `.github/workflows/ci.yml` (the changed-file classifier lists root Go filenames and
  `dynamo/*`-style globs, `setup-go` reads `go.mod`), `release.yml` (release-please config and
  manifest, goreleaser, `go-version-file: go.mod`), `pages.yml` (`website/dist`).
- `.goreleaser.yml` (`main: ./cmd/evt-doctor`), `.golangci.yml` (no paths, but it must run from
  the module directory), `.prototools` (tool pins only).
- `release-please-config.json` and `.release-please-manifest.json` (package paths and
  changelog paths; `crates/evt-lambda`'s extra-files point at `/crates/...` and stay).
- `.github/dependabot.yml` (`gomod` at `directory: /`, terraform at `infra/*`).
- `scripts/compat-integration.sh` builds `./internal/compat/cmd/evt-compat`; the readiness
  scripts reference `infra/`.
- The website: `src/docs.ts` globs `../../docs/` and keys its ordering config by slug, and
  `src/content.ts` hardcodes `go get github.com/photon-grove/evt` and a packages list with
  import paths.
- Docs cross-links and README links use relative paths like `docs/spec/storage-format.md`;
  AGENTS.md and CONTRIBUTING.md reference `go.mod`, `dynamo/integration` and moon task ids.
- The compat harness: `internal/compat` reads `compat/fixtures` by relative path, and the Rust
  `evt-compat` crate does the same from its side. `compat/` itself doesn't move, so the Rust
  side is untouched.

Public API surface, meaning only the names a human actually stumbles on:

- `evt.Context` is the per-command execution context (the loaded entity, its id, the current
  sequence and snapshot, and the command ids seen during replay), and
  it sits in signatures next to `context.Context`. Two different "contexts" in one line is the
  single most common stumble in the Go API.
- Go calls the thing an `Entity` (`Entity`, `EntityID`, `EntityType`, `EntityFactory`). Rust
  calls the same concept `Aggregate` but kept `EntityId` for the identifier. Docs, ADRs and
  everyday speech all say aggregate. This is the one real Go/Rust inconsistency.
- `viewstore` the package, `views.go` the file, `ViewRepository` the interface, and Rust's
  `viewcodec` module with `ViewCodec`: four view-ish names for two concepts.
- `dynamo.Transaction` is an alias of `evt.Transaction` and both are exported.
- `Rejectioner` looks odd but is the idiomatic Go `-er` of its `Rejection()` method, and Rust's
  parallel trait is `ToRejection`. I'd keep both and document the mapping.
- `Endpoint`, `Source` and `Origin` in metadata are vague words, but the Rust `Origin` has the
  same `source`/`endpoint` shape, so they are already parallel. Keep.
- `StorageType` values differ in convention (`dynamo.StorageType` is `"DynamoDB"`,
  `postgres.StorageTypePostgres` is `"postgres"`). Before normalizing
  anyone should verify the string is never persisted; it appears in backend-capability checks.
- History leaks from the cut list (`CommitStream`, the freshness observers, the `ByQuery` rebuild
  sources, `HeadStore`, `publishers` and the SNS publisher, the `policy` hooks, and the K7
  stragglers) are deleted in the cut phase, not renamed, so the reorg doesn't carry them into
  `go/` twice. See the decisions below for the two items that stay.

The docs, with audience and currency:

| Document | Audience | State |
| --- | --- | --- |
| `README.md` | users | current but bloated; five docs in one |
| `BEHAVIORAL_INVARIANTS.md` | agents | current, overlaps the storage spec |
| `CONTRIBUTING.md` | contributors | current; paths go stale in the move |
| `SECURITY.md` | users | current, stays |
| `CHANGELOG.md` | users | current; belongs to the Go module |
| `AGENTS.md` / `CLAUDE.md` | agents | current; needs path updates |
| `docs/getting-started.md` | users | current |
| `docs/concepts.md` | users | current |
| `docs/projections.md` | users | current |
| `docs/streams.md` | users | current (SNS fan-out already marked deprecated) |
| `docs/dynamodb.md` | users | current |
| `docs/postgres.md` | users | current |
| `docs/integration-cookbook.md` | users | current, thin (102 lines) |
| `docs/migrating-to-stream-heads.md` | operators | current, it is the evt-doctor runbook |
| `docs/spec/storage-format.md` | users + contributors | current, normative |
| `docs/parity/feature-matrix.md` | contributors + agents | current, tracking source of truth |
| `docs/adr/0001–0007` | contributors | history, keep as is |
| `compat/README.md`, crate and package READMEs | contributors | current, stay |

## Part 1 — the target tree

Here is the layout I'm proposing, two to three levels deep:

```
photon-grove/evt/
├── README.md                  # short: what evt is, why, a 30-second example per language
├── LICENSE, CONTRIBUTING.md, SECURITY.md
├── AGENTS.md (CLAUDE.md → symlink)
├── Cargo.toml, Cargo.lock, rust-toolchain.toml     # Rust workspace, unchanged
├── package.json, pnpm-workspace.yaml, tsconfig.json
├── .github/, .moon/, .prototools, .golangci.yml, .goreleaser.yml
├── release-please-config.json, .release-please-manifest.json
├── go/                        # the Go module: github.com/photon-grove/evt/go, package evt
│   ├── go.mod, go.sum, moon.yml, README.md, CHANGELOG.md
│   ├── *.go                   # the 45 core files, still one package: evt
│   ├── backends/
│   │   ├── dynamo/            # package dynamo (+ integration/, test/)
│   │   ├── mem/               # package mem
│   │   └── postgres/          # package postgres
│   ├── projectors/            # package projectors
│   ├── stream/                # package stream (what survives the K5 cut)
│   ├── snapshots/             # package snapshots
│   ├── viewcodec/             # package viewcodec (renamed from viewstore)
│   ├── conformance/           # package conformance (the shared Go suite)
│   ├── test/                  # package test (shared test aggregate)
│   ├── cmd/evt-doctor/
│   ├── examples/banking/
│   └── internal/              # compat/, eventjson/, jsondefaults/, streamheads/, docexamples/
├── crates/                    # evt (photon-evt), evt-lambda, evt-compat — unchanged
├── packages/react-flow-diagrams/
├── website/
├── compat/                    # shared scenarios + generated fixtures — unchanged
├── docs/
│   ├── guide/                 # getting-started, concepts, dynamodb, postgres, projections,
│   │                          #   streams, integration, migrating-to-stream-heads
│   ├── reference/             # storage-format.md, feature-matrix.md
│   ├── adr/                   # 0001–0007, kept as history
│   └── internal/              # reorg-plan.md, behavioral-invariants.md
├── infra/local, infra/local-postgres
└── scripts/
```

The first call is whether the root package stays one package or splits. I recommend keeping it
as one package living directly in `go/`, so the core import is
`github.com/photon-grove/evt/go` with the package name still `evt`. Splitting the core into
`go/outcome`, `go/projection` and friends would move every exported identifier into a different
package, which breaks consumer code far beyond a find-and-replace on the import path, and it
buys little because the real problem was the repo root, not the package itself. One flat core
package that the guide walks through end to end is honest about what evt is: a handful of
interfaces. Go allows the package name to differ from the last path segment, and every
editor and the language server resolve `package evt` automatically, so the one-time surprise of
typing a `go` import that isn't `package go` costs nothing after the first look.

Within that package I'd keep the 45 files mostly as they are for the move itself, then take one
small consolidation pass afterwards: `rejection_numbers.go` becomes `numbers.go` (parallel to
Rust's `numbers.rs`), `eventjson.go` folds into `serialization.go`, and `snapshot_test.go`
joins `serialize_test.go`. File moves within a package are zero-impact, so this can ride any
later PR.

The backends group under `backends/` because that's the distinction a reader actually needs:
`dynamo`, `postgres` and `mem` all implement the same backend-neutral contracts, and everything
else is a building block layered on top. Import paths read like
`github.com/photon-grove/evt/go/backends/dynamo`. The package names stay `dynamo`, `postgres`
and `mem`, so consumer code only changes on the import line. The building blocks
(`projectors`, `stream`, `snapshots`, `viewcodec`, `conformance`, `test`) stay at the module
root. Nesting them one level deeper than the backends they support would suggest a hierarchy
that isn't there.

`internal/` moves to `go/internal/`, which keeps Go's internal-visibility rule intact for the
new module path. `cmd/evt-doctor` becomes `go/cmd/evt-doctor` so goreleaser can keep building
it from the module. `examples/banking` becomes `go/examples/banking` so `go test ./...` and the
example live in the same module.

On the Rust side nothing moves. `crates/` is already right, and `compat/` stays at the repo
root where both the Go and Rust halves of the compat harness expect it.

## Part 2 — the move table

Every move is `git mv` so history follows, and the move commit stays separate from edit
commits.

| Old path | New path | Notes |
| --- | --- | --- |
| `go.mod`, `go.sum` | `go/go.mod`, `go/go.sum` | module path becomes `github.com/photon-grove/evt/go` |
| 45 root `.go` files | `go/*.go` | names unchanged in the move PR |
| `dynamo/` | `go/backends/dynamo/` | package `dynamo` |
| `mem/` | `go/backends/mem/` | package `mem` |
| `postgres/` | `go/backends/postgres/` | package `postgres` |
| `snapshots/` | `go/snapshots/` | freshness (`K2`) and `do.go` (`K7`) are already deleted |
| `projectors/` | `go/projectors/` | |
| `publishers/` | — | K5 cut, already deleted |
| `stream/` | `go/stream/` | the SNS publisher is already gone |
| `policy/` | `go/policy/` | K6 already removed the hooks and `ExecuteWithRetry`, and what stays is the error classification and the backoff helpers |
| `result/` | `go/result/` | stays for now, because the repository stream contracts still send `result.Result` over channels |
| `viewstore/` | `go/viewcodec/` | rename rides the API-rename wave |
| `test/` | `go/test/` | |
| `conformance/` | `go/conformance/` | |
| `internal/` | `go/internal/` | compat harness included |
| `cmd/evt-doctor` | `go/cmd/evt-doctor/` | |
| `examples/banking` | `go/examples/banking/` | |
| `moon.yml` | `go/moon.yml` | project id stays `evt` so task names hold |
| `CHANGELOG.md` | `go/CHANGELOG.md` | release-please changelog for the `go` package |
| `dynamo/awsmeasure/` | — | folded into dynamo tests in the tidy wave (`dynamo/mock` already moved to `dynamo/internal/mock`) |
| `docs/concepts.md` etc. | `docs/guide/…` | see the docs map |
| `docs/spec/storage-format.md` | `docs/reference/storage-format.md` | |
| `docs/parity/feature-matrix.md` | `docs/reference/feature-matrix.md` | links updated in the same PR |
| `BEHAVIORAL_INVARIANTS.md` | `docs/internal/behavioral-invariants.md` | move first, dedupe against the spec later |
| `README.md` | `README.md` | rewritten in place, short |
| `compat/`, `crates/`, `packages/`, `website/`, `infra/`, `scripts/`, `docs/adr/` | unchanged | |

## Part 3 — proposed API renames

All breaking, all in one release, all paralleled in Rust where the concept exists.

| Language | Old | New | Why |
| --- | --- | --- | --- |
| Go | module `github.com/photon-grove/evt` | module `github.com/photon-grove/evt/go` | language directories; Rust and TS already have theirs |
| Go | `Entity` | `Aggregate` | Rust's trait is `Aggregate`, the docs say aggregate, and `Entity` reads like a database row. `EntityID`/`EntityType` stay, matching Rust's `EntityId` |
| Go | `Context` | `CommandContext` | stops colliding with `context.Context` in every `Execute` signature |
| Go | package `viewstore` | package `viewcodec` | matches Rust's `viewcodec` module and `ViewCodec`; retires one of the four view names |
| Go | `dynamo.Transaction` | (deleted) | alias of `evt.Transaction`; keep one name |

Renames I considered and rejected, so the review doesn't have to: `Rejectioner` → anything
(it's the correct Go `-er` idiom and Rust's `ToRejection` documents the mapping), renaming
`Origin`/`Source`/`Endpoint` (already parallel in Rust), renaming the `snapshots` package (the
name says what it does), and flipping Rust to say `Entity` (the ecosystem language is
aggregate). I'd also leave `photon-evt` as the crate name. The one-sentence story is that the
Go module is `evt/go` and the Rust crate is `photon-evt`, and both READMEs will say it.

The `Entity` → `Aggregate` rename is the biggest single call in this plan. It touches every
consumer's aggregate definition. I still think it belongs in this release, because consumers
are already doing a mechanical find-and-replace for the import path, and adding
`Entity` → `Aggregate` to the same pass makes the cost nearly flat while removing the
last Go/Rust vocabulary mismatch. If you disagree, dropping it doesn't disturb anything else
in the plan.

## Part 4 — the docs map

Moves and rewrites:

- Root `README.md` gets rewritten to the newcomer front door. Outline: what evt is in two
  sentences (an event-sourcing framework for Go with a wire-compatible Rust implementation), why
  you'd use it (immutable events, one shared log across languages, backend-neutral contracts),
  a 30-second Go example and a 30-second Rust example, a repo map (`go/`, `crates/`, `packages/`,
  `website/`, `compat/`), and where to go next (docs site, guide, reference). Target length is
  about 150 lines. The packages table moves to `go/README.md`, the deprecations section moves
  into the guide's relevant pages, and Local Development moves to CONTRIBUTING.
- New `go/README.md`: install line for the new module path, the package table (core, backends,
  building blocks), a link to the guide and the storage spec, and one sentence on the
  `go/vX.Y.Z` tag scheme. `crates/evt/README.md` gets the same one-sentence treatment in
  reverse. Crate and package READMEs otherwise stay.
- `docs/` splits into `guide/` (the eight how-to pages listed in the tree), `reference/`
  (storage-format and the feature matrix) and `internal/` (agent and process notes). ADRs stay
  put. Nothing is deleted and nothing is merged. The one consolidation is the later
  invariants-vs-spec dedupe described below. `integration-cookbook.md` moves to
  `guide/integration.md` under its existing title rather than being merged. It's thin but it
  answers a distinct question.
- `BEHAVIORAL_INVARIANTS.md` moves to `docs/internal/behavioral-invariants.md` unchanged, then a
  later edit commit dedupes it against the storage spec, leaving the spec normative and the
  invariants file pointing at it. Doing the move and the dedupe as separate commits keeps the
  review honest.
- `AGENTS.md` and `CONTRIBUTING.md` get path and command updates in the move PR, and AGENTS.md
  loses anything the reorg makes moot.

The website needs three edits and no redesign: `src/docs.ts` keeps globbing `../../docs/` (the
prefix is unchanged) but its `DOC_CONFIG` keys gain the `guide/` and `reference/` prefixes,
`src/content.ts` swaps the install command to `go get github.com/photon-grove/evt/go` and
updates its packages list, and cross-links inside the moved markdown get the new relative
paths. Docs cross-links, ADR links and the feature-matrix links get swept in the same PR as
each move that breaks them.

## Part 5 — release and tooling impact

The release-please Go package moves from `.` to `go/`. Today `include-component-in-tag: false`
makes plain `vX.Y.Z` tags. The `go` package entry sets a component of `go` with the tag
separator set to `/` (the schema supports `tag-separator`), so tags come out `go/vX.Y.Z`,
which is what Go's module proxy expects for a module in a subdirectory. The manifest entry
becomes `"go": "0.9.0"` (or whatever the pinned first version is), and `go/CHANGELOG.md`
continues from the current changelog, with the old entries riding along under `git mv`. The
existing `v0.1.0` through `v0.8.0` tags stay where they are. They remain resolvable for
the old import path forever, and `go get github.com/photon-grove/evt@v0.8.0` keeps working for
anyone who hasn't moved.

The first `go/` release: I recommend the move rides the core-reshape release, so the first tag
is `go/v0.9.0` and consumers get one breaking cutover for the module path. Most of the
reshape is already on main (stream heads, commit-wide projectors, the rebuild fence, outcomes),
and the README already markets v0.9.0 deprecations. If the remaining reshape work slips, the
fallback is to ship v0.9.0 at the old path and make the move `go/v0.10.0`. Nothing in this plan
changes except the number, so I've left the version pinned as an open question.

goreleaser keeps building `evt-doctor`: the build gains `dir: go` (or an equivalent) so it
compiles inside the new module, `main` stays `./cmd/evt-doctor` relative to that directory, and
the `prod` tag build flag is unchanged. One thing to verify with a dry run during the move PR:
how goreleaser derives the version from a `go/v0.9.0` tag, since the prefix is new. If it
mis-parses, the fix is a version template or a tag-prefix flag, not a change to the tag scheme.

CI: the changed-file classifier in `ci.yml` swaps its root Go filename list for `go/**`, keeps
`crates/*` and `compat/*` as they are, and `setup-go` reads `go/go.mod`. The moon graph check
needs no change once `.moon/workspace.yml` lists `go` instead of `.`. Dependabot moves the
gomod ecosystem to `directory: /go`. The moon project stays id `evt` (its `moon.yml` just moves
into `go/`), so `evt:check`, `evt:lint`, `evt:test`, `evt:integration` and
`evt:integration-postgres` keep their names while their args pick up the `backends/` level.
golangci-lint runs from `go/` now, which the moon lint task handles by running in the project
directory. `scripts/compat-integration.sh` builds `./go/internal/compat/cmd/evt-compat`, and
the Rust side of the harness is untouched because `compat/` didn't move.

Downstream migration is one `go get` plus one find-and-replace, in that order:

```sh
go get github.com/photon-grove/evt/go@v0.9.0
# then, across the repo:
#   "github.com/photon-grove/evt"        → "github.com/photon-grove/evt/go"
#   "github.com/photon-grove/evt/<pkg>"  → "github.com/photon-grove/evt/go/backends/<pkg>"  (backends)
```

The import rewrite must match the exact module path or with a trailing slash first, so
`github.com/photon-grove/evt/go` never becomes `github.com/photon-grove/evt/go/go`. The rename
wave (Entity → Aggregate, Context → CommandContext, viewstore → viewcodec) lands in the next
minor and is a second, smaller mechanical pass.

### Phase breakdown

Ordered PRs, each reviewable on its own, sizes S (under ~200 lines), M, L:

1. **Plan** (S) — this document. No code moves.
2. **Module move** (L, mechanical) — `git mv` of everything in the move table, the module path
   change, and the 230 import rewrites in one commit. Then tooling in a second commit:
   release-please, goreleaser, CI filters, moon, dependabot, compat script. Then docs links,
   website config, AGENTS.md and CONTRIBUTING.md in a third. Reviewers can skip commit one
   entirely.
3. **Docs restructure** (M) — `docs/guide`, `docs/reference`, `docs/internal`, the
   BEHAVIORAL_INVARIANTS move, website DOC_CONFIG slugs, and the cross-link sweep.
4. **READMEs** (M) — root README rewrite and `go/README.md`.
5. **Renames, big** (M, breaking) — `Entity` → `Aggregate`, `Context` → `CommandContext`,
   across code, tests, conformance and examples.
6. **Renames, small** (S, breaking) — `viewstore` → `viewcodec`, delete `dynamo.Transaction`,
   fold `awsmeasure`, the `StorageType` string check, the file consolidations.
7. **Cut deletions** (done, and moved ahead of the module move) — K1 through K7 landed in one
   PR with one commit per group, so dead code isn't moved into `go/`. The decisions behind the
   reorder are recorded below.
8. **Final sweep** (S) — AGENTS.md trim, a links pass, and updating the feature matrix rows for
   anything a rename touched (renames don't change capabilities, so most rows won't move).

Waves 2–4 are the reorg proper and can ship as one release. Waves 5–7 are breaking API work
that lands in the following minor, aligned with the vNext cut schedule.

## Decisions

The owner settled the first three open questions after the plan was written.

The cuts happen now. K1 through K7 are deleted in their own phase, before the module move,
which answers question 2. The reorg then moves a smaller, living codebase into `go/`. Two items
on the list stay, because non-cut code still needs them: the `result` package, since
`Repository.StreamAllEvents`, `StreamEntities` and the rebuild streams send `result.Result` values
over channels, and the `evt.EntityHeadStreamer` and `EntityHeadVisitor` interfaces, which the mem
and postgres backends implement. Both can be reconsidered when the stream contracts are reshaped.

Go `Entity` becomes `Aggregate`, which answers question 3. `EntityID` and `EntityType` keep their
names, because they name the identity of a stream and not the domain object. Go `Context` becomes
`CommandContext`, and it is passed explicitly beside `context.Context`. It is not embedded in or
pulled out of `context.Context`. Both renames land in a later phase and not in the cut PR.

The module move ships as `go/v0.9.0`, which answers question 1.

Since the Go package sits at 0.8.0, the breaking commits of the cut PR would take release-please
to 1.0.0, so the Go package now sets `bump-minor-pre-major`. A breaking change bumps the minor
until the owner decides to cut 1.0.0 on purpose.

## Open questions

1. ~~Which minor carries the module move?~~ Decided: `go/v0.9.0`.
2. ~~Do the K-cut deletions ride this effort?~~ Decided: yes, deleted now, ahead of the move.
3. ~~`Entity` → `Aggregate`: in or out?~~ Decided: in, with `Context` → `CommandContext` passed
   explicitly.
4. After K5, does what survives in `stream/` merge into `projectors/` (one fan-out story) or
   stay a separate package?
5. Is `StorageType`'s string ever persisted? If not, normalize `"DynamoDB"`/`"postgres"` in the
   small-rename wave. If yes, it's a wire format and stays.
6. goreleaser's version parsing for `go/`-prefixed tags — confirm with a dry run in the move PR.

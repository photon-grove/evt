# Contributing

The repo holds three things that ship separately: the Go module in `go/`, the Rust crates in
`crates/`, and the docs site in `website/` (with its diagram package in `packages/`). The Go and
Rust sides share one storage format, so most changes touch one language, and the ones that touch
the format touch both. The [docs map](docs/) splits the guide, the reference spec, the ADRs and
internal process notes.

## Local checks

Install the JavaScript workspace once, then run the Go and website checks:

```sh
pnpm install
(cd go && go test ./...)
moon run evt:test
moon run evt:lint
moon run website:build
```

`evt:lint` runs golangci-lint with `.golangci.yml`, and CI fails on any finding. The Go toolchain
comes from `go/go.mod`.

The Rust toolchain is pinned in `rust-toolchain.toml`, and `rustup` installs it on first use.
`evt-rust:check` is the format check plus clippy with `-D warnings`, and `evt-rust:test` runs the
crate tests with the feature sets CI uses:

```sh
cargo fmt --all
moon run evt-rust:check evt-rust:test
```

## Integration tests

The DynamoDB tests run against Moto, a local AWS emulator, never real AWS. Terraform creates the
tables:

```sh
docker run -d --name evt-moto -p 4566:5000 motoserver/moto:5.1.22
terraform -chdir=infra/local init
terraform -chdir=infra/local apply -auto-approve
AWS_ENDPOINT_URL=http://localhost:4566 moon run evt:integration
```

The PostgreSQL tests need a local server. Terraform provisions the database and role, and
`postgres.Repository.EnsureSchema` owns the tables:

```sh
docker run -d --name evt-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:17
terraform -chdir=infra/local-postgres init
terraform -chdir=infra/local-postgres apply -auto-approve
moon run evt:integration-postgres
```

With both stacks up, `moon run evt-rust:integration evt-rust:integration-postgres` runs the Rust
conformance suite and the Go/Rust cross-language suite against each backend. The cross-language
half is `scripts/compat-integration.sh dynamo` and `scripts/compat-integration.sh postgres`, where
Go and Rust take turns writing the same streams.

## Storage format changes

DynamoDB items, PostgreSQL rows, serialized JSON, stream envelopes and the snapshot policy are all
part of the [storage format](docs/reference/storage-format.md). Go and Rust both read and write
it, so a change there has a longer checklist:

1. Regenerate the fixtures with `go -C go test ./internal/compat -run TestFixtures -update`.
2. Pass `go test ./...` from `go/` and `cargo test --workspace`.
3. Pass both live cross-language runs.
4. Keep older stored data readable, with upcasters and fixture coverage.
5. Update the spec. [`compat/README.md`](compat/README.md) has the details.

## Pull requests

Keep framework behavior compatible unless the change ships as a deliberate breaking change. Add
tests for any change to storage, serialization or retry behavior. Docs and tests use neutral
example names and local emulator values, because the repo is public.

## Releases

Release Please runs on every push to `main` and keeps a release pull request open with the next
version and changelog. It can also be started by hand through `workflow_dispatch`. Opening that
pull request relies on the organization setting that lets GitHub Actions create pull requests. If
that setting is ever turned off again, pass the action a bot or PAT token from a secret that can
open pull requests.

Each published unit is versioned on its own. The Go module is tagged `go/vX.Y.Z`, which is the
form Go's module proxy expects for a module in a subdirectory. When Release Please cuts a Go
release, the `goreleaser` job in `release.yml` builds `evt-doctor` and uploads the archives to
that `go/vX.Y.Z` release. GoReleaser only parses plain semver tags, so it builds from a local
`vX.Y.Z` tag that is never pushed. The Rust crates are tagged
`photon-evt-vX.Y.Z` and `photon-evt-lambda-vX.Y.Z`. In 0.x, features bump the patch version and
breaking changes bump the minor version. Merging a crate's release pull request runs its publish
job in `release.yml`, which pushes it to crates.io.

crates.io only allows trusted publishing for a crate that already exists, so a crate's first
release authenticates with a `CARGO_REGISTRY_TOKEN` secret. After that, configure a trusted
publisher on crates.io (repository `photon-grove/evt`, workflow `release.yml`, environment
`crates-io`) and delete the secret. Later releases authenticate over OIDC.

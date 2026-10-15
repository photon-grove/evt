# Contributing

## Local Checks

```sh
pnpm install
go test ./...
moon run evt:lint
moon run website:build
```

`evt:lint` runs golangci-lint with `.golangci.yml`. CI fails on any finding.

For the Rust crates (toolchain pinned in `rust-toolchain.toml`):

```sh
cargo fmt --all
moon run evt-rust:check evt-rust:test
```

For DynamoDB integration:

```sh
docker run -d --name evt-moto -p 4566:5000 motoserver/moto:5.1.22
terraform -chdir=infra/local init
terraform -chdir=infra/local apply -auto-approve
AWS_ENDPOINT_URL=http://localhost:4566 moon run evt:integration
moon run evt-rust:integration
```

`evt-rust:integration` runs the Rust conformance suite and the Go/Rust
cross-language suite (`scripts/compat-integration.sh dynamo`).
`evt-rust:integration-postgres` does the same against the `infra/local-postgres`
stack.

## Storage Format Changes

The Go and Rust implementations share one storage format
([spec](docs/spec/storage-format.md)). A change to DynamoDB items, PostgreSQL rows,
serialized JSON, envelopes, or the snapshot policy must:

1. regenerate the fixtures with `go test ./internal/compat -run TestFixtures -update`;
2. pass `go test ./...` and `cargo test --workspace`;
3. pass both live cross-language runs;
4. keep older stored data readable (upcasters plus fixture coverage);
5. update the spec. See [`compat/README.md`](compat/README.md).

## Pull Requests

Keep framework behavior compatible unless the change is explicitly released as a
breaking change. Add tests for storage, serialization, or retry behavior changes.

Use neutral example names and local emulator values in docs and tests.

## Releases

Published GitHub releases trigger GoReleaser to build `evt-doctor` artifacts.

Release Please runs on every push to `main`, maintaining a release pull request
with the next version and changelog. It can also be triggered manually via
`workflow_dispatch`. Creating that pull request relies on the organization
setting that allows GitHub Actions to create pull requests; if that is ever
disabled again, supply a bot or PAT token via a secret (with permission to open
pull requests) and pass it to the action instead.

Release Please versions the Rust crate separately (`crates/evt`, tags
`photon-evt-vX.Y.Z`). In 0.x, features bump the patch version and breaking
changes bump the minor version. Merging its release pull request runs the
`publish-crate` job in `release.yml`, which publishes `photon-evt` to crates.io.
crates.io only allows trusted publishing for crates that already exist, so the
first release authenticates with a `CARGO_REGISTRY_TOKEN` secret. After that
release, configure a trusted publisher on crates.io (repository
`photon-grove/evt`, workflow `release.yml`, environment `crates-io`) and delete
the secret. Later releases authenticate over OIDC.

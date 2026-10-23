# evt-compat

This crate is the Rust half of the harness that keeps [`photon-evt`](../evt) and the Go module
wire-compatible. It isn't published. It implements a shared account domain, runs the scenarios in
[`compat/scenarios.json`](../../compat/scenarios.json), and checks what Rust writes and reads
against the golden fixtures in [`compat/fixtures/`](../../compat/fixtures). The
[`compat/` README](../../compat/README.md) lists each fixture, which language generates it, and how
to regenerate it.

The fixture tests run offline as part of the workspace:

```sh
cargo test -p evt-compat
```

The `drivers` feature adds the `evt-compat` binary and the tests that need a live DynamoDB or
PostgreSQL. [`scripts/compat-integration.sh`](../../scripts/compat-integration.sh) builds this binary
and the Go driver, then has them take turns writing the same streams, one scenario step at a time,
before both verify every stream against the fixtures:

```sh
scripts/compat-integration.sh dynamo
scripts/compat-integration.sh postgres
```

Both runs need the local backends that [CONTRIBUTING](../../CONTRIBUTING.md) describes. The format
they hold each other to is the [storage format](../../docs/reference/storage-format.md).

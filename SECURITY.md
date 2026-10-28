# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through
[GitHub Security Advisories](https://github.com/photon-grove/evt/security/advisories/new) on this
repository, not as public issues. Include the affected implementation (Go, `photon-evt` or
`photon-evt-lambda`), the version, and a reproduction if you have one. We'll acknowledge the
report, work on a fix in a private advisory, and credit you when the fix is published unless you'd
rather we didn't.

## Supported versions

evt is pre-1.0. Fixes land on the latest release of each published unit: the Go module
(`go/vX.Y.Z` tags), `photon-evt`, and `photon-evt-lambda`. Older minor versions don't get
backports.

## What stays out of this repo

The repo is public, so it should never contain production secrets, private account IDs, private
hostnames or environment names from any project that uses evt. Examples and tests use local
emulator endpoints and placeholder values only.

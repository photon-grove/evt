# Changelog

## [0.3.0](https://github.com/photon-grove/evt/compare/photon-evt-lambda-v0.2.0...photon-evt-lambda-v0.3.0) (2026-11-02)


### ⚠ BREAKING CHANGES

* **journal:** a repository handle not wrapped by JournalRepository is refused on a journal table, even in a build with the journal feature. JournalRepository::new now requires R: FeatureMarker, and a decorator placed under the journal must forward accept_journal to its inner repository.

### Bug Fixes

* **journal:** accept the journal marker per repository handle ([#257](https://github.com/photon-grove/evt/issues/257)) ([bda29fe](https://github.com/photon-grove/evt/commit/bda29fe53931cbb987f184a87a928ed8704e2797))

## [0.2.0](https://github.com/photon-grove/evt/compare/photon-evt-lambda-v0.1.0...photon-evt-lambda-v0.2.0) (2026-10-30)


### ⚠ BREAKING CHANGES

* **lambda:** photon-evt-lambda 0.2 requires photon-evt 0.3. Stay on photon-evt-lambda 0.1 with photon-evt 0.2.

### Features

* **lambda:** release photon-evt-lambda 0.2 for photon-evt 0.3 ([#242](https://github.com/photon-grove/evt/issues/242)) ([ebec986](https://github.com/photon-grove/evt/commit/ebec9862a826e25c9c4867021043a2a741cd065f))

## 0.1.0 (2026-10-26)


### ⚠ BREAKING CHANGES

* Repository no longer has CommitStream. Commit batched commands through Repository.Commit.

### Features

* **lambda:** photon-evt-lambda crate and release-please package (X3) ([#189](https://github.com/photon-grove/evt/issues/189)) ([8223a15](https://github.com/photon-grove/evt/commit/8223a15cd943ad4eac97aade5017ebc041134637))


### Documentation

* reorganize and rewrite the docs for humans ([#198](https://github.com/photon-grove/evt/issues/198)) ([f413eb1](https://github.com/photon-grove/evt/commit/f413eb14a6e692d05b4ae34dbf1f41d4aaa43e58))

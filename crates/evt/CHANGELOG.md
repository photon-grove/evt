# Changelog

## [0.2.0](https://github.com/photon-grove/evt/compare/photon-evt-v0.1.0...photon-evt-v0.2.0) (2026-10-23)


### Features

* **core:** add latest(n), reserved namespaces, and the feature marker (E2.2) ([#140](https://github.com/photon-grove/evt/issues/140)) ([fa2ef5a](https://github.com/photon-grove/evt/commit/fa2ef5a4a80aed307fe0874561ea8f06ae566238))
* **core:** command outcomes and rejections (C3+C4) ([#149](https://github.com/photon-grove/evt/issues/149)) ([54b1eb8](https://github.com/photon-grove/evt/commit/54b1eb88c0498fcbcf8a4d269f520c26c1df1ea8))
* **core:** commit-wide projectors, any-pk/sk views, per-operation errors (C7) ([#166](https://github.com/photon-grove/evt/issues/166)) ([0e2c796](https://github.com/photon-grove/evt/commit/0e2c796d5e6405e63f014c155a69727d8699b8bf))
* **core:** fence-aware retry (C5) ([#151](https://github.com/photon-grove/evt/issues/151)) ([b6ae1fb](https://github.com/photon-grove/evt/commit/b6ae1fb2fde4113dfdb5d5b374ab2f0255455c53))
* **core:** multi-stream transactions and read fences (C1+C2) ([#145](https://github.com/photon-grove/evt/issues/145)) ([6a7e774](https://github.com/photon-grove/evt/commit/6a7e77483e6d42f41d6886854f18dae6d8bb8c9c))
* **core:** per-stream head item for cheaper commits and read fences (ADR 0007) ([#164](https://github.com/photon-grove/evt/issues/164)) ([37c5d92](https://github.com/photon-grove/evt/commit/37c5d92941a39e49ef09d48be3586599059a9c2c))
* **core:** rebuild fence in Go and Rust (C10) ([#176](https://github.com/photon-grove/evt/issues/176)) ([f5508b8](https://github.com/photon-grove/evt/commit/f5508b8337815d452614f148ede55fdccdcd00c5))
* **core:** rejected outcomes that carry events ([#157](https://github.com/photon-grove/evt/issues/157)) ([bcffcb0](https://github.com/photon-grove/evt/commit/bcffcb0f7fa01bb95eb32296b640066a10eeba27)), closes [#156](https://github.com/photon-grove/evt/issues/156)
* explicit store initialization so DynamoDB writers never need dynamodb:Scan ([#188](https://github.com/photon-grove/evt/issues/188)) ([#193](https://github.com/photon-grove/evt/issues/193)) ([0d57406](https://github.com/photon-grove/evt/commit/0d57406e55ca681043243fddbdf584b684a73d6a))
* **journal:** compaction with a 410 Gone handoff (X2) ([#187](https://github.com/photon-grove/evt/issues/187)) ([cc632b1](https://github.com/photon-grove/evt/commit/cc632b14ab665474a8e88793028e5daebd0817d2))
* **journal:** make the counter put a compare-and-set ([#186](https://github.com/photon-grove/evt/issues/186)) ([4db0deb](https://github.com/photon-grove/evt/commit/4db0deb4971503a4e4dd9456d25c89335200e127))
* **journal:** sync journal with scope allocations, counters and gap-detecting reads (X1) ([#177](https://github.com/photon-grove/evt/issues/177)) ([f368d17](https://github.com/photon-grove/evt/commit/f368d175d12605e3e8a32fc03b36fadeac7fac29))
* **lambda:** photon-evt-lambda crate and release-please package (X3) ([#189](https://github.com/photon-grove/evt/issues/189)) ([8223a15](https://github.com/photon-grove/evt/commit/8223a15cd943ad4eac97aade5017ebc041134637))
* **metadata:** C6 principalId, causationId and extensions (E2.1) ([#138](https://github.com/photon-grove/evt/issues/138)) ([a0960b1](https://github.com/photon-grove/evt/commit/a0960b15bcd03a8d7d616ea3f3de733432022e04))
* **rust:** event-log enumeration streams (P3) ([#147](https://github.com/photon-grove/evt/issues/147)) ([21b1dd2](https://github.com/photon-grove/evt/commit/21b1dd2d01a9c963b196502f3fca93a1e35bc36a))
* **rust:** latest(n), reserved namespaces and the feature marker (C8+C9, E3.2) ([#141](https://github.com/photon-grove/evt/issues/141)) ([4760ccd](https://github.com/photon-grove/evt/commit/4760ccdcd6f09fc730f2e6358f510c1b1fb610ed))
* **rust:** test kit and banking example (P7+P8) ([#148](https://github.com/photon-grove/evt/issues/148)) ([27647a3](https://github.com/photon-grove/evt/commit/27647a3d44acf044fa893a313ab84fd239625760))
* **views:** Rust ViewRepository and ViewCodec (P1, P2, P10) ([#175](https://github.com/photon-grove/evt/issues/175)) ([b696d3d](https://github.com/photon-grove/evt/commit/b696d3d2123a30573fdad2de4c1905b2e875a932))
* **views:** strongly consistent partition view listings ([#182](https://github.com/photon-grove/evt/issues/182)) ([a635746](https://github.com/photon-grove/evt/commit/a635746248e86f8e67d212614eebc93312d4cc51)), closes [#181](https://github.com/photon-grove/evt/issues/181)


### Bug Fixes

* **core:** refuse commits while the rebuild generation is unsettled ([#183](https://github.com/photon-grove/evt/issues/183)) ([dace50d](https://github.com/photon-grove/evt/commit/dace50d13c97f7429664d5c0563991b61eb51f6d))
* **core:** store rejection detail numbers in one canonical text ([#153](https://github.com/photon-grove/evt/issues/153)) ([d767f7a](https://github.com/photon-grove/evt/commit/d767f7ab2a5a8c0e8c73926278e108505ac9cc98))

## 0.1.0 (2026-10-13)


### Features

* **rust:** add photon-evt, a wire-compatible Rust implementation ([#57](https://github.com/photon-grove/evt/issues/57)) ([7b40e4c](https://github.com/photon-grove/evt/commit/7b40e4cf9e32ec717fd2244448c4329acca13e28))

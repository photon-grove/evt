# Changelog

## [0.9.0](https://github.com/photon-grove/evt/compare/v0.8.0...v0.9.0) (2026-10-23)


### Features

* **core:** add latest(n), reserved namespaces, and the feature marker (E2.2) ([#140](https://github.com/photon-grove/evt/issues/140)) ([fa2ef5a](https://github.com/photon-grove/evt/commit/fa2ef5a4a80aed307fe0874561ea8f06ae566238))
* **core:** command outcomes and rejections (C3+C4) ([#149](https://github.com/photon-grove/evt/issues/149)) ([54b1eb8](https://github.com/photon-grove/evt/commit/54b1eb88c0498fcbcf8a4d269f520c26c1df1ea8))
* **core:** commit-wide projectors, any-pk/sk views, per-operation errors (C7) ([#166](https://github.com/photon-grove/evt/issues/166)) ([0e2c796](https://github.com/photon-grove/evt/commit/0e2c796d5e6405e63f014c155a69727d8699b8bf))
* **core:** fence-aware retry (C5) ([#151](https://github.com/photon-grove/evt/issues/151)) ([b6ae1fb](https://github.com/photon-grove/evt/commit/b6ae1fb2fde4113dfdb5d5b374ab2f0255455c53))
* **core:** multi-stream transactions and read fences (C1+C2) ([#145](https://github.com/photon-grove/evt/issues/145)) ([6a7e774](https://github.com/photon-grove/evt/commit/6a7e77483e6d42f41d6886854f18dae6d8bb8c9c))
* **core:** per-stream head item for cheaper commits and read fences (ADR 0007) ([#164](https://github.com/photon-grove/evt/issues/164)) ([37c5d92](https://github.com/photon-grove/evt/commit/37c5d92941a39e49ef09d48be3586599059a9c2c))
* **core:** rebuild fence in Go and Rust (C10) ([#176](https://github.com/photon-grove/evt/issues/176)) ([f5508b8](https://github.com/photon-grove/evt/commit/f5508b8337815d452614f148ede55fdccdcd00c5))
* **core:** rejected outcomes that carry events ([#157](https://github.com/photon-grove/evt/issues/157)) ([bcffcb0](https://github.com/photon-grove/evt/commit/bcffcb0f7fa01bb95eb32296b640066a10eeba27)), closes [#156](https://github.com/photon-grove/evt/issues/156)
* **deprecation:** deprecate the D6-cut surfaces (K1-K7) for v0.9.0 (E2.9) ([#135](https://github.com/photon-grove/evt/issues/135)) ([01cef4c](https://github.com/photon-grove/evt/commit/01cef4cb2378183715a71d918d22923ba8a26a50))
* **doctor:** migrate v0.8 stores to stream heads ([#165](https://github.com/photon-grove/evt/issues/165)) ([160fee8](https://github.com/photon-grove/evt/commit/160fee8b328ab4a5205b620a8a26eeef8b4f6498))
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

## [0.8.0](https://github.com/photon-grove/evt/compare/v0.7.0...v0.8.0) (2026-10-13)


### Features

* **rust:** add photon-evt, a wire-compatible Rust implementation ([#57](https://github.com/photon-grove/evt/issues/57)) ([7b40e4c](https://github.com/photon-grove/evt/commit/7b40e4cf9e32ec717fd2244448c4329acca13e28))

## [0.7.0](https://github.com/photon-grove/evt/compare/v0.6.0...v0.7.0) (2026-06-16)


### Features

* **diagrams:** swimlanes, 100% initial view, and SNS fan-out guidance ([#46](https://github.com/photon-grove/evt/issues/46)) ([a5a02b1](https://github.com/photon-grove/evt/commit/a5a02b1f94e55c93d75799e4baa372570421bb38))


### Bug Fixes

* **diagrams:** denser nodes and a legible initial zoom ([#42](https://github.com/photon-grove/evt/issues/42)) ([1df6aa4](https://github.com/photon-grove/evt/commit/1df6aa471e760ed8180530d27f388ce2c79067ef))

## [0.6.0](https://github.com/photon-grove/evt/compare/v0.5.0...v0.6.0) (2026-06-16)


### Features

* **evt:** backend-neutral storage contracts for a future PostgreSQL backend ([#37](https://github.com/photon-grove/evt/issues/37)) ([27fb74f](https://github.com/photon-grove/evt/commit/27fb74f37bf67e3eb6ece10062cc62f9dd8db455))
* **postgres:** PostgreSQL storage backend ([#40](https://github.com/photon-grove/evt/issues/40)) ([6f4efdb](https://github.com/photon-grove/evt/commit/6f4efdbb020627921ed1e64f79be8bae10dd3e4b))
* **website:** render the docs/ guides in-site with routing ([#35](https://github.com/photon-grove/evt/issues/35)) ([747b912](https://github.com/photon-grove/evt/commit/747b91219323e6e8bebea7d8a14b7bf199b9a389))

## [0.5.0](https://github.com/photon-grove/evt/compare/v0.4.0...v0.5.0) (2026-06-16)


### Features

* **dynamo:** constant-memory enumeration for snapshot-aware rebuilds ([#34](https://github.com/photon-grove/evt/issues/34)) ([3a91cf2](https://github.com/photon-grove/evt/commit/3a91cf220cd75c6f02a358d2cd95f3542c480871))
* **dynamo:** constant-memory streaming enumeration for rebuilds ([#28](https://github.com/photon-grove/evt/issues/28)) ([68dac42](https://github.com/photon-grove/evt/commit/68dac426ef935593d770701f79cea13a813c9ac2))
* **dynamo:** convenience rebuild wrapper wiring HeadSource enumeration ([#33](https://github.com/photon-grove/evt/issues/33)) ([39b01be](https://github.com/photon-grove/evt/commit/39b01bec4a7c4891779dea055c6677824aeb825d))
* **mem:** implement EntityHeadVisitor on the in-memory repository ([#32](https://github.com/photon-grove/evt/issues/32)) ([a76fe38](https://github.com/photon-grove/evt/commit/a76fe3866c9c5aff4c7057fc7238b289ce350d84))

## [0.4.0](https://github.com/photon-grove/evt/compare/v0.3.0...v0.4.0) (2026-06-16)


### Features

* **dynamo:** default HeadStore reads to eventual consistency ([#26](https://github.com/photon-grove/evt/issues/26)) ([38e9306](https://github.com/photon-grove/evt/commit/38e93065e98f7d5ee16d17904e033352a38223a0))
* **dynamo:** entity-heads projector + reader for incremental rebuild change detection ([#22](https://github.com/photon-grove/evt/issues/22)) ([67a271b](https://github.com/photon-grove/evt/commit/67a271b3d73363d4dbba888e7a770c8030eac6d9))

## [0.3.0](https://github.com/photon-grove/evt/compare/v0.2.0...v0.3.0) (2026-06-04)


### Features

* **dynamo:** per-entity-type TTL retention for transient streams ([#19](https://github.com/photon-grove/evt/issues/19)) ([f07b38c](https://github.com/photon-grove/evt/commit/f07b38c76ecd51c726df13b4a50416e0c0aca611))

## [0.2.0](https://github.com/photon-grove/evt/compare/v0.1.0...v0.2.0) (2026-06-04)


### Features

* **compaction:** snapshot-verified event-log compaction + snapshot-aware rebuild ([#14](https://github.com/photon-grove/evt/issues/14)) ([3500bcb](https://github.com/photon-grove/evt/commit/3500bcb356e43c8e871698ce59355815bcecdaf0))
* **dynamo:** correct snapshot/scan correctness + bounded-memory rebuilds ([#13](https://github.com/photon-grove/evt/issues/13)) ([4bb2d2c](https://github.com/photon-grove/evt/commit/4bb2d2c1fdd48fdea7ce6dd70b09d9e71928f4fc))


### Bug Fixes

* **dynamo:** error on non-map marshal; clarify projector DLQ docs; tidy comments ([#17](https://github.com/photon-grove/evt/issues/17)) ([99a6867](https://github.com/photon-grove/evt/commit/99a68675ff4434d6388a4b30705aed9cc5e8a5c7))

## 0.1.0 (2026-06-03)


### Features

* **evt:** bootstrap public event sourcing framework ([48ad53f](https://github.com/photon-grove/evt/commit/48ad53fa7392766dd2dbb6590f0916e4463e76be))
* **website:** add Photon Grove attribution with env-aware homepage link ([#6](https://github.com/photon-grove/evt/issues/6)) ([dfc72e1](https://github.com/photon-grove/evt/commit/dfc72e118e1427cf8bf1fcd32d7542f8937c6f4c))


### Miscellaneous Chores

* release evt 0.1.0 ([#10](https://github.com/photon-grove/evt/issues/10)) ([f8bf7c8](https://github.com/photon-grove/evt/commit/f8bf7c8247e865f4243d8c75f6c654e2d2aa61b4))

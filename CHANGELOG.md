# Changelog

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

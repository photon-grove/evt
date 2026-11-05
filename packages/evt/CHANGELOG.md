# Changelog

## [0.2.1](https://github.com/photon-grove/evt/compare/evt-ts-v0.2.0...evt-ts-v0.2.1) (2026-11-05)


### Features

* **ts:** record command outcome rows on every backend ([#294](https://github.com/photon-grove/evt/issues/294)) ([cce0bc1](https://github.com/photon-grove/evt/commit/cce0bc1d7fd92cb42867bd84652e026b0501b4a7))

## [0.2.0](https://github.com/photon-grove/evt/compare/evt-ts-v0.1.2...evt-ts-v0.2.0) (2026-11-05)


### ⚠ BREAKING CHANGES

* photon_evt::wire::postgres::OutcomeRow has three new public fields and is now #[non_exhaustive]; build one with OutcomeRow::from_outcome. RecordedIdentity is #[non_exhaustive] too; use RecordedIdentity::new.
* **dynamo:** a DynamoDB TransactionConflict is no longer an optimistic conflict. Rust reports Error::Transient (is_conflict() is false), Go's dynamo.ErrTransactionConflict no longer matches evt.ErrOptimisticConflict, and TypeScript throws TransientError. Retry loops keyed on is_retryable, policy.IsTransient or isRetryable still retry it; ones keyed on the conflict checks no longer do.

### Features

* record the principal, command type and targets on command outcomes ([#286](https://github.com/photon-grove/evt/issues/286)) ([8e48d82](https://github.com/photon-grove/evt/commit/8e48d829cd195da3e16de643a0cb8a9eb6d3cd25))
* **ts:** add the Store transaction API and run the multi-stream scenarios ([#293](https://github.com/photon-grove/evt/issues/293)) ([7242144](https://github.com/photon-grove/evt/commit/724214471a077b5b0868e645570b2e7ec46bfa6a))
* **ts:** commit transactions with read fences on every backend ([#291](https://github.com/photon-grove/evt/issues/291)) ([b9e81ef](https://github.com/photon-grove/evt/commit/b9e81efb0257f72cb7f0cce018fc08db1c09ce8f))
* **ts:** join the PostgreSQL cross-language run ([#285](https://github.com/photon-grove/evt/issues/285)) ([b5f15a1](https://github.com/photon-grove/evt/commit/b5f15a164440f9712838f039e152ee4577e6ffe7))


### Bug Fixes

* **dynamo:** classify a TransactionConflict as transient, never a stale fence ([#288](https://github.com/photon-grove/evt/issues/288)) ([e49906e](https://github.com/photon-grove/evt/commit/e49906ead90b1c5e1813a2324a46f6b1eccc996b))

## [0.1.2](https://github.com/photon-grove/evt/compare/evt-ts-v0.1.1...evt-ts-v0.1.2) (2026-11-04)


### Features

* **ts:** add PgRepository ([#280](https://github.com/photon-grove/evt/issues/280)) ([daebc61](https://github.com/photon-grove/evt/commit/daebc61bbeb6690c8267a7f75a49f538a0b5f439))
* **ts:** add the per-entity view projector ([#276](https://github.com/photon-grove/evt/issues/276)) ([e4c7747](https://github.com/photon-grove/evt/commit/e4c77475b4bb71d2828dad69cb532a6ebe85b925))
* **ts:** add the PostgreSQL schema layer ([#279](https://github.com/photon-grove/evt/issues/279)) ([a4db99f](https://github.com/photon-grove/evt/commit/a4db99f1374747514c9696f69866f12b6ee992f6))


### Performance Improvements

* **ts:** read a retained stream's remaining items once per failed event put ([#275](https://github.com/photon-grove/evt/issues/275)) ([1c470a2](https://github.com/photon-grove/evt/commit/1c470a2c18b63067e0d8ec067dae577277054b4f))

## [0.1.1](https://github.com/photon-grove/evt/compare/evt-ts-v0.1.0...evt-ts-v0.1.1) (2026-11-04)


### Features

* **ts:** add DynamoRepository to @photon-grove/evt/dynamodb ([#267](https://github.com/photon-grove/evt/issues/267)) ([e042fe5](https://github.com/photon-grove/evt/commit/e042fe551ee1d23c3c3a6a3f3ea3e04a6e5140a2))
* **ts:** add retention TTL to DynamoRepository ([#269](https://github.com/photon-grove/evt/issues/269)) ([964723c](https://github.com/photon-grove/evt/commit/964723c324a4973bc971604d3ced1345385d5b72))
* **ts:** validate the feature marker and check the rebuild lock in MemRepository ([#265](https://github.com/photon-grove/evt/issues/265)) ([315387a](https://github.com/photon-grove/evt/commit/315387a563fd3bab0871c99316d83a5c3d32a63a))

## 0.1.0 (2026-11-02)


### Features

* **ts:** add aggregates, upcasting and the Store ([#252](https://github.com/photon-grove/evt/issues/252)) ([f8a44db](https://github.com/photon-grove/evt/commit/f8a44dbf2506753d508982c1ac333c32838bac27))
* **ts:** add the given/when/then helpers in @photon-grove/evt/testing ([#253](https://github.com/photon-grove/evt/issues/253)) ([3a9211e](https://github.com/photon-grove/evt/commit/3a9211e938f9e5b9629289eec3e31db417158878))
* **ts:** add the repository contract and MemRepository ([#250](https://github.com/photon-grove/evt/issues/250)) ([2c07978](https://github.com/photon-grove/evt/commit/2c0797848b605768cc2ffb76b530f34857b36f05))
* **ts:** add the wire layer and check it against codec.json ([#248](https://github.com/photon-grove/evt/issues/248)) ([67c3711](https://github.com/photon-grove/evt/commit/67c3711faa09afb1358403fdfb67c0a7d5a32636))
* **ts:** prepare @photon-grove/evt for its first npm release ([#258](https://github.com/photon-grove/evt/issues/258)) ([46a929c](https://github.com/photon-grove/evt/commit/46a929c0888c962348904bb99c3e8eedf504cc9e))

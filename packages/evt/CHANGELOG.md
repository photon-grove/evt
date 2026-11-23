# Changelog

## [0.2.3](https://github.com/photon-grove/evt/compare/evt-ts-v0.2.2...evt-ts-v0.2.3) (2026-11-23)


### Features

* **ts:** read commit-wide view rows through the repository that wrote them ([#314](https://github.com/photon-grove/evt/issues/314)) ([58a5557](https://github.com/photon-grove/evt/commit/58a55573649dc7c545a8736eed8e0796cd4f2621))
* **ts:** rebuild the journal under the rebuild lock ([#332](https://github.com/photon-grove/evt/issues/332)) ([65944be](https://github.com/photon-grove/evt/commit/65944be83faae0f290930e418037560c4c3249d9))

## [0.2.2](https://github.com/photon-grove/evt/compare/evt-ts-v0.2.1...evt-ts-v0.2.2) (2026-11-23)


### Features

* **ts:** add the engine's expected-payload view condition ([#319](https://github.com/photon-grove/evt/issues/319)) ([021a2d3](https://github.com/photon-grove/evt/commit/021a2d31b454845eb9b80443c58ef8dada4bbfe8))
* **ts:** add the view repository and the typed view codec ([#309](https://github.com/photon-grove/evt/issues/309)) ([0cd8733](https://github.com/photon-grove/evt/commit/0cd873311290603cc9e789e49f6663b9921afbf0))
* **ts:** apply the expected-payload condition on the PostgreSQL views table ([#322](https://github.com/photon-grove/evt/issues/322)) ([6c39dc7](https://github.com/photon-grove/evt/commit/6c39dc7084880aebe4e7e5d42ba78c583ff5876c))
* **ts:** declare history-dependent types and run the feature-marker scenario ([#317](https://github.com/photon-grove/evt/issues/317)) ([444e9d0](https://github.com/photon-grove/evt/commit/444e9d09e8779930341b38d989158d1a4a045e1e))
* **ts:** enumerate the event log, report entity heads, seed from snapshots ([#321](https://github.com/photon-grove/evt/issues/321)) ([c74869a](https://github.com/photon-grove/evt/commit/c74869a73106a4a8f3e5d4f05db0c1e8f839e0b9))
* **ts:** journal compaction on the memory and DynamoDB repositories ([#324](https://github.com/photon-grove/evt/issues/324)) ([e913199](https://github.com/photon-grove/evt/commit/e91319991ac26be5575aff38befb18c237e53fe2))
* **ts:** journal compaction on the PostgreSQL views table ([#330](https://github.com/photon-grove/evt/issues/330)) ([553d3ff](https://github.com/photon-grove/evt/commit/553d3ffbbaead51d0c745f3561866394c6a2f317))
* **ts:** journal on the memory and DynamoDB repositories ([#323](https://github.com/photon-grove/evt/issues/323)) ([558b7db](https://github.com/photon-grove/evt/commit/558b7db05463ee8c24f9d19260b1ed5b3fa7ee7b))
* **ts:** journal on the PostgreSQL views table ([#326](https://github.com/photon-grove/evt/issues/326)) ([44576de](https://github.com/photon-grove/evt/commit/44576dedc514e700908e40355df9d9bf921d0fa7))
* **ts:** port photon-evt-lambda as @photon-grove/evt-lambda ([#310](https://github.com/photon-grove/evt/issues/310)) ([e010e7a](https://github.com/photon-grove/evt/commit/e010e7aaa0371057d2d7ce72258703afeff17c3e))
* **ts:** rebuild commit-wide projectors from the log behind the rebuild fence ([#327](https://github.com/photon-grove/evt/issues/327)) ([cca4289](https://github.com/photon-grove/evt/commit/cca428990269e34d3e1b679316ce5444dff03f58))
* **ts:** return commits' events as stored through commit receipts ([#318](https://github.com/photon-grove/evt/issues/318)) ([bffebb1](https://github.com/photon-grove/evt/commit/bffebb11419ab029854c79bb88f8b8fa287f29f9))
* **ts:** run the rebuild fence's lifecycle on every backend ([#325](https://github.com/photon-grove/evt/issues/325)) ([19add0f](https://github.com/photon-grove/evt/commit/19add0fa1ad2d57b84782dad0e000dd9637c32c2))
* **ts:** wake PostgreSQL journal listeners through NOTIFY ([#328](https://github.com/photon-grove/evt/issues/328)) ([73780fa](https://github.com/photon-grove/evt/commit/73780fa26d887447db16a0b4e7266147961d342c))
* **ts:** write commit-wide views on PostgreSQL through a view writer ([#312](https://github.com/photon-grove/evt/issues/312)) ([d57275e](https://github.com/photon-grove/evt/commit/d57275e0d7620e289e49e7ab9657cb3f8adb3ed5))
* **ts:** write commit-wide views to the PostgreSQL views table ([#320](https://github.com/photon-grove/evt/issues/320)) ([00adc1f](https://github.com/photon-grove/evt/commit/00adc1fa65eb2544af192fe1bb9cddf1f1c21d84))


### Bug Fixes

* **ts:** poll briefly after a TransactionConflict on the outcome put ([#329](https://github.com/photon-grove/evt/issues/329)) ([827dbd5](https://github.com/photon-grove/evt/commit/827dbd565bbb6d6159b680588b71f465167a33bd))

## [0.2.1](https://github.com/photon-grove/evt/compare/evt-ts-v0.2.0...evt-ts-v0.2.1) (2026-11-21)


### Features

* **ts:** add commit-wide projectors in memory and on DynamoDB ([#305](https://github.com/photon-grove/evt/issues/305)) ([2cf3b1e](https://github.com/photon-grove/evt/commit/2cf3b1e33c0ba4a7e0ee2c2f58ec989b2efe3e13))
* **ts:** record command outcome rows on every backend ([#294](https://github.com/photon-grove/evt/issues/294)) ([cce0bc1](https://github.com/photon-grove/evt/commit/cce0bc1d7fd92cb42867bd84652e026b0501b4a7))
* **ts:** run commands with durable outcomes, replay and recovery ([#296](https://github.com/photon-grove/evt/issues/296)) ([6513801](https://github.com/photon-grove/evt/commit/6513801e10e42b5e5833a0ce601eab2b77e3c9ed))
* **ts:** run multi-stream commands under fence-aware retry ([#302](https://github.com/photon-grove/evt/issues/302)) ([e0ce0aa](https://github.com/photon-grove/evt/commit/e0ce0aac458bc815ddefb187a56a28787aef5328))

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

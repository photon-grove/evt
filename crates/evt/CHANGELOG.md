# Changelog

## [0.4.0](https://github.com/photon-grove/evt/compare/photon-evt-v0.3.0...photon-evt-v0.4.0) (2026-11-02)


### ⚠ BREAKING CHANGES

* **journal:** a repository handle not wrapped by JournalRepository is refused on a journal table, even in a build with the journal feature. JournalRepository::new now requires R: FeatureMarker, and a decorator placed under the journal must forward accept_journal to its inner repository.

### Bug Fixes

* **journal:** accept the journal marker per repository handle ([#257](https://github.com/photon-grove/evt/issues/257)) ([bda29fe](https://github.com/photon-grove/evt/commit/bda29fe53931cbb987f184a87a928ed8704e2797))
* **journal:** check a compaction checkpoint before declaring the marker or taking the lease ([#255](https://github.com/photon-grove/evt/issues/255)) ([8b35bd7](https://github.com/photon-grove/evt/commit/8b35bd75fbfe506c025f665d489809192adbe9d3))
* **projectors:** refuse application writes to engine-reserved journal partitions ([#256](https://github.com/photon-grove/evt/issues/256)) ([e952288](https://github.com/photon-grove/evt/commit/e95228877506c5b266e5390dd982f2b136f5fcab))
* **rebuild:** report a begin whose result is unknown as RebuildBeginIndeterminate ([#251](https://github.com/photon-grove/evt/issues/251)) ([3d314cd](https://github.com/photon-grove/evt/commit/3d314cd1a4a675e1588b0df170b3774a9a614396))
* **views:** refuse U+0000 in view rows on every backend ([#254](https://github.com/photon-grove/evt/issues/254)) ([faa9fbd](https://github.com/photon-grove/evt/commit/faa9fbdc1613ff1b4cdd699974d8fab805a6301e))

## [0.3.0](https://github.com/photon-grove/evt/compare/photon-evt-v0.2.1...photon-evt-v0.3.0) (2026-10-30)


### ⚠ BREAKING CHANGES

* **rust:** an implementation of CommitProjector that overrides engine_metadata no longer compiles. Remove the override: only the journal's projector reads engine metadata, and returning true never gave a consistent view.
* **rust:** `PgStatement::params` is now `Vec<Arc<dyn ToSql + Sync + Send>>`, so `PgStatement` is `Clone`, which a journaled repository needs to stage a commit again. Build statements with

### Features

* **rust:** add the evt-owned PostgreSQL views table and PgViewRepository ([#236](https://github.com/photon-grove/evt/issues/236)) ([6d88825](https://github.com/photon-grove/evt/commit/6d88825351e27f5538d744040231bfe8b2413468))
* **rust:** diff-mode projection rebuild that writes only changes ([#234](https://github.com/photon-grove/evt/issues/234)) ([2815ebc](https://github.com/photon-grove/evt/commit/2815ebc65cb51e71ca991da41a2e58b7e1d4f350))
* **rust:** look up a command's outcome by CommandIdentity ([#233](https://github.com/photon-grove/evt/issues/233)) ([6b3554e](https://github.com/photon-grove/evt/commit/6b3554ed595965b4dae3564bd15757946f3d95f4))
* **rust:** return committed events, journal allocations included, from a commit ([#231](https://github.com/photon-grove/evt/issues/231)) ([5db0774](https://github.com/photon-grove/evt/commit/5db0774f5dd00cfdb5fd7e2f847250b03e13603b))
* **rust:** run the sync journal and its compaction on PostgreSQL ([#239](https://github.com/photon-grove/evt/issues/239)) ([29ad621](https://github.com/photon-grove/evt/commit/29ad6212d4f88d2de3f6f1f7a7a5a0758c69247f))
* **rust:** wake journal consumers on PostgreSQL with NOTIFY ([#241](https://github.com/photon-grove/evt/issues/241)) ([86c8001](https://github.com/photon-grove/evt/commit/86c8001d57cee3d41e2c65f55054960c2c944d9f))
* **rust:** write commit-wide views to the PostgreSQL views table ([#238](https://github.com/photon-grove/evt/issues/238)) ([a623bba](https://github.com/photon-grove/evt/commit/a623bba1bd221861609ed442111ca00a84592d42))


### Bug Fixes

* **rust:** seal CommitProjector::engine_metadata to the engine's own projectors ([#230](https://github.com/photon-grove/evt/issues/230)) ([edaa2f7](https://github.com/photon-grove/evt/commit/edaa2f76e1eaab01f271e4f46284a3085a22be50)), closes [#179](https://github.com/photon-grove/evt/issues/179)
* skip a snapshot over the per-item budget instead of failing the commit ([#226](https://github.com/photon-grove/evt/issues/226)) ([12fd9b0](https://github.com/photon-grove/evt/commit/12fd9b01688e0538fccc03dd47688648e9bd1d3e)), closes [#155](https://github.com/photon-grove/evt/issues/155)

## [0.2.1](https://github.com/photon-grove/evt/compare/photon-evt-v0.2.0...photon-evt-v0.2.1) (2026-10-30)


### Features

* **rust:** full projection rebuild under the rebuild fence, seeded from snapshots ([#213](https://github.com/photon-grove/evt/issues/213)) ([083786e](https://github.com/photon-grove/evt/commit/083786e1658a32b98bbfb9ce149d5a5340fedc43))
* **rust:** per-aggregate snapshot interval, catch-up snapshots and load tracing ([#208](https://github.com/photon-grove/evt/issues/208)) ([492f1e7](https://github.com/photon-grove/evt/commit/492f1e7ae26f47ffbf93c6ee0a2c709796fc0ebb))
* **rust:** rebuild views and the sync journal under one rebuild fence ([#228](https://github.com/photon-grove/evt/issues/228)) ([70b6e58](https://github.com/photon-grove/evt/commit/70b6e582aefe083acb851581b5a99f56ab7f6263))
* **rust:** renew the lease during a journal rebuild ([#227](https://github.com/photon-grove/evt/issues/227)) ([517c2a8](https://github.com/photon-grove/evt/commit/517c2a814f89b026422be5a53266a3defe6b8191))


### Bug Fixes

* **postgres:** run engine transactions at READ COMMITTED whatever the server default ([#229](https://github.com/photon-grove/evt/issues/229)) ([27a30e0](https://github.com/photon-grove/evt/commit/27a30e0431a452c6aa41b2314d1705c929e53b20)), closes [#223](https://github.com/photon-grove/evt/issues/223)
* **rust:** keep the read deadline from cutting a held commit's landing short ([#217](https://github.com/photon-grove/evt/issues/217)) ([9fb73e4](https://github.com/photon-grove/evt/commit/9fb73e41d91460de13115f8ef7cc5d96c35c0057)), closes [#216](https://github.com/photon-grove/evt/issues/216)

## [0.2.0](https://github.com/photon-grove/evt/compare/photon-evt-v0.1.0...photon-evt-v0.2.0) (2026-10-26)


### ⚠ BREAKING CHANGES

* Repository no longer has CommitStream. Commit batched commands through Repository.Commit.
* **go:** Repository no longer has CommitStream. Commit batched commands through Repository.Commit.
* **go:** Repository no longer has CommitStream. Commit batched commands through Repository.Commit.
* Repository no longer has CommitStream. Commit batched commands through Repository.Commit.

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


### Documentation

* reorganize and rewrite the docs for humans ([#198](https://github.com/photon-grove/evt/issues/198)) ([f413eb1](https://github.com/photon-grove/evt/commit/f413eb14a6e692d05b4ae34dbf1f41d4aaa43e58))


### Code Refactoring

* delete the K1–K7 cut features ([#195](https://github.com/photon-grove/evt/issues/195)) ([6e3ae2e](https://github.com/photon-grove/evt/commit/6e3ae2e2d1a93a559cedb75539f68a169c837e5b))
* **go:** move the Go module into go/ ([#196](https://github.com/photon-grove/evt/issues/196)) ([035293c](https://github.com/photon-grove/evt/commit/035293c74c85f3402954d854e889be0c1c97d14f))
* **go:** rename Entity to Aggregate and Context to CommandContext ([#197](https://github.com/photon-grove/evt/issues/197)) ([d11b22e](https://github.com/photon-grove/evt/commit/d11b22ed5b306c15d642144eb15d01bdb591d2b4))

## 0.1.0 (2026-10-13)


### Features

* **rust:** add photon-evt, a wire-compatible Rust implementation ([#57](https://github.com/photon-grove/evt/issues/57)) ([7b40e4c](https://github.com/photon-grove/evt/commit/7b40e4cf9e32ec717fd2244448c4329acca13e28))

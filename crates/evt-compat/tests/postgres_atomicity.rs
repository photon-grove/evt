//! Live PostgreSQL parity checks; run with `--features drivers --test postgres_atomicity`.
#![cfg(feature = "drivers")]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use evt_compat::drivers;
use photon_evt::postgres::PostgresRepository;
use photon_evt::wire::postgres::{
    FEATURES_LOCK_EXCLUSIVE, features_lock_key, features_table, schema_ddl,
};
use photon_evt::{
    Commit, EntityId, Error, EventId, EventSequence, EventType, EventVersion, Metadata, Repository,
    SerializedEvent, SerializedSnapshot,
};
use serde_json::Value;

fn unique() -> String {
    format!(
        "pa{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

async fn repository() -> (PostgresRepository, String, String) {
    let events = unique();
    let snapshots = format!("{events}_s");
    let repo = PostgresRepository::new(drivers::postgres_pool().unwrap())
        .with_tables(&events, &snapshots)
        .unwrap();
    repo.ensure_schema().await.unwrap();
    (repo, events, snapshots)
}

fn snapshot(id: &EntityId) -> SerializedSnapshot {
    SerializedSnapshot {
        entity_type: "account".into(),
        entity_id: id.clone(),
        sequence: EventSequence(1),
        event_sequence: EventSequence(1),
        payload: b"{}".to_vec(),
    }
}

async fn seed(repo: &PostgresRepository, id: &EntityId) {
    let event = SerializedEvent {
        id: EventId::for_event(id, EventSequence(1)),
        sequence: EventSequence(1),
        event_type: EventType::new("account:opened"),
        version: EventVersion(1),
        entity_id: id.clone(),
        entity_type: "account".into(),
        payload: b"{}".to_vec(),
        metadata: Metadata::new("local"),
    };
    repo.commit(Commit::new(id.clone(), vec![event]))
        .await
        .unwrap();
    assert!(repo.put_snapshot(&snapshot(id)).await.unwrap());
}

async fn unsupported(repo: &PostgresRepository) {
    repo.update_manifest(|m| {
        m.features.insert("future".into(), 1);
    })
    .await
    .unwrap();
}

/// Wait for a real database wait edge, not a scheduler delay. The repository's connection must
/// have reached the statement that blocks before the test proceeds.
async fn wait_for(repo: &PostgresRepository, query: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let client = repo.pool().get().await.unwrap();
        loop {
            if client
                .query_one(query, &[])
                .await
                .unwrap()
                .get::<_, bool>(0)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("operation reached its database lock wait");
}

#[tokio::test]
async fn declaration_cannot_complete_ahead_of_a_paused_compactors_delete() {
    let (repo, events, snapshots) = repository().await;
    let id = EntityId::new("account-1");
    seed(&repo, &id).await;
    let mut holder = repo.pool().get().await.unwrap();
    let pause = holder.transaction().await.unwrap();
    pause
        .batch_execute(&format!("LOCK TABLE {snapshots} IN ACCESS EXCLUSIVE MODE"))
        .await
        .unwrap();

    let compact = tokio::spawn({
        let repo = repo.clone();
        let id = id.clone();
        async move { repo.compact_below(&id, EventSequence(1)).await }
    });
    wait_for(&repo, &format!(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = '{snapshots}'::regclass AND NOT granted)"
    )).await;

    let declare = tokio::spawn({
        let repo = repo.clone();
        async move {
            repo.update_manifest(|m| {
                m.history_dependent.insert("account".into());
            })
            .await
        }
    });
    wait_for(
        &repo,
        &format!(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND classid = 1702261761
         AND objid = hashtext('{}')::oid AND NOT granted)", features_lock_key(&events)
    ),
    )
    .await;
    assert!(
        !declare.is_finished(),
        "declaration waits until the compactor commits"
    );
    pause.rollback().await.unwrap();
    assert_eq!(compact.await.unwrap().unwrap(), 1);
    declare.await.unwrap().unwrap();
    assert!(matches!(
        repo.compact_below(&id, EventSequence(1)).await,
        Err(Error::HistoryDependentProjection { .. })
    ));
}

#[tokio::test]
async fn mixed_case_handles_share_feature_locks_and_refuse_after_declaration() {
    let (repo, events, snapshots) = repository().await;
    let id = EntityId::new("account-1");
    seed(&repo, &id).await;
    let mixed = PostgresRepository::new(repo.pool().clone())
        .with_tables(&events.to_uppercase(), &snapshots.to_uppercase())
        .unwrap();
    assert_eq!(
        features_lock_key(&events),
        features_lock_key(&events.to_uppercase())
    );
    let mut holder = repo.pool().get().await.unwrap();
    let declaration = holder.transaction().await.unwrap();
    declaration
        .execute(FEATURES_LOCK_EXCLUSIVE, &[&features_lock_key(&events)])
        .await
        .unwrap();
    let put = tokio::spawn({
        let mixed = mixed.clone();
        let id = id.clone();
        async move { mixed.put_snapshot(&snapshot(&id)).await }
    });
    let compact = tokio::spawn({
        let mixed = mixed.clone();
        let id = id.clone();
        async move { mixed.compact_below(&id, EventSequence(1)).await }
    });
    wait_for(
        &repo,
        &format!(
            "SELECT count(*) = 2 FROM pg_locks WHERE locktype = 'advisory' AND classid = 1702261761
         AND objid = hashtext('{}')::oid AND NOT granted",
            features_lock_key(&events)
        ),
    )
    .await;
    declaration
        .execute(
            &format!(
                "INSERT INTO {} (id, version, features) VALUES (0, 1, '{{\"future\":1}}')",
                features_table(&events)
            ),
            &[],
        )
        .await
        .unwrap();
    declaration.commit().await.unwrap();
    assert!(matches!(
        put.await.unwrap(),
        Err(Error::UnsupportedFeature { .. })
    ));
    assert!(matches!(
        compact.await.unwrap(),
        Err(Error::UnsupportedFeature { .. })
    ));
    assert_eq!(repo.events(&id).await.unwrap().len(), 1);
    assert!(matches!(
        repo.put_snapshot(&snapshot(&id)).await,
        Err(Error::UnsupportedFeature { .. })
    ));
}

#[tokio::test]
async fn marker_cache_is_invalidated_by_updates_and_log_reconfiguration() {
    let (repo, _, _) = repository().await;
    let id = EntityId::new("account-1");
    repo.update_manifest(|_| {}).await.unwrap();
    assert!(repo.put_snapshot(&snapshot(&id)).await.unwrap()); // cache version 1
    let (other, events, snapshots) = repository().await;
    unsupported(&other).await; // also version 1, but a different log
    let moved = repo.with_tables(&events, &snapshots).unwrap();
    assert!(matches!(
        moved.put_snapshot(&snapshot(&id)).await,
        Err(Error::UnsupportedFeature { .. })
    ));
    other.update_manifest(|m| m.features.clear()).await.unwrap();
    assert!(other.put_snapshot(&snapshot(&id)).await.unwrap());
    unsupported(&other).await;
    assert!(matches!(
        other.put_snapshot(&snapshot(&id)).await,
        Err(Error::UnsupportedFeature { .. })
    ));
    assert!(matches!(
        other
            .compact_below(&EntityId::new("SYSTEM#FEATURES"), EventSequence(0))
            .await,
        Err(Error::ReservedEntityId { .. })
    ));
}

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../compat/fixtures/feature-marker.json")).unwrap()
}

#[tokio::test]
async fn declaration_rows_match_go_canonical_bytes() {
    let (repo, events, _) = repository().await;
    for case in fixture()["declarations"].as_array().unwrap() {
        let input = &case["input"];
        repo.update_manifest(|m| {
            m.features = serde_json::from_value(input["features"].clone()).unwrap();
            m.history_dependent =
                serde_json::from_value(input["historyDependent"].clone()).unwrap();
            m.retention = serde_json::from_value(input["retention"].clone()).unwrap();
        })
        .await
        .unwrap();
        let row = repo.pool().get().await.unwrap().query_one(&format!(
            "SELECT id, version, features, history_dependent, retention FROM {} WHERE id = 0",
            features_table(&events)
        ), &[]).await.unwrap();
        let want = &case["output"]["postgresRow"];
        assert_eq!(
            i64::from(row.get::<_, i32>(0)),
            want["id"].as_i64().unwrap()
        );
        assert_eq!(row.get::<_, i64>(1), want["version"].as_i64().unwrap());
        for (index, name) in [(2, "features"), (3, "history_dependent"), (4, "retention")] {
            assert_eq!(
                row.get::<_, &str>(index),
                want[name].as_str().unwrap(),
                "{}: {name}",
                case["name"]
            );
        }
    }
}

#[tokio::test]
async fn go_and_rust_ddl_create_and_upgrade_in_either_order() {
    let pool = drivers::postgres_pool().unwrap();
    let mut client = pool.get().await.unwrap();
    for vector in fixture()["postgresSchemas"].as_array().unwrap() {
        let events = vector["eventsTable"].as_str().unwrap();
        let snapshots = vector["snapshotsTable"].as_str().unwrap();
        let go = vector["ddl"].as_str().unwrap();
        let rust = schema_ddl(events, snapshots);
        // The old index name can also belong to an unrelated table. Neither implementation
        // may drop it merely because its spelling looks like a legacy engine index.
        if snapshots != format!("{events}_entity_type_idx") {
            for ddl in [go, rust.as_str()] {
                let tx = client.transaction().await.unwrap();
                let schema = unique();
                tx.batch_execute(&format!(
                    "CREATE SCHEMA {schema}; SET LOCAL search_path TO {schema};
                     CREATE TABLE unrelated (entity_type text);
                     CREATE INDEX {events}_entity_type_idx ON unrelated (entity_type)"
                ))
                .await
                .unwrap();
                tx.batch_execute(ddl).await.unwrap();
                let row = tx
                    .query_one(
                        "SELECT EXISTS (SELECT 1 FROM pg_index WHERE indexrelid = to_regclass($1)
                     AND indrelid = 'unrelated'::regclass)",
                        &[&format!("{events}_entity_type_idx")],
                    )
                    .await
                    .unwrap();
                assert!(row.get::<_, bool>(0));
                tx.rollback().await.unwrap();
            }
        }
        for legacy in [false, true] {
            for go_first in [false, true] {
                // Isolated transactional schemas avoid collisions with other integration suites.
                let tx = client.transaction().await.unwrap();
                let schema = unique();
                tx.batch_execute(&format!(
                    "CREATE SCHEMA {schema}; SET LOCAL search_path TO {schema}"
                ))
                .await
                .unwrap();
                if legacy {
                    // Install precisely the old events table/index, before either language upgrades.
                    let events_ddl = rust.split("CREATE INDEX").next().unwrap();
                    tx.batch_execute(events_ddl).await.unwrap();
                    tx.batch_execute(&format!(
                        "CREATE INDEX {events}_entity_type_idx ON {events} (entity_type)"
                    ))
                    .await
                    .unwrap();
                }
                let order = if go_first {
                    [go, rust.as_str()]
                } else {
                    [rust.as_str(), go]
                };
                for ddl in order.into_iter().cycle().take(4) {
                    tx.batch_execute(ddl).await.unwrap();
                }
                let index = tx.query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_index WHERE indexrelid = to_regclass($1) AND indrelid = to_regclass($2))",
                    &[&format!("{events}__evt_entity_type_idx"), &events]
                ).await.unwrap();
                assert!(index.get::<_, bool>(0));
                // A legal snapshots table named like the old index must survive repeated DDL.
                tx.execute(&format!("INSERT INTO {snapshots} (entity_id, entity_type, sequence, event_sequence, payload)
                    VALUES ('account-1', 'account', 1, 1, ''::bytea)"), &[]).await.unwrap();
                tx.rollback().await.unwrap();
            }
        }
    }
}

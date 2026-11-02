//! The sync journal (ADR 0005, X1) and its compaction (X2) against the local DynamoDB emulator:
//! the shared conformance cases over private tables, the stored items, the feature marker, and a
//! store end to end. Needs the `drivers` feature and `AWS_ENDPOINT_URL` pointing at the emulator:
//!
//! ```sh
//! cargo test -p evt-compat --features drivers --test journal
//! ```

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};
use evt_compat::drivers;
use photon_evt::conformance::{
    Add, Ledger, TestClock, compaction_cases, journal_cases, journal_rebuild_cases,
};
use photon_evt::dynamodb::{DynamoRepository, DynamoViewRepository};
use photon_evt::journal::{
    Journal, JournalRepository, JournalStore, Scope, allocations, high_watermark, page,
};
use photon_evt::{Error, Metadata, OutcomeConfig, Repository, RetryPolicy, SerializedEvent, Store};

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

async fn create_table(
    client: &aws_sdk_dynamodb::Client,
    name: &str,
    sort_key: ScalarAttributeType,
) {
    let attribute = |name: &str, kind| {
        AttributeDefinition::builder()
            .attribute_name(name)
            .attribute_type(kind)
            .build()
            .unwrap()
    };
    let key = |name: &str, key_type| {
        KeySchemaElement::builder()
            .attribute_name(name)
            .key_type(key_type)
            .build()
            .unwrap()
    };

    client
        .create_table()
        .table_name(name)
        .attribute_definitions(attribute("pk", ScalarAttributeType::S))
        .attribute_definitions(attribute("sk", sort_key))
        .key_schema(key("pk", KeyType::Hash))
        .key_schema(key("sk", KeyType::Range))
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .expect("create the table");
}

/// A private event table and views table, deleted by [`Tables::drop`]. Private tables keep the
/// emulator's transaction rollback copies small and give each suite a clean feature marker.
struct Tables {
    client: aws_sdk_dynamodb::Client,
    events: String,
    views: String,
}

impl Tables {
    async fn new(label: &str) -> Self {
        let client = drivers::dynamo_client().await;
        let (events, views) = (
            unique(&format!("evt-journal-{label}-events")),
            unique(&format!("evt-journal-{label}-views")),
        );

        create_table(&client, &events, ScalarAttributeType::N).await;
        create_table(&client, &views, ScalarAttributeType::S).await;

        Self {
            client,
            events,
            views,
        }
    }

    fn repository(&self) -> DynamoRepository {
        DynamoRepository::new(self.client.clone(), &self.events)
            .with_views_table(&self.views)
            .with_lazy_initialization(true)
    }

    fn view_repository(&self) -> DynamoViewRepository {
        DynamoViewRepository::new(self.client.clone(), &self.views)
    }

    async fn drop(self) {
        for table in [&self.events, &self.views] {
            self.client
                .delete_table()
                .table_name(table)
                .send()
                .await
                .expect("delete the table");
        }
    }
}

fn scope(name: &str) -> Scope {
    Scope::new(name).unwrap()
}

// One test, run in sequence: the emulator rolls back a failed transaction by restoring a copy of
// every table it touched, which drops writes a concurrent test made in the meantime.
#[tokio::test]
async fn journal_conforms_and_works_end_to_end() {
    tokio::time::timeout(Duration::from_secs(300), async {
        let tables = Tables::new("conf").await;
        let repository = tables.repository();

        journal_cases(&repository, &tables.view_repository()).await;
        journal_rebuild_cases(&repository, &tables.view_repository()).await;
        tables.drop().await;

        // Compaction (X2) on tables of its own, with a clock its lease cases move.
        let tables = Tables::new("cmp").await;
        let clock = TestClock::starting_now();
        let repository = tables.repository().with_clock(clock.reader());

        compaction_cases(&repository, &tables.view_repository(), &clock).await;
        tables.drop().await;

        Box::pin(stored_items_and_marker()).await;
        Box::pin(store_end_to_end()).await;
        Box::pin(without_a_views_table()).await;
    })
    .await
    .expect("the journal run exceeded its time budget");
}

/// The rows, counters, and marker as DynamoDB stores them.
async fn stored_items_and_marker() {
    let tables = Tables::new("items").await;
    let repository = tables.repository();
    let journal = Journal::new(["ledger"], |event: &SerializedEvent| {
        vec![scope(&format!("ledger:{}", event.entity_id))]
    })
    .unwrap();
    let journaled = JournalRepository::new(repository.clone(), journal);
    let store = Store::<Ledger, _>::new(&journaled, ()).with_snapshot_interval(0);

    store
        .execute("acct-1", Add(5), Metadata::new("local"))
        .await
        .unwrap();
    store
        .execute("acct-1", Add(7), Metadata::new("local"))
        .await
        .unwrap();

    let get = |pk: String, sk: String| {
        let (client, table) = (tables.client.clone(), tables.views.clone());

        async move {
            client
                .get_item()
                .table_name(table)
                .consistent_read(true)
                .key("pk", aws_sdk_dynamodb::types::AttributeValue::S(pk))
                .key("sk", aws_sdk_dynamodb::types::AttributeValue::S(sk))
                .send()
                .await
                .unwrap()
                .item
                .expect("the item exists")
        }
    };
    let text =
        |item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
         name: &str|
         -> String { item[name].as_s().unwrap().clone() };

    // A journal row: the five view attributes, at the 20-digit sort key, owned by the stream.
    let row = get(
        "journal#ledger:acct-1".into(),
        "00000000000000000002".into(),
    )
    .await;
    let mut names: Vec<_> = row.keys().map(String::as_str).collect();
    names.sort_unstable();
    assert_eq!(names, ["entityID", "entityType", "payload", "pk", "sk"]);
    assert_eq!(text(&row, "entityID"), "acct-1");
    assert_eq!(text(&row, "entityType"), "journal");
    assert!(
        text(&row, "payload")
            .starts_with(r#"{"scope":"ledger:acct-1","position":2,"event":{"id":"acct-1:2""#),
        "{}",
        text(&row, "payload")
    );

    // The counter, owned by the engine.
    let counter = get("journal-counter#ledger:acct-1".into(), "COUNTER".into()).await;
    assert_eq!(text(&counter, "entityID"), "SYSTEM#JOURNAL");
    assert_eq!(text(&counter, "entityType"), "journal-counter");
    assert_eq!(
        text(&counter, "payload"),
        r#"{"scope":"ledger:acct-1","position":2}"#
    );

    // The stored event carries its allocation in metadata.
    let events = repository.events(&"acct-1".into()).await.unwrap();
    let stored = allocations(&events[1].metadata).unwrap();
    assert_eq!(
        (stored[0].scope.as_str(), stored[0].position),
        ("ledger:acct-1", 2)
    );

    // The marker: the journal at format version 1 beside stream-heads, and the history set.
    let manifest = repository.read_manifest().await.unwrap().unwrap();
    assert_eq!(
        manifest.features_json(),
        r#"{"journal":1,"stream-heads":1}"#
    );
    assert!(manifest.is_history_dependent(&"ledger".into()));

    // A newer journal format is refused by this build, even by the journal's own writer.
    repository
        .update_manifest(|manifest| {
            manifest.features.insert("journal".into(), 2);
        })
        .await
        .unwrap();
    let refused = Store::<Ledger, _>::new(&journaled, ())
        .execute("acct-2", Add(1), Metadata::new("local"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            refused.as_store_error(),
            Some(Error::UnsupportedFeature { feature, version: 2 }) if feature == "journal"
        ),
        "{refused:?}"
    );

    tables.drop().await;
}

/// A store with outcomes over the journal, and writers racing for one scope on the emulator.
async fn store_end_to_end() {
    let tables = Tables::new("store").await;
    let journal = Journal::new(["ledger"], |_: &SerializedEvent| vec![scope("everything")])
        .unwrap()
        .with_retry(RetryPolicy::immediate(40));
    let journaled = JournalRepository::new(tables.repository(), journal);

    {
        let store = Store::<Ledger, _>::new(&journaled, ())
            .with_outcomes(OutcomeConfig::new().with_retry(RetryPolicy::immediate(5)));
        let command = |id: &str| Metadata::new("local").with_command_id(id);

        assert!(
            store
                .execute_with_outcome("acct-1", Add(5), command("cmd-1"))
                .await
                .unwrap()
                .is_accepted()
        );
        assert!(
            store
                .execute_with_outcome("acct-1", Add(5), command("cmd-1"))
                .await
                .unwrap()
                .replayed
        );
        assert!(
            store
                .execute_with_outcome("acct-1", Add(0), command("cmd-2"))
                .await
                .unwrap()
                .is_rejected()
        );
        assert_eq!(
            high_watermark(&journaled, &scope("everything"))
                .await
                .unwrap(),
            1,
            "a replay and a rejection journal nothing"
        );
    }

    // Writers on different streams race for the same scope. Each loses some positions to the
    // others and retries; none is lost or doubled.
    let writers: Vec<_> = (0..3)
        .map(|writer| {
            let journaled = journaled.clone();

            tokio::spawn(async move {
                let store = Store::<Ledger, _>::new(&journaled, ());

                for amount in 1..=3 {
                    store
                        .execute(
                            format!("racer-{writer}"),
                            Add(amount),
                            Metadata::new("local"),
                        )
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }

    let everything = page(&journaled, &scope("everything"), 0, 100)
        .await
        .unwrap();
    assert!(everything.is_caught_up(), "{:?}", everything.gap);
    assert_eq!(
        everything
            .rows
            .iter()
            .map(|row| row.position)
            .collect::<Vec<_>>(),
        (1..=10).collect::<Vec<_>>()
    );
    let mut ids: Vec<_> = everything
        .rows
        .iter()
        .map(|row| row.event.id.to_string())
        .collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 10);

    tables.drop().await;
}

/// No views table: the journal has nowhere to write, and says so before any write.
async fn without_a_views_table() {
    let tables = Tables::new("noviews").await;
    let repository =
        DynamoRepository::new(tables.client.clone(), &tables.events).with_lazy_initialization(true);
    let journal =
        Journal::new(["ledger"], |_: &SerializedEvent| vec![scope("everything")]).unwrap();
    let journaled = JournalRepository::new(repository.clone(), journal);

    let error = Store::<Ledger, _>::new(&journaled, ())
        .execute("acct-1", Add(1), Metadata::new("local"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.as_store_error(),
            Some(Error::ViewsNotConfigured {
                backend: "dynamodb"
            })
        ),
        "{error:?}"
    );
    assert!(
        repository
            .events(&"acct-1".into())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        repository
            .read_manifest()
            .await
            .unwrap()
            .is_none_or(|manifest| !manifest.declares_journal()),
        "a commit that could not be built declared nothing"
    );
    assert!(matches!(
        journaled.read_counter(&scope("everything")).await,
        Err(Error::ViewsNotConfigured { .. })
    ));

    tables.drop().await;
}

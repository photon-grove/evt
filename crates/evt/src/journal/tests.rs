//! Journal tests over the in-memory repository: the shared conformance cases, and the behavior
//! only reachable through a [`Store`](crate::Store).

use serde::{Deserialize, Serialize};

use super::{
    Journal, JournalRepository, Scope, allocations, high_watermark, page, rebuild_with_lock, replay,
};
use crate::conformance::{Add, Claim, Ledger, journal_cases};
use crate::{
    Aggregate, Error, FeatureMarker, MemRepository, Metadata, OutcomeCommand, OutcomeConfig,
    Rejection, Repository, RetryPolicy, SerializedEvent, Store, ToRejection, ViewRepository,
    canonical_json,
};

fn scope(name: &str) -> Scope {
    Scope::new(name).unwrap()
}

/// Every ledger and claim event goes to one scope of its stream and one shared scope.
fn journal() -> Journal {
    Journal::new(["ledger", "claim"], |event: &SerializedEvent| {
        vec![scope(&format!("stream:{}", event.entity_id)), scope("all")]
    })
    .unwrap()
    .with_retry(RetryPolicy::immediate(3))
}

fn ids(page: &super::JournalPage) -> Vec<String> {
    page.rows
        .iter()
        .map(|row| row.event.id.to_string())
        .collect()
}

#[tokio::test]
async fn the_journal_conformance_cases_pass_on_the_in_memory_repository() {
    let repository = MemRepository::new();

    journal_cases(&repository, &repository.clone()).await;
}

#[tokio::test]
async fn a_store_journals_every_command_atomically_with_its_events() {
    let repository = JournalRepository::new(MemRepository::new(), journal());
    let store = Store::<Ledger, _>::new(&repository, ()).with_snapshot_interval(2);

    for (id, amount) in [("l-1", 1), ("l-2", 2), ("l-1", 3), ("l-1", 4), ("l-2", 5)] {
        store
            .execute(id, Add(amount), Metadata::new("local"))
            .await
            .unwrap();
    }

    let all = repository.page(&scope("all"), 0, 100).await.unwrap();
    assert_eq!(
        ids(&all),
        ["l-1:1", "l-2:1", "l-1:2", "l-1:3", "l-2:2"],
        "the shared scope is in commit order"
    );
    assert!(all.is_caught_up());
    assert_eq!(
        ids(&repository.page(&scope("stream:l-2"), 0, 100).await.unwrap()),
        ["l-2:1", "l-2:2"]
    );
    assert_eq!(
        high_watermark(&repository, &scope("stream:l-1"))
            .await
            .unwrap(),
        3
    );

    // Snapshots (every second event here) do not disturb the journal.
    assert!(repository.snapshot(&"l-1".into()).await.unwrap().is_some());

    // The row carries the event as stored, allocations and all.
    let stored = repository.events(&"l-1".into()).await.unwrap();
    assert_eq!(all.rows[2].event, stored[1]);
    assert_eq!(
        allocations(&stored[1].metadata)
            .unwrap()
            .iter()
            .map(|allocation| (allocation.scope.as_str(), allocation.position))
            .collect::<Vec<_>>(),
        [("all", 3), ("stream:l-1", 2)]
    );
}

#[tokio::test]
async fn outcomes_replay_and_rejections_journal_nothing_more() {
    let repository = JournalRepository::new(MemRepository::new(), journal());
    let store = Store::<Ledger, _>::new(&repository, ())
        .with_snapshot_interval(0)
        .with_outcomes(OutcomeConfig::new().with_retry(RetryPolicy::immediate(3)));
    let command = |id: &str| Metadata::new("local").with_command_id(id);

    let first = store
        .execute_with_outcome("l-1", Add(5), command("cmd-1"))
        .await
        .unwrap();
    assert!(first.outcome.is_accepted() && !first.replayed);

    // The same command again replays the stored outcome and allocates nothing.
    let again = store
        .execute_with_outcome("l-1", Add(5), command("cmd-1"))
        .await
        .unwrap();
    assert!(again.replayed);

    // A refused command writes a rejection and no event, so no row.
    let refused = store
        .execute_with_outcome("l-1", Add(0), command("cmd-2"))
        .await
        .unwrap();
    assert!(refused.outcome.is_rejected());

    assert_eq!(high_watermark(&repository, &scope("all")).await.unwrap(), 1);
    assert_eq!(
        ids(&repository.page(&scope("all"), 0, 10).await.unwrap()),
        ["l-1:1"]
    );
}

#[tokio::test]
async fn a_transaction_journals_every_stream_it_writes_in_one_commit() {
    let repository = JournalRepository::new(MemRepository::new(), journal());
    let store = Store::<Ledger, _>::new(&repository, ()).with_snapshot_interval(0);

    let mut transaction = store.transaction();
    let mut ledger = transaction.read::<Ledger>("ledger-1").await.unwrap();
    let mut claim = transaction.read::<Claim>("claim-ada").await.unwrap();
    transaction
        .append(
            &mut ledger,
            vec![crate::conformance::Added { amount: 5 }.into()],
        )
        .unwrap();
    transaction
        .append(
            &mut claim,
            vec![
                crate::conformance::Taken {
                    holder: "ada".into(),
                }
                .into(),
            ],
        )
        .unwrap();
    transaction.commit(&Metadata::new("local")).await.unwrap();

    // `claim-ada` sorts before `ledger-1`.
    let all = repository.page(&scope("all"), 0, 10).await.unwrap();
    assert_eq!(ids(&all), ["claim-ada:1", "ledger-1:1"]);

    // Both streams' types are declared history-dependent, once the journal is used.
    let manifest = repository.inner().read_manifest().await.unwrap().unwrap();
    assert!(manifest.history_dependent.contains("ledger"));
    assert!(manifest.history_dependent.contains("claim"));
    assert!(manifest.declares_journal());
}

#[tokio::test]
async fn rebuilding_into_wiped_rows_restores_them_exactly() {
    let memory = MemRepository::new();
    let repository = JournalRepository::new(memory.clone(), journal());
    let store = Store::<Ledger, _>::new(&repository, ());

    for index in 0..6 {
        store
            .execute(
                format!("l-{}", index % 3),
                Add(index + 1),
                Metadata::new("local"),
            )
            .await
            .unwrap();
    }

    let before = memory.views();
    assert!(!before.is_empty());

    let mut events = Vec::new();
    for id in ["l-0", "l-1", "l-2"] {
        events.extend(memory.events(&id.into()).await.unwrap());
    }
    events.reverse();

    for row in &before {
        memory.delete_view(&row.pk, &row.sk).await.unwrap();
    }
    assert!(memory.views().is_empty());

    rebuild_with_lock(&memory, &events, std::time::Duration::from_secs(60))
        .await
        .unwrap();

    assert_eq!(memory.views(), before, "the same rows, counters included");
    assert_eq!(replay(&events).unwrap().views(), before);
}

/// A note aggregate whose handler pre-checks the journal row of what it decides, so an oversized
/// note is refused as a recorded rejection instead of failing the commit.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Notes {
    count: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Noted {
    text: String,
}

crate::domain_events! {
    #[derive(Clone, Debug, PartialEq)]
    enum NoteEvent {
        Noted(Noted) => ("note:noted", 1),
    }
}

#[derive(Clone)]
struct Note {
    stream: String,
    text: String,
}

impl OutcomeCommand for Note {
    fn command_type(&self) -> String {
        "note:note".into()
    }

    fn canonical_payload(&self) -> Result<Option<Vec<u8>>, crate::BoxError> {
        Ok(Some(canonical_json(&self.text)?))
    }
}

#[derive(Debug, thiserror::Error)]
enum NoteError {
    #[error(transparent)]
    TooLarge(#[from] crate::RowTooLarge),

    #[error("the journal failed: {0}")]
    Journal(String),
}

impl ToRejection for NoteError {
    fn to_rejection(&self) -> Option<Rejection> {
        match self {
            Self::TooLarge(row) => Some(row.to_rejection()),
            Self::Journal(_) => None,
        }
    }
}

impl Aggregate for Notes {
    const TYPE: &'static str = "note";

    type Command = Note;
    type Event = NoteEvent;
    type Error = NoteError;
    type Services = Journal;

    async fn handle(&self, command: Note, journal: &Journal) -> Result<Vec<NoteEvent>, NoteError> {
        let events = vec![Noted { text: command.text }.into()];

        match journal.pre_check_events::<Notes>(
            &command.stream.as_str().into(),
            crate::EventSequence(self.count),
            &events,
            &Metadata::new("local").with_command_id("cmd-big"),
        ) {
            Ok(()) => Ok(events),
            Err(Error::JournalRowTooLarge(row)) => Err(row.into()),
            Err(other) => Err(NoteError::Journal(other.to_string())),
        }
    }

    fn apply(&mut self, _: NoteEvent) {
        self.count += 1;
    }
}

#[tokio::test]
async fn a_pre_check_turns_an_oversized_row_into_a_recorded_rejection() {
    let journal = Journal::new(["note"], |event: &SerializedEvent| {
        vec![scope(&format!("notes:{}", event.entity_id))]
    })
    .unwrap()
    .with_max_row_bytes(1_000)
    .unwrap();
    let repository = JournalRepository::new(MemRepository::new(), journal.clone());
    let store = Store::<Notes, _>::new(&repository, journal)
        .with_outcomes(OutcomeConfig::new().with_retry(RetryPolicy::immediate(3)));
    let command = |id: &str| Metadata::new("local").with_command_id(id);
    let note = |text: String| Note {
        stream: "n-1".into(),
        text,
    };

    let small = store
        .execute_with_outcome("n-1", note("hello".into()), command("cmd-small"))
        .await
        .unwrap();
    assert!(small.outcome.is_accepted());

    // A command ID of the same length as the one the pre-check assumed measures the same row.
    let big = store
        .execute_with_outcome("n-1", note("x".repeat(2_000)), command("cmd-big"))
        .await
        .unwrap();
    let rejection = big.rejection().expect("a recorded rejection");
    assert_eq!(rejection.code.as_str(), "journal-row-too-large");
    assert_eq!(rejection.details.as_ref().unwrap()["limit"], 1_000);

    // Nothing was written for it, and the same command ID replays the same rejection.
    assert_eq!(repository.events(&"n-1".into()).await.unwrap().len(), 1);
    let replayed = store
        .execute_with_outcome("n-1", note("x".repeat(2_000)), command("cmd-big"))
        .await
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.rejection(), Some(rejection));
    assert_eq!(
        high_watermark(&repository, &scope("notes:n-1"))
            .await
            .unwrap(),
        1
    );

    // The commit-time guard agrees with what the pre-check measured: a note that passes the
    // pre-check commits.
    let page = page(&repository, &scope("notes:n-1"), 0, 10).await.unwrap();
    assert_eq!(page.rows.len(), 1);
}

#[tokio::test]
async fn journaled_events_are_ordinary_events_to_a_plain_repository() {
    // Journaled events are ordinary events: a plain repository over the same storage reads and
    // replays them.
    let memory = MemRepository::new();
    let repository = JournalRepository::new(memory.clone(), journal());
    Store::<Ledger, _>::new(&repository, ())
        .execute("l-1", Add(5), Metadata::new("local"))
        .await
        .unwrap();

    let plain = Store::<Ledger, _>::new(&memory, ());
    assert_eq!(plain.load("l-1").await.unwrap().aggregate().total, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_to_one_scope_get_contiguous_unique_positions() {
    let journal = journal().with_retry(RetryPolicy::immediate(200));
    let repository = JournalRepository::new(MemRepository::new(), journal);

    let writers: Vec<_> = (0..8)
        .map(|writer| {
            let repository = repository.clone();

            tokio::spawn(async move {
                let store = Store::<Ledger, _>::new(&repository, ());

                for amount in 1..=5 {
                    store
                        .execute(format!("l-{writer}"), Add(amount), Metadata::new("local"))
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }

    let all = repository.page(&scope("all"), 0, 100).await.unwrap();
    assert!(all.is_caught_up(), "{:?}", all.gap);
    assert_eq!(
        all.rows.iter().map(|row| row.position).collect::<Vec<_>>(),
        (1..=40).collect::<Vec<_>>()
    );

    let mut seen: Vec<String> = ids(&all);
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 40, "every event is in the journal exactly once");

    // Each writer's own events keep their stream order within the shared scope.
    for writer in 0..8 {
        let sequences: Vec<i64> = all
            .rows
            .iter()
            .filter(|row| row.owner.as_str() == format!("l-{writer}"))
            .map(|row| row.event.sequence.0)
            .collect();
        assert_eq!(sequences, [1, 2, 3, 4, 5]);
    }
}

#[tokio::test]
async fn a_commit_to_an_unmigrated_table_leaves_no_journal_declaration() {
    let memory = MemRepository::new();

    // A stream with no head item: the table was written by a build before stream heads.
    memory.append_bypassing_head(SerializedEvent {
        id: "old-1:1".into(),
        sequence: crate::EventSequence(1),
        event_type: "ledger:added".into(),
        version: crate::EventVersion(1),
        entity_id: "old-1".into(),
        entity_type: "ledger".into(),
        payload: br#"{"amount":1}"#.to_vec(),
        ..Default::default()
    });

    let repository = JournalRepository::new(memory.clone(), journal());
    let error = Store::<Ledger, _>::new(&repository, ())
        .execute("l-1", Add(5), Metadata::new("local"))
        .await
        .unwrap_err();
    assert!(
        matches!(error.as_store_error(), Some(Error::StreamHeadsNotMigrated)),
        "{error:?}"
    );

    // The marker is not left declaring a journal that nothing was written into, which would
    // otherwise block the migration tool on a table the journal never touched.
    let manifest = memory.read_manifest().await.unwrap();
    assert!(
        manifest.is_none_or(|manifest| !manifest.declares_journal()),
        "the journal declaration was retracted"
    );
    assert!(memory.views().is_empty());
}

#[tokio::test]
async fn a_row_the_guard_refuses_after_the_pre_check_passed_is_a_recorded_rejection() {
    let journal = Journal::new(["note"], |event: &SerializedEvent| {
        vec![scope(&format!("notes:{}", event.entity_id))]
    })
    .unwrap()
    .with_max_row_bytes(1_000)
    .unwrap();
    let repository = JournalRepository::new(MemRepository::new(), journal.clone());
    let store = Store::<Notes, _>::new(&repository, journal)
        .with_outcomes(OutcomeConfig::new().with_retry(RetryPolicy::immediate(3)));

    // The handler pre-checks with a short command ID; the real one is long, and the command ID
    // is in the row three times (command, causation, and commit ID), so only the guard sees it.
    let long = "c".repeat(300);
    let outcome = store
        .execute_with_outcome(
            "n-1",
            Note {
                stream: "n-1".into(),
                text: "hello".into(),
            },
            Metadata::new("local").with_command_id(long.clone()),
        )
        .await
        .unwrap();

    let rejection = outcome.rejection().expect("a recorded rejection");
    assert_eq!(rejection.code.as_str(), "journal-row-too-large");
    assert!(repository.events(&"n-1".into()).await.unwrap().is_empty());
    assert_eq!(
        high_watermark(&repository, &scope("notes:n-1"))
            .await
            .unwrap(),
        0
    );

    // The same command ID replays the recorded rejection.
    let replay = store
        .execute_with_outcome(
            "n-1",
            Note {
                stream: "n-1".into(),
                text: "hello".into(),
            },
            Metadata::new("local").with_command_id(long),
        )
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.rejection(), Some(rejection));
}

#[test]
fn a_row_cap_over_the_item_limit_is_refused_and_the_pre_check_headroom_adds_up() {
    let build = || Journal::new(["ledger"], |_: &SerializedEvent| vec![scope("s")]).unwrap();

    assert!(build().with_max_row_bytes(super::MAX_ROW_BYTES).is_ok());
    assert!(matches!(
        build().with_max_row_bytes(super::MAX_ROW_BYTES + 1),
        Err(Error::InvalidJournal(_))
    ));

    let event = SerializedEvent {
        entity_type: "ledger".into(),
        ..Default::default()
    };
    let Err(Error::JournalRowTooLarge(measured)) = build()
        .with_max_row_bytes(0)
        .unwrap()
        .pre_check(std::slice::from_ref(&event))
    else {
        panic!("a zero cap refuses");
    };

    let tight = build().with_max_row_bytes(measured.size).unwrap();
    assert!(tight.pre_check(std::slice::from_ref(&event)).is_ok());
    assert!(
        tight
            .with_pre_check_headroom(1)
            .pre_check(std::slice::from_ref(&event))
            .is_err(),
        "headroom counts against the cap in the pre-check"
    );
}

#[test]
fn the_readme_example_is_the_compiled_example() {
    let example = include_str!("../../examples/journal.rs");
    let readme = include_str!("../../README.md");
    let body = example
        .split("// README:begin\n")
        .nth(1)
        .and_then(|rest| rest.split("// README:end").next())
        .expect("the example marks its README block")
        .trim_end();

    assert!(
        readme.contains(body),
        "the journal example in crates/evt/README.md differs from examples/journal.rs"
    );
}

#[test]
fn a_resolver_may_return_a_result_and_its_failure_is_an_invalid_journal() {
    let journal = Journal::new(["ledger"], |event: &SerializedEvent| {
        Ok::<_, crate::Error>(vec![Scope::new(format!("l:{}", event.entity_id))?])
    })
    .unwrap();
    let ok = SerializedEvent {
        entity_id: "a".into(),
        entity_type: "ledger".into(),
        ..Default::default()
    };
    assert_eq!(journal.scopes_of(&ok).unwrap(), vec![scope("l:a")]);

    // An empty entity ID makes `Scope::new` fail inside the closure: the error is the commit's.
    let bad = SerializedEvent {
        entity_type: "ledger".into(),
        ..ok
    };
    let bad = SerializedEvent {
        entity_id: "".into(),
        ..bad
    };
    let journal = Journal::new(["ledger"], |event: &SerializedEvent| {
        Ok::<_, crate::Error>(vec![Scope::new(event.entity_id.as_str())?])
    })
    .unwrap();
    assert!(matches!(
        journal.scopes_of(&bad),
        Err(Error::InvalidJournal(_))
    ));
}

#[test]
fn replay_refuses_different_records_claiming_one_position_whatever_the_order() {
    let make = |payload: &str| {
        let mut event = SerializedEvent {
            id: "l-1:1".into(),
            sequence: crate::EventSequence(1),
            entity_id: "l-1".into(),
            entity_type: "ledger".into(),
            payload: payload.as_bytes().to_vec(),
            ..Default::default()
        };
        super::set_allocations(&mut event, &[super::Allocation::new(&scope("s"), 1)]);

        event
    };
    let (first, second) = (make(r#"{"amount":1}"#), make(r#"{"amount":2}"#));

    // The same ID and allocation, another payload: not the same record.
    for events in [[&first, &second], [&second, &first]] {
        assert!(
            matches!(replay(events), Err(Error::InvalidJournal(_))),
            "order must not decide which record wins"
        );
    }

    // An exact duplicate is one row.
    let state = replay([&first, &first]).unwrap();
    assert_eq!(state.rows.len(), 1);
    assert_eq!(state.counters[&scope("s")], 1);
}

/// A projector that writes what it sees of each event's metadata extensions.
struct SeesMetadata;

impl crate::CommitProjector for SeesMetadata {
    fn name(&self) -> &str {
        "sees-metadata"
    }

    fn history(&self) -> &[crate::EntityType] {
        static LEDGER: std::sync::OnceLock<[crate::EntityType; 1]> = std::sync::OnceLock::new();

        LEDGER.get_or_init(|| [crate::EntityType::new("ledger")])
    }

    fn project(
        &self,
        commit: &crate::CommitView,
    ) -> Result<Vec<crate::ViewOperation>, crate::BoxError> {
        let mut operations = Vec::new();

        for stream in &commit.streams {
            for event in &stream.events {
                operations.push(crate::ViewOperation::put(
                    "seen",
                    event.id.as_str(),
                    stream.entity_id.clone(),
                    "seen",
                    serde_json::to_vec(&event.metadata.extensions)?,
                ));
            }
        }

        Ok(operations)
    }
}

#[tokio::test]
async fn a_projector_that_reads_metadata_sees_the_same_events_live_and_on_replay() {
    let memory = MemRepository::new();
    let repository = JournalRepository::new(memory.clone(), journal());
    let projector: std::sync::Arc<dyn crate::CommitProjector> = std::sync::Arc::new(SeesMetadata);
    let store = Store::<Ledger, _>::new(&repository, ()).with_commit_projector(projector.clone());

    for (id, amount) in [("l-1", 1), ("l-2", 2), ("l-1", 3)] {
        store
            .execute(id, Add(amount), Metadata::new("local"))
            .await
            .unwrap();
    }

    // The stored events carry the journal's allocation; the projector's live rows never saw it.
    let stored = memory.events(&"l-1".into()).await.unwrap();
    assert!(stored[0].metadata.extensions.contains_key("evt.journal"));

    let live: Vec<_> = memory
        .views()
        .into_iter()
        .filter(|row| row.pk == "seen")
        .collect();
    assert_eq!(live.len(), 3);
    assert!(
        live.iter()
            .all(|row| !String::from_utf8_lossy(&row.payload).contains("evt.journal"))
    );

    let mut events = stored;
    events.extend(memory.events(&"l-2".into()).await.unwrap());
    let replayed =
        crate::replay_views(&[projector], &crate::group_commits(events).unwrap()).unwrap();

    assert_eq!(
        replayed, live,
        "a replay of the stored events reproduces the live rows"
    );
}

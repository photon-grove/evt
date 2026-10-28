//! A todo list whose read model commits with its events and can be rebuilt from the log: the Rust
//! port of Go's `go/examples/todo`.
//!
//! - `TodoList` is an aggregate on `MemRepository`.
//! - `SummaryProjector` is a current-state [`CommitProjector`]: it reads each written list's state
//!   after the commit and puts that list's open and done counts in a view row. The row is part of
//!   the same commit as the events, so it never disagrees with the stream behind it, and a
//!   rejected command writes neither.
//! - `rebuild_summaries` rebuilds every row from the event log alone: it enumerates the stored
//!   events, regroups them into the commits that wrote them with `group_commits`, folds each
//!   list's state as it stood after each commit, and runs the projector again with
//!   `replay_views`, which starts from an empty set of rows, as a wiped view table does. The
//!   rebuilt rows equal the live ones.
//!
//! Run it with `cargo run -p photon-evt --example todo`.

use std::future::poll_fn;
use std::pin::pin;
use std::sync::Arc;

use futures_core::Stream;
use photon_evt::{
    Aggregate, BoxError, CommitProjector, CommitView, DEFAULT_VIEW_SK, DomainEvent, EntityId,
    EventLog, EventSequence, MemRepository, Metadata, SerializedEvent, Store, StreamFilter,
    ViewOperation, ViewRow, domain_events, group_commits, json, replay_views,
};
use serde::{Deserialize, Serialize};

/// The prefix of a summary row's partition key.
const SUMMARY_PREFIX: &str = "todo_summary#";

// ---- Events ----

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemAdded {
    pub item_id: String,
    pub title: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemCompleted {
    pub item_id: String,
}

domain_events! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum TodoEvent {
        Added(ItemAdded) => ("todo_list.item_added", 1),
        Completed(ItemCompleted) => ("todo_list.item_completed", 1),
    }
}

// ---- Aggregate ----

#[derive(Clone, Debug, PartialEq)]
pub enum TodoCommand {
    Add { item_id: String, title: String },
    Complete { item_id: String },
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum TodoError {
    #[error("item already added")]
    AlreadyAdded,
    #[error("no such item")]
    NoSuchItem,
    #[error("item already done")]
    AlreadyDone,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Item {
    pub id: String,
    pub title: String,
    pub done: bool,
}

/// The aggregate. Its JSON is the state a commit-wide projector is given.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TodoList {
    pub items: Vec<Item>,
}

impl TodoList {
    fn item(&self, item_id: &str) -> Option<&Item> {
        self.items.iter().find(|item| item.id == item_id)
    }

    /// The list's open and done counts.
    pub fn summary(&self) -> Summary {
        let done = self.items.iter().filter(|item| item.done).count();

        Summary {
            open: self.items.len() - done,
            done,
        }
    }
}

impl Aggregate for TodoList {
    const TYPE: &'static str = "todo_list";

    type Command = TodoCommand;
    type Event = TodoEvent;
    type Error = TodoError;
    type Services = ();

    async fn handle(&self, command: TodoCommand, _: &()) -> Result<Vec<TodoEvent>, TodoError> {
        match command {
            TodoCommand::Add { item_id, title } => {
                if self.item(&item_id).is_some() {
                    return Err(TodoError::AlreadyAdded);
                }

                Ok(vec![ItemAdded { item_id, title }.into()])
            }

            TodoCommand::Complete { item_id } => match self.item(&item_id) {
                None => Err(TodoError::NoSuchItem),
                Some(item) if item.done => Err(TodoError::AlreadyDone),
                Some(_) => Ok(vec![ItemCompleted { item_id }.into()]),
            },
        }
    }

    fn apply(&mut self, event: TodoEvent) {
        match event {
            TodoEvent::Added(e) => self.items.push(Item {
                id: e.item_id,
                title: e.title,
                done: false,
            }),
            TodoEvent::Completed(e) => {
                if let Some(item) = self.items.iter_mut().find(|item| item.id == e.item_id) {
                    item.done = true;
                }
            }
        }
    }
}

// ---- Summary view ----

/// The payload of a list's summary row.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub open: usize,
    pub done: usize,
}

/// Puts one summary row per list, at `todo_summary#<list ID>`, owned by the list's stream.
///
/// It is a current-state projector (it names no `history`), so it is given each written stream's
/// state after the commit and no events, and its rows are a function of that state. It is pure: no
/// clock, no store reads, no state kept between calls.
pub struct SummaryProjector;

impl CommitProjector for SummaryProjector {
    fn name(&self) -> &str {
        "todo_summary"
    }

    fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
        commit
            .streams
            .iter()
            .filter(|stream| stream.entity_type == TodoList::TYPE)
            .map(|stream| {
                let list: TodoList = serde_json::from_slice(&stream.state)?;

                Ok(ViewOperation::put(
                    summary_pk(stream.entity_id.as_str()),
                    DEFAULT_VIEW_SK,
                    stream.entity_id.clone(),
                    TodoList::TYPE,
                    json::to_vec(&list.summary())?,
                ))
            })
            .collect()
    }
}

fn summary_pk(list_id: &str) -> String {
    format!("{SUMMARY_PREFIX}{list_id}")
}

/// A list's summary row, if a commit wrote one.
pub fn read_summary(repository: &MemRepository, list_id: &str) -> Option<Summary> {
    let row = repository.view(&summary_pk(list_id), DEFAULT_VIEW_SK)?;

    serde_json::from_slice(&row.payload).ok()
}

// ---- Wiring ----

type Lists = Store<TodoList, MemRepository>;

/// A store over the in-memory repository, which keeps the rows commit-wide projectors write
/// (`view`, `views`). On DynamoDB the same projector writes to `with_views_table`'s table.
pub fn new_store() -> Lists {
    Store::new(MemRepository::new(), ()).with_commit_projector(Arc::new(SummaryProjector))
}

/// A short history: two lists, three items, one completed.
pub async fn seed(lists: &Lists) -> Result<(), Box<dyn std::error::Error>> {
    let add = |item_id: &str, title: &str| TodoCommand::Add {
        item_id: item_id.into(),
        title: title.into(),
    };

    for (list, command) in [
        ("list-1", add("milk", "Buy milk")),
        ("list-1", add("bread", "Buy bread")),
        ("list-2", add("taxes", "File taxes")),
        (
            "list-1",
            TodoCommand::Complete {
                item_id: "milk".into(),
            },
        ),
    ] {
        lists.execute(list, command, Metadata::default()).await?;
    }

    Ok(())
}

// ---- Rebuild ----

/// Rebuilds every summary row from the stored events alone, into an empty set of rows, and returns
/// the rows and the number of commits replayed.
pub async fn rebuild_summaries(
    repository: &MemRepository,
) -> Result<(Vec<ViewRow>, usize), Box<dyn std::error::Error>> {
    // Enumerating the log is the rebuild and diagnostic path, not a hot read. Any
    // `futures_core::Stream` consumer works; this one needs no extra dependency.
    let mut stored: Vec<SerializedEvent> = Vec::new();
    let mut batches = pin!(repository.stream_all_events(StreamFilter::of_type(TodoList::TYPE)));
    while let Some(batch) = poll_fn(|cx| batches.as_mut().poll_next(cx)).await {
        stored.extend(batch?);
    }

    let mut commits = group_commits(stored.iter().cloned())?;
    for commit in &mut commits {
        for stream in &mut commit.streams {
            let through = stream
                .events
                .last()
                .map_or(EventSequence::ZERO, |e| e.sequence);
            stream.state = state_after(&stored, &stream.entity_id, through)?;
        }
    }

    let projectors: [Arc<dyn CommitProjector>; 1] = [Arc::new(SummaryProjector)];
    let rows = replay_views(&projectors, &commits)?;

    Ok((rows, commits.len()))
}

/// Folds a list's stored events up to and including `through` and returns its JSON: the state the
/// store gave the projector when that commit was live.
fn state_after(
    stored: &[SerializedEvent],
    list_id: &EntityId,
    through: EventSequence,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut events: Vec<&SerializedEvent> = stored
        .iter()
        .filter(|event| event.entity_id == *list_id && event.sequence <= through)
        .collect();
    events.sort_by_key(|event| event.sequence);

    let mut list = TodoList::default();
    for event in events {
        list.apply(TodoEvent::from_payload(
            event.event_type.as_str(),
            event.version,
            &event.payload,
        )?);
    }

    Ok(json::to_vec(&list)?)
}

/// Seeds the lists, then returns what `main` prints: each summary, and whether the rebuild matched.
pub async fn run() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let lists = new_store();
    seed(&lists).await?;

    let repository = lists.repository();
    let mut lines = Vec::new();

    for list in ["list-1", "list-2"] {
        let summary = read_summary(repository, list).ok_or("no summary row")?;
        lines.push(format!(
            "{list}: open={} done={}",
            summary.open, summary.done
        ));
    }

    let (rebuilt, commits) = rebuild_summaries(repository).await?;
    lines.push(format!(
        "rebuilt {} rows from {commits} commits; equal to the live rows: {}",
        rebuilt.len(),
        rebuilt == repository.views(),
    ));

    Ok(lines)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    for line in run().await? {
        println!("{line}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use photon_evt::ExecuteError;

    use super::*;

    #[tokio::test]
    async fn it_prints_the_summaries_and_a_matching_rebuild() {
        assert_eq!(
            run().await.unwrap(),
            [
                "list-1: open=1 done=1",
                "list-2: open=1 done=0",
                "rebuilt 2 rows from 4 commits; equal to the live rows: true",
            ]
        );
    }

    #[tokio::test]
    async fn the_summary_row_commits_with_each_command() {
        let lists = new_store();

        lists
            .execute(
                "list-1",
                TodoCommand::Add {
                    item_id: "a".into(),
                    title: "A".into(),
                },
                Metadata::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            read_summary(lists.repository(), "list-1"),
            Some(Summary { open: 1, done: 0 })
        );

        lists
            .execute(
                "list-1",
                TodoCommand::Complete {
                    item_id: "a".into(),
                },
                Metadata::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            read_summary(lists.repository(), "list-1"),
            Some(Summary { open: 0, done: 1 })
        );

        let row = lists
            .repository()
            .view("todo_summary#list-1", DEFAULT_VIEW_SK)
            .unwrap();
        assert_eq!(
            row.entity_id, "list-1",
            "the row is owned by the list's stream"
        );
    }

    #[tokio::test]
    async fn a_rejected_command_writes_no_event_and_no_row() {
        let lists = new_store();

        let error = lists
            .execute(
                "list-1",
                TodoCommand::Complete {
                    item_id: "missing".into(),
                },
                Metadata::default(),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, ExecuteError::Rejected(TodoError::NoSuchItem)),
            "{error:?}"
        );
        assert!(lists.repository().views().is_empty());
        assert_eq!(
            lists.load("list-1").await.unwrap().sequence(),
            EventSequence::ZERO
        );
    }

    #[tokio::test]
    async fn the_rebuild_reproduces_the_live_rows_from_the_log_alone() {
        let lists = new_store();
        seed(&lists).await.unwrap();

        let (rebuilt, commits) = rebuild_summaries(lists.repository()).await.unwrap();

        assert_eq!(commits, 4);
        assert_eq!(rebuilt.len(), 2);
        assert_eq!(rebuilt, lists.repository().views());
    }
}

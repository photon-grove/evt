//! The commit-wide projector cases (v0.9, C7): view rows written atomically with a commit's events,
//! per-operation error mapping, ranking against the other commit failures, the budget, and
//! determinism.

use std::error::Error as StdError;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::*;
use crate::error::ExecuteError;
use crate::json;
use crate::projection::{
    CommitProjector, CommitView, ProjectedView, ProjectorOperationError, REASON_CONDITION_FAILED,
    ViewOperation, ViewOperationKind, ViewRow, group_commits, project_commit, replay_views,
};

/// A backend's commit-wide view storage, for [`commit_view_cases`]: how to read back the row a
/// view operation wrote. The repository handed to the cases must already be configured to write
/// views (a views table, a view writer; the in-memory repository always is).
pub trait CommitViewTarget<R: Repository>: Send + Sync {
    /// Reads the row at `(pk, sk)`, if one exists.
    fn row(
        &self,
        pk: &str,
        sk: &str,
    ) -> impl Future<Output = Result<Option<ViewRow>, BoxError>> + Send;

    /// Whether the backend counts view payload bytes in the commit budget. A PostgreSQL
    /// repository sizes a view statement by its SQL text only, because the bound parameters are
    /// opaque, so it opts out of the byte-budget case; the action count is exact everywhere.
    fn budgets_view_bytes(&self) -> bool {
        true
    }

    /// The most UTF-8 bytes a row's entity type, `pk` and `sk` may hold together, when the backend
    /// bounds them below DynamoDB's 2,048-byte `pk` and 1,024-byte `sk`: PostgreSQL's views table
    /// stores them in btree indexes, whose entries hold at most 2,704 bytes, and refuses a row over
    /// 2,600 with [`Error::ViewKeyTooLong`] (a known difference). `None` is DynamoDB's limits.
    fn max_view_key_bytes(&self) -> Option<usize> {
        None
    }
}

impl CommitViewTarget<MemRepository> for MemRepository {
    async fn row(&self, pk: &str, sk: &str) -> Result<Option<ViewRow>, BoxError> {
        Ok(self.view(pk, sk))
    }
}

/// Runs the commit-wide projector cases against a repository configured to write views. Like
/// [`outcome_cases`], it panics after running every case if any failed. Every case namespaces its
/// view keys, so a durable table can be shared across runs.
pub async fn commit_view_cases<R, T>(repo: &R, target: &T)
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let mut failures = Vec::new();

    let mut record = |name: &str, result: CaseResult| match result {
        Ok(()) => println!("ok   {name}"),
        Err(Failure(message)) => {
            println!("FAIL {name}");
            failures.push(format!("{name}: {message}"));
        }
    };

    record(
        "view rows are written beside the events at any pk/sk, overwritten, and deleted",
        Box::pin(view_rows(repo, target)).await,
    );
    record(
        "a failed view condition names the projector and operation and carries the mapped error",
        Box::pin(view_conditions(repo, target)).await,
    );
    record(
        "a projector failure ranks below the outcome conflict and every stream conflict",
        Box::pin(view_precedence(repo, target)).await,
    );
    record(
        "view operations with no event are refused on every entry point before any write",
        Box::pin(view_projector_only(repo, target)).await,
    );
    record(
        "view keys and text are checked before any write",
        Box::pin(view_checks(repo, target)).await,
    );
    record(
        "view operations count against the commit budget",
        Box::pin(view_budget(repo, target)).await,
    );
    if target.budgets_view_bytes() {
        record(
            "view payload bytes count against the byte budgets",
            Box::pin(view_byte_budget(repo, target)).await,
        );
    }
    record(
        "a re-prepared command projects again and only the committing attempt writes",
        Box::pin(view_recompute(repo, target)).await,
    );
    record(
        "a rejection that carries events projects them, a plain rejection and a no-op project nothing",
        Box::pin(view_rejections(repo, target)).await,
    );
    record(
        "a stream never overwrites or deletes a row another stream owns",
        Box::pin(view_ownership(repo, target)).await,
    );
    record(
        "a projector sees only what its kind declares, and owns only rows of streams it may name",
        Box::pin(view_visibility(repo, target)).await,
    );
    record(
        "a projector replayed over regrouped events reproduces the live rows",
        Box::pin(view_determinism(repo, target)).await,
    );
    record(
        "a history-dependent projector is declared durably and its type is never compacted",
        Box::pin(view_history_declaration(repo, target)).await,
    );
    record(
        "a failing projector is an internal failure: nothing written, nothing recorded, no retry",
        Box::pin(view_projector_failure(repo, target)).await,
    );
    record(
        "an application projector's operation on a journal partition is refused and writes nothing",
        Box::pin(view_reserved_partitions(repo, target)).await,
    );

    if !failures.is_empty() {
        panic!(
            "{} commit-wide projector case(s) failed:\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }
}

type Script = Box<dyn Fn(&CommitView) -> Vec<ViewOperation> + Send + Sync>;

/// A projector over a closure, counting its calls and recording what it returned, per commit ID.
struct Scripted {
    name: String,
    history: Vec<EntityType>,
    script: Script,
    calls: AtomicUsize,
    emitted: Mutex<Vec<(CommitView, Vec<ViewOperation>)>>,
}

impl Scripted {
    /// A current-state projector: it is given views without events.
    fn new(
        name: &str,
        script: impl Fn(&CommitView) -> Vec<ViewOperation> + Send + Sync + 'static,
    ) -> Arc<Self> {
        Self::reading(name, &[], script)
    }

    /// A history-dependent projector over the events of the given entity types.
    fn reading(
        name: &str,
        history: &[&str],
        script: impl Fn(&CommitView) -> Vec<ViewOperation> + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            name: name.to_owned(),
            history: history
                .iter()
                .map(|entity_type| EntityType::new(*entity_type))
                .collect(),
            script: Box::new(script),
            calls: AtomicUsize::new(0),
            emitted: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn emitted(&self) -> Vec<(CommitView, Vec<ViewOperation>)> {
        self.emitted
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl CommitProjector for Scripted {
    fn name(&self) -> &str {
        &self.name
    }

    fn history(&self) -> &[EntityType] {
        &self.history
    }

    fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
        self.calls.fetch_add(1, Ordering::SeqCst);

        let operations = (self.script)(commit);
        self.emitted
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((commit.clone(), operations.clone()));

        Ok(operations)
    }
}

/// The projector's own error for a claim that is already taken.
#[derive(Debug, PartialEq, Eq)]
struct NameTaken(String);

impl fmt::Display for NameTaken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the name {} is taken", self.0)
    }
}

impl StdError for NameTaken {}

async fn row<R: Repository, T: CommitViewTarget<R>>(
    target: &T,
    pk: &str,
    sk: &str,
) -> Result<Option<ViewRow>, Failure> {
    target
        .row(pk, sk)
        .await
        .map_err(|err| Failure(format!("reading the row {pk}/{sk}: {err}")))
}

fn summary(view: &CommitView) -> Vec<u8> {
    format!(
        r#"{{"commitId":"{}","streams":{}}}"#,
        view.commit_id,
        view.streams.len()
    )
    .into_bytes()
}

/// A current-state projector for the row cases: one directory row per written stream, owned by the
/// stream, at a partition that names no entity, and a count row per stream. It sees no commit ID
/// and no events, so none of its rows can depend on them.
fn directory(ns: &str) -> Arc<Scripted> {
    let ns = ns.to_owned();

    Scripted::new("directory", move |view| {
        view.streams
            .iter()
            .flat_map(|stream| {
                [
                    ViewOperation::put(
                        format!("{ns}-directory"),
                        stream.entity_id.as_str(),
                        stream.entity_id.clone(),
                        stream.entity_type.as_str(),
                        stream.state.clone(),
                    ),
                    ViewOperation::put(
                        format!("{ns}-width"),
                        stream.entity_id.as_str(),
                        stream.entity_id.clone(),
                        "",
                        format!(r#"{{"streams":{}}}"#, view.streams.len()),
                    ),
                ]
            })
            .collect()
    })
}

/// A history-dependent projector that writes only the per-commit summary row of
/// [`directory_reading`].
fn commit_summary(ns: &str) -> Arc<Scripted> {
    let ns = ns.to_owned();

    Scripted::reading("summary", &[Ledger::TYPE, Claim::TYPE], move |view| {
        vec![ViewOperation::put(
            format!("{ns}-summary"),
            view.commit_id.as_str(),
            view.streams[0].entity_id.clone(),
            "",
            summary(view),
        )]
    })
}

/// A history-dependent projector over the ledger and claim types: like [`directory`], plus one
/// summary row per commit, keyed by the commit ID (which only a history-dependent projector sees)
/// and owned by the commit's first stream. Every row has one stable owner, so a replay can apply
/// the writes to it in its owner's order.
fn directory_reading(ns: &str) -> Arc<Scripted> {
    let ns = ns.to_owned();

    Scripted::reading("directory", &[Ledger::TYPE, Claim::TYPE], move |view| {
        let mut operations: Vec<ViewOperation> = view
            .streams
            .iter()
            .map(|stream| {
                ViewOperation::put(
                    format!("{ns}-directory"),
                    stream.entity_id.as_str(),
                    stream.entity_id.clone(),
                    stream.entity_type.as_str(),
                    stream.state.clone(),
                )
            })
            .collect();
        operations.push(ViewOperation::put(
            format!("{ns}-summary"),
            view.commit_id.as_str(),
            view.streams[0].entity_id.clone(),
            "",
            summary(view),
        ));

        operations
    })
}

fn claim_id() -> EntityId {
    EntityId::new(format!("{}-claim", namespace()))
}

/// The error of a failed result, or a failure naming `what` when it succeeded.
fn error_of<T: fmt::Debug>(result: Result<T, Error>, what: &str) -> Result<Error, Failure> {
    match result {
        Err(error) => Ok(error),
        Ok(value) => Err(Failure(format!(
            "{what}: expected a failure, got {value:?}"
        ))),
    }
}

/// The store error of a failed command.
fn store_failed<T: fmt::Debug, E: fmt::Debug>(
    result: Result<T, ExecuteError<E>>,
    what: &str,
) -> Result<Error, Failure> {
    match result {
        Err(ExecuteError::Store(error)) => Ok(error),
        other => Err(Failure(format!(
            "{what}: expected a store failure, got {other:?}"
        ))),
    }
}

/// Checks that `error` is a projector operation failure of the given operation, and returns it.
fn expect_operation(
    error: Error,
    what: &str,
    projector: (&str, usize),
    operation: usize,
    kind: ViewOperationKind,
) -> Result<Box<ProjectorOperationError>, Failure> {
    ensure_not_a_conflict(&error, what)?;

    match error {
        Error::ProjectorOperation(failed) => {
            ensure_eq!(
                (failed.projector.as_str(), failed.projector_index),
                projector,
                format!("{what}: the failed projector")
            );
            ensure_eq!(
                (failed.operation, failed.kind),
                (operation, kind),
                format!("{what}: the failed operation")
            );

            Ok(failed)
        }
        other => Err(Failure(format!(
            "{what}: want a projector operation failure, got {other:?}"
        ))),
    }
}

fn ensure_not_a_conflict(error: &Error, what: &str) -> CaseResult {
    ensure!(
        !error.is_retryable() && !error.is_conflict(),
        "{what}: a projector operation failure is neither a conflict nor retryable: {error:?}"
    );

    Ok(())
}

async fn view_rows<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();
    let ledger = ledger_id();
    let claim = claim_id();
    let (directory_pk, summary_pk) = (format!("{ns}-directory"), format!("{ns}-summary"));

    let projector = directory(&ns);
    let summaries = commit_summary(&ns);
    let store = Store::<Ledger, _>::new(repo, ())
        .with_snapshot_interval(0)
        .with_commit_projector(projector.clone())
        .with_commit_projector(summaries);

    // One commit across two aggregate types writes its rows beside both streams' events, at
    // partitions that are not `{entityType}#{entityId}`.
    let mut tx = store.transaction();
    let mut ledger_read = tx.read::<Ledger>(&ledger).await?;
    let mut claim_read = tx.read::<Claim>(&claim).await?;
    tx.append(&mut ledger_read, added(5))?;
    tx.append(
        &mut claim_read,
        vec![
            Taken {
                holder: "ada".into(),
            }
            .into(),
        ],
    )?;
    let committed = tx.commit(&metadata()).await?;
    ensure_eq!(committed.len(), 2, "both streams committed");
    ensure_eq!(
        projector.calls(),
        1,
        "the projector ran once for the commit"
    );

    let ledger_row = row(target, &directory_pk, ledger.as_str()).await?;
    let ledger_row = ledger_row.ok_or_else(|| Failure("the ledger's directory row".into()))?;
    ensure_eq!(ledger_row.entity_id, ledger.to_string(), "entityID");
    ensure_eq!(ledger_row.entity_type, "ledger".to_owned(), "entityType");
    ensure_eq!(
        ledger_row.payload,
        json::to_vec(&Ledger {
            total: 5,
            entries: 1
        })?,
        "the state after the commit"
    );
    let claim_row = row(target, &directory_pk, claim.as_str()).await?;
    ensure_eq!(
        claim_row.map(|row| (row.entity_type, row.payload)),
        Some(("claim".to_owned(), br#"{"holder":"ada"}"#.to_vec())),
        "the claim's directory row, of the other aggregate type"
    );
    let commit_id = committed[0].metadata.commit_id.clone();
    ensure!(!commit_id.is_empty(), "the events carry the commit ID");
    let summary_row = row(target, &summary_pk, &commit_id).await?;
    ensure_eq!(
        summary_row.map(|row| (
            row.entity_id,
            String::from_utf8_lossy(&row.payload).into_owned()
        )),
        Some((
            ledger.to_string(),
            format!(r#"{{"commitId":"{commit_id}","streams":2}}"#)
        )),
        "the summary row, keyed by the final stamped commit ID, saw both streams and is owned by \
         the first"
    );
    ensure_eq!(
        row(target, &format!("{ns}-width"), claim.as_str())
            .await?
            .map(|row| (
                row.entity_id,
                String::from_utf8_lossy(&row.payload).into_owned()
            )),
        Some((claim.to_string(), r#"{"streams":2}"#.to_owned())),
        "a current-state projector reads every stream's state"
    );

    // A later commit overwrites the rows it touches and leaves the others.
    store.execute(&ledger, Add(2), metadata()).await?;
    ensure_eq!(
        row(target, &directory_pk, ledger.as_str())
            .await?
            .map(|row| row.payload),
        Some(json::to_vec(&Ledger {
            total: 7,
            entries: 2
        })?),
        "the ledger row is overwritten"
    );
    ensure!(
        row(target, &directory_pk, claim.as_str()).await?.is_some(),
        "the claim row is untouched"
    );

    // A later commit deletes a row its stream's earlier commit wrote.
    let retire = {
        let (pk, ledger) = (directory_pk.clone(), ledger.clone());

        Scripted::new("retire", move |_| {
            vec![ViewOperation::delete(pk.clone(), ledger.to_string(), ledger.clone()).if_present()]
        })
    };
    Store::<Ledger, _>::new(repo, ())
        .with_snapshot_interval(0)
        .with_commit_projector(retire)
        .execute(&ledger, Add(1), metadata())
        .await?;
    ensure!(
        row(target, &directory_pk, ledger.as_str()).await?.is_none(),
        "the ledger row was deleted by its owner"
    );
    ensure!(
        row(target, &directory_pk, claim.as_str()).await?.is_some(),
        "the claim row stays"
    );

    // A single-stream commit through the store's other write path projects too.
    let loaded = store.load(&ledger).await?;
    store.commit(loaded, added(1), &metadata()).await?;
    ensure_eq!(
        row(target, &directory_pk, ledger.as_str())
            .await?
            .map(|row| row.payload),
        Some(json::to_vec(&Ledger {
            total: 9,
            entries: 4
        })?),
        "Store::commit projects"
    );

    Ok(())
}

async fn view_conditions<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();
    let (names, audit) = (format!("{ns}-names"), format!("{ns}-audit"));

    // Declaration order: `claims` (index 0) claims the holder's name; `audit` (index 1) records the
    // holder, and for "grace" also deletes a row that was never written.
    let claims = {
        let names = names.clone();

        Scripted::new("claims", move |view| {
            view.streams
                .iter()
                .filter_map(|stream| {
                    let claim = serde_json::from_slice::<Claim>(&stream.state).ok()?;

                    Some((stream.entity_id.clone(), claim.holder?))
                })
                .map(|(owner, holder)| {
                    ViewOperation::put(names.clone(), holder.clone(), owner, "", "{}")
                        .if_absent()
                        .on_conflict(NameTaken(holder))
                })
                .collect()
        })
    };
    let auditor = {
        let audit = audit.clone();

        Scripted::new("audit", move |view| {
            let (owner, holder) = view
                .streams
                .iter()
                .find_map(|stream| {
                    let claim = serde_json::from_slice::<Claim>(&stream.state).ok()?;

                    Some((stream.entity_id.clone(), claim.holder?))
                })
                .unwrap_or_default();

            let mut operations = vec![ViewOperation::put(
                audit.clone(),
                format!("held-{holder}"),
                owner.clone(),
                "",
                "{}",
            )];
            if holder == "grace" {
                operations.push(
                    ViewOperation::delete(audit.clone(), "never-written", owner)
                        .if_present()
                        .on_conflict(NameTaken("never-written".into())),
                );
            }

            operations
        })
    };
    let store = Store::<Claim, _>::new(repo, ())
        .with_snapshot_interval(0)
        .with_commit_projector(claims.clone())
        .with_commit_projector(auditor.clone());

    // The first holder of a name commits with both projectors' rows.
    let first = claim_id();
    store
        .execute(&first, Take("ada".into()), metadata())
        .await?;
    ensure!(
        row(target, &names, "ada").await?.is_some()
            && row(target, &audit, "held-ada").await?.is_some(),
        "both projectors' rows are written"
    );

    // A second claimant of the name fails the first projector's first operation: an absent put.
    let second = claim_id();
    let error = store_failed(
        store.execute(&second, Take("ada".into()), metadata()).await,
        "a taken name",
    )?;
    let failed = expect_operation(
        error,
        "a taken name",
        ("claims", 0),
        0,
        ViewOperationKind::Put,
    )?;
    ensure_eq!(
        failed.reason.as_str(),
        REASON_CONDITION_FAILED,
        "the reason"
    );
    ensure_eq!(
        (failed.pk.as_str(), failed.sk.as_str()),
        (names.as_str(), "ada"),
        "the row"
    );
    let expected_mapped = NameTaken("ada".into());
    ensure_eq!(
        failed
            .mapped()
            .and_then(|mapped| mapped.downcast_ref::<NameTaken>()),
        Some(&expected_mapped),
        "the operation's mapped error"
    );
    ensure!(
        failed
            .source()
            .is_some_and(|source| source.downcast_ref::<NameTaken>().is_some()),
        "source() is the mapped error"
    );
    ensure!(
        repo.events(&second).await?.is_empty(),
        "the refused commit wrote no event"
    );
    ensure!(
        row(target, &audit, "held-ada").await?.is_some(),
        "the earlier commit's rows stand"
    );

    // A failed `present` delete in the second projector's second operation, after both projectors'
    // first operations: nothing of the commit is written.
    let third = claim_id();
    let error = store_failed(
        store
            .execute(&third, Take("grace".into()), metadata())
            .await,
        "a delete of a missing row",
    )?;
    let failed = expect_operation(
        error,
        "a delete of a missing row",
        ("audit", 1),
        1,
        ViewOperationKind::Delete,
    )?;
    ensure_eq!(
        failed.reason.as_str(),
        REASON_CONDITION_FAILED,
        "the reason"
    );
    let expected_mapped = NameTaken("never-written".into());
    ensure_eq!(
        failed
            .source()
            .and_then(|source| source.downcast_ref::<NameTaken>()),
        Some(&expected_mapped),
        "the delete's mapped error"
    );
    for (pk, sk) in [(&names, "grace"), (&audit, "held-grace")] {
        ensure!(
            row(target, pk, sk).await?.is_none(),
            "{sk}: the first operations of the refused commit were not written"
        );
    }
    ensure!(
        repo.events(&third).await?.is_empty(),
        "the refused commit wrote no event"
    );

    // On a store with outcomes the failure is returned at once, records no outcome, and the
    // command is not prepared again.
    let outcomes = Store::<Claim, _>::new(repo, ())
        .with_snapshot_interval(0)
        .with_outcomes(outcome_config())
        .with_commit_projector(claims.clone());
    let calls = claims.calls();
    let command = command_id();
    let result = outcomes
        .execute_with_outcome(&claim_id(), Take("ada".into()), command_metadata(&command))
        .await;
    let error = error_of(result, "a taken name with outcomes")?;
    expect_operation(
        error,
        "a taken name with outcomes",
        ("claims", 0),
        0,
        ViewOperationKind::Put,
    )?;
    ensure_eq!(claims.calls(), calls + 1, "the failed command ran once");
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "an internal failure records no outcome"
    );

    Ok(())
}

/// An absent put of `pk`/`x` that maps its failure to [`NameTaken`], as the first operation of the
/// projector `claims`.
fn claimed(pk: &str, owner: &EntityId) -> ProjectedView {
    ProjectedView::new(
        "claims",
        0,
        0,
        ViewOperation::put(pk, "x", owner.clone(), "", "{}")
            .if_absent()
            .on_conflict(NameTaken("x".into())),
    )
}

async fn view_precedence<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();
    let pk = format!("{ns}-claims");
    let stream = Stream::new(&ns, "s", "ledger");
    let owner = Stream::new(&ns, "owner", "ledger");

    // The owner claims the row; the stream has one event.
    repo.commit(
        Commit::new(owner.id.clone(), owner.events(1..=1)).with_views([claimed(&pk, &owner.id)]),
    )
    .await?;
    stream.commit(repo, stream.events(1..=1), None).await?;

    // Alone, the failing claim is the projector's failure.
    let error = error_of(
        repo.commit(
            Commit::new(stream.id.clone(), stream.events(2..=2))
                .with_views([claimed(&pk, &stream.id)]),
        )
        .await,
        "a failing claim alone",
    )?;
    expect_operation(
        error,
        "a failing claim alone",
        ("claims", 0),
        0,
        ViewOperationKind::Put,
    )?;
    ensure_eq!(
        repo.events(&stream.id).await?.len(),
        1,
        "nothing was written"
    );

    // A stale head (the event sequence already exists) outranks it, as does a gap.
    for (what, events) in [("a stale head", 1..=1), ("a sequence gap", 5..=5)] {
        let error = error_of(
            repo.commit(
                Commit::new(stream.id.clone(), stream.events(events))
                    .with_views([claimed(&pk, &stream.id)]),
            )
            .await,
            what,
        )?;
        ensure!(
            error.is_conflict() && !matches!(error, Error::ProjectorOperation(_)),
            "{what} outranks the failing claim: {error:?}"
        );
    }
    let error = error_of(
        repo.commit_transaction(
            TransactionCommit::new(vec![Commit::new(stream.id.clone(), stream.events(1..=1))])
                .with_views([claimed(&pk, &stream.id)]),
        )
        .await,
        "a stale head in a transaction",
    )?;
    ensure!(
        error.is_conflict() && !matches!(error, Error::ProjectorOperation(_)),
        "a stale head outranks the failing claim in a transaction: {error:?}"
    );

    // A failed read fence outranks it too.
    let fenced = Stream::new(&ns, "fenced", "ledger");
    fenced.commit(repo, fenced.events(1..=1), None).await?;
    let writes = Stream::new(&ns, "writes", "ledger");
    let error = error_of(
        repo.commit_transaction(
            TransactionCommit::new(vec![Commit::new(writes.id.clone(), writes.events(1..=1))])
                .with_fences([ReadFence::new(
                    fenced.id.clone(),
                    fenced.entity_type.clone(),
                    EventSequence(0),
                    FenceProvenance::Engine,
                )])
                .with_views([claimed(&pk, &writes.id)]),
        )
        .await,
        "a failed fence",
    )?;
    ensure!(
        matches!(error, Error::FenceConflict { .. }),
        "a failed fence outranks the failing claim: {error:?}"
    );
    ensure!(
        repo.events(&writes.id).await?.is_empty(),
        "nothing was written"
    );

    // A passing fence leaves the failing claim as the failure.
    let error = error_of(
        repo.commit_transaction(
            TransactionCommit::new(vec![Commit::new(writes.id.clone(), writes.events(1..=1))])
                .with_fences([ReadFence::new(
                    fenced.id.clone(),
                    fenced.entity_type.clone(),
                    EventSequence(1),
                    FenceProvenance::Engine,
                )])
                .with_views([claimed(&pk, &writes.id)]),
        )
        .await,
        "a passing fence",
    )?;
    expect_operation(
        error,
        "a passing fence",
        ("claims", 0),
        0,
        ViewOperationKind::Put,
    )?;

    // A recorded outcome outranks everything, a failing claim and a stale head included.
    let command = command_id();
    let recorder = Stream::new(&ns, "recorder", "ledger");
    repo.commit(
        Commit::new(recorder.id.clone(), recorder.events(1..=1))
            .with_outcome(Outcome::accepted(command.clone(), "digest")),
    )
    .await?;
    for (what, events) in [("a failing claim", 2..=2), ("a stale head", 1..=1)] {
        let error = error_of(
            repo.commit(
                Commit::new(stream.id.clone(), stream.events(events))
                    .with_views([claimed(&pk, &stream.id)])
                    .with_outcome(Outcome::accepted(command.clone(), "digest")),
            )
            .await,
            what,
        )?;
        ensure!(
            matches!(error, Error::OutcomeConflict { .. }),
            "an outcome conflict outranks {what}: {error:?}"
        );
    }
    let error = error_of(
        repo.commit_transaction(
            TransactionCommit::new(vec![Commit::new(stream.id.clone(), stream.events(2..=2))])
                .with_views([claimed(&pk, &stream.id)])
                .with_outcome(Outcome::accepted(command.clone(), "digest")),
        )
        .await,
        "an outcome conflict in a transaction",
    )?;
    ensure!(
        matches!(error, Error::OutcomeConflict { .. }),
        "an outcome conflict outranks a failing claim in a transaction: {error:?}"
    );
    ensure_eq!(
        repo.events(&stream.id).await?.len(),
        1,
        "nothing was written"
    );
    ensure!(
        row(target, &pk, "x").await?.is_some(),
        "the owner's claim stands"
    );

    Ok(())
}

async fn view_projector_only<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();
    let pk = format!("{ns}-orphan");
    let stream = Stream::new(&ns, "s", "ledger");
    let view = || {
        ProjectedView::new(
            "orphan",
            0,
            0,
            ViewOperation::put(&pk, "r", "orphan", "", "{}"),
        )
    };
    let fence = || {
        ReadFence::new(
            stream.id.clone(),
            stream.entity_type.clone(),
            EventSequence(0),
            FenceProvenance::Engine,
        )
    };
    let outcome = || Outcome::accepted(command_id(), "digest");

    let refused = [
        (
            "Repository::commit",
            repo.commit(Commit::new(stream.id.clone(), Vec::new()).with_views([view()]))
                .await,
        ),
        (
            "Repository::commit with an outcome",
            repo.commit(
                Commit::new(stream.id.clone(), Vec::new())
                    .with_views([view()])
                    .with_outcome(outcome()),
            )
            .await,
        ),
        (
            "commit_transaction",
            repo.commit_transaction(TransactionCommit::new(Vec::new()).with_views([view()]))
                .await,
        ),
        (
            "commit_transaction with a fence",
            repo.commit_transaction(
                TransactionCommit::new(Vec::new())
                    .with_fences([fence()])
                    .with_views([view()]),
            )
            .await,
        ),
        (
            "commit_transaction with an outcome",
            repo.commit_transaction(
                TransactionCommit::new(Vec::new())
                    .with_views([view()])
                    .with_outcome(outcome()),
            )
            .await,
        ),
        (
            "commit_transaction with views on an eventless stream",
            repo.commit_transaction(TransactionCommit::new(vec![
                Commit::new(stream.id.clone(), Vec::new()).with_views([view()]),
            ]))
            .await,
        ),
    ];

    for (entry_point, result) in refused {
        ensure!(
            matches!(result, Err(Error::ProjectorWithoutEvents { operations: 1 })),
            "{entry_point}: {result:?}"
        );
    }
    ensure!(
        row(target, &pk, "r").await?.is_none(),
        "no view row was written"
    );

    Ok(())
}

/// A string of `length` bytes that starts with `prefix`.
fn padded(prefix: &str, length: usize) -> String {
    format!("{prefix}{}", "k".repeat(length - prefix.len()))
}

async fn view_checks<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();
    let stream = Stream::new(&ns, "s", "ledger");
    let view = |operation: usize, op: ViewOperation| ProjectedView::new("checks", 0, operation, op);
    let attempt = |views: Vec<ProjectedView>| {
        repo.commit(Commit::new(stream.id.clone(), stream.events(1..=1)).with_views(views))
    };

    // Over the key limits by one byte.
    let error = error_of(
        attempt(vec![view(
            3,
            ViewOperation::put(padded(&ns, 2049), "s", "o", "", "{}"),
        )])
        .await,
        "a pk of 2049 bytes",
    )?;
    ensure!(
        matches!(&error, Error::ViewKeyTooLong { field: "pk", length: 2049, max: 2048, projector, operation: 3 } if projector == "checks"),
        "a pk of 2049 bytes: {error:?}"
    );
    let error = error_of(
        attempt(vec![view(
            0,
            ViewOperation::delete(format!("{ns}-p"), padded("", 1025), "o"),
        )])
        .await,
        "an sk of 1025 bytes",
    )?;
    ensure!(
        matches!(
            &error,
            Error::ViewKeyTooLong {
                field: "sk",
                length: 1025,
                max: 1024,
                ..
            }
        ),
        "an sk of 1025 bytes: {error:?}"
    );

    // Repeating a key, however the operations differ.
    let key = format!("{ns}-dup");
    let error = error_of(
        attempt(vec![
            view(0, ViewOperation::put(&key, "s", "o", "", "{}")),
            view(1, ViewOperation::put(&key, "t", "o", "", "{}")),
            view(2, ViewOperation::delete(&key, "s", "o")),
        ])
        .await,
        "a repeated key",
    )?;
    ensure!(
        matches!(&error, Error::DuplicateViewKey { pk, sk, first, second }
            if *pk == key && sk == "s" && *first == ("checks".to_owned(), 0) && *second == ("checks".to_owned(), 2)),
        "a repeated key: {error:?}"
    );

    // Operations that cannot be stored the same way everywhere.
    let invalid = [
        ("an empty pk", ViewOperation::put("", "s", "o", "", "{}")),
        ("an empty sk", ViewOperation::put(&key, "", "o", "", "{}")),
        (
            "an empty owner",
            ViewOperation::put(&key, "s", "", "", "{}"),
        ),
        (
            "an empty owner on a delete",
            ViewOperation::delete(&key, "s", ""),
        ),
        (
            "U+0000 in a pk",
            ViewOperation::put(format!("{key}\0"), "s", "o", "", "{}"),
        ),
        (
            "U+0000 in an owner",
            ViewOperation::put(&key, "s", "a\0", "", "{}"),
        ),
        (
            "U+0000 in the owner of a delete",
            ViewOperation::delete(&key, "s", "a\0"),
        ),
        (
            "U+0000 in a payload",
            ViewOperation::put(&key, "s", "o", "", "{\0}"),
        ),
        (
            "a payload that is not UTF-8",
            ViewOperation::put(&key, "s", "o", "", vec![0xff, 0xfe]),
        ),
        (
            "a present put",
            ViewOperation::put(&key, "s", "o", "", "{}").if_present(),
        ),
        (
            "an absent delete",
            ViewOperation::delete(&key, "s", "o").if_absent(),
        ),
    ];
    for (what, op) in invalid {
        let error = error_of(attempt(vec![view(0, op)]).await, what)?;
        ensure!(
            matches!(error, Error::InvalidViewOperation(_)),
            "{what}: {error:?}"
        );
    }
    ensure!(
        repo.events(&stream.id).await?.is_empty(),
        "nothing was written by a refused commit"
    );

    // The engine checks a projector's operations when it projects, before any write.
    let long = Scripted::new("long", {
        let ns = ns.clone();

        move |view| {
            vec![ViewOperation::put(
                padded(&ns, 2049),
                "s",
                view.streams[0].entity_id.clone(),
                "",
                "{}",
            )]
        }
    });
    let store = Store::<Ledger, _>::new(repo, ())
        .with_snapshot_interval(0)
        .with_commit_projector(long);
    let id = ledger_id();
    let result = store.execute(&id, Add(1), metadata()).await;
    let error = store_failed(result, "a projector's long key")?;
    ensure!(
        matches!(&error, Error::ViewKeyTooLong { field: "pk", projector, operation: 0, .. } if projector == "long"),
        "a projector's long key: {error:?}"
    );
    ensure!(repo.events(&id).await?.is_empty(), "no event was written");

    // Exactly at the limits is stored and read back, unless the backend bounds keys lower: then a
    // key at DynamoDB's limits is refused before any write, and one at its own bound is stored.
    let (long_pk, long_sk) = (padded(&ns, 2048), padded("", 1024));
    let stored = match target.max_view_key_bytes() {
        None => (long_pk, long_sk),
        Some(max) => {
            let error = error_of(
                attempt(vec![view(
                    0,
                    ViewOperation::put(long_pk.clone(), long_sk.clone(), "o", "", "{}"),
                )])
                .await,
                "a key over the backend's bound",
            )?;
            ensure!(
                matches!(&error, Error::ViewKeyTooLong { max: bound, .. } if *bound == max),
                "a key over the backend's bound: {error:?}"
            );
            ensure!(
                row(target, &long_pk, &long_sk).await?.is_none(),
                "nothing was written"
            );

            (padded(&ns, max - 1_000), padded("", 1_000))
        }
    };
    attempt(vec![view(
        0,
        ViewOperation::put(stored.0.clone(), stored.1.clone(), "o", "", "{}"),
    )])
    .await?;
    ensure!(
        row(target, &stored.0, &stored.1).await?.is_some(),
        "a key at the limits is stored"
    );

    Ok(())
}

fn bulk(pk: &str, count: usize, payload: &str) -> Vec<ProjectedView> {
    (0..count)
        .map(|index| {
            ProjectedView::new(
                "bulk",
                0,
                index,
                ViewOperation::put(pk, format!("row-{index:03}"), "bulk-owner", "", payload),
            )
        })
        .collect()
}

async fn view_budget<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();

    // The two control checks, the head update, and one event leave 96 actions for views.
    let plain = Stream::new(&ns, "plain", "ledger");
    let pk = format!("{ns}-plain");
    let result = repo
        .commit(Commit::new(plain.id.clone(), plain.events(1..=1)).with_views(bulk(&pk, 97, "{}")))
        .await;
    ensure!(
        matches!(&result, Err(Error::CommitTooLarge { actions: 101, .. })),
        "97 views are one action too many: {result:?}"
    );
    ensure!(
        repo.events(&plain.id).await?.is_empty(),
        "no event was written"
    );
    ensure!(
        row(target, &pk, "row-000").await?.is_none(),
        "no view was written"
    );
    repo.commit(Commit::new(plain.id.clone(), plain.events(1..=1)).with_views(bulk(&pk, 96, "{}")))
        .await?;
    ensure!(
        row(target, &pk, "row-095").await?.is_some(),
        "96 views fit with one event"
    );

    // A snapshot put is one more action.
    let snapshotted = Stream::new(&ns, "snapshotted", "ledger");
    let pk = format!("{ns}-snapshotted");
    let result = repo
        .commit(
            Commit::new(snapshotted.id.clone(), snapshotted.events(1..=1))
                .with_snapshot(snapshotted.new_snapshot(1, "{}"))
                .with_views(bulk(&pk, 96, "{}")),
        )
        .await;
    ensure!(
        matches!(&result, Err(Error::CommitTooLarge { actions: 101, .. })),
        "96 views and a snapshot are one action too many: {result:?}"
    );
    repo.commit(
        Commit::new(snapshotted.id.clone(), snapshotted.events(1..=1))
            .with_snapshot(snapshotted.new_snapshot(1, "{}"))
            .with_views(bulk(&pk, 95, "{}")),
    )
    .await?;
    ensure!(
        row(target, &pk, "row-094").await?.is_some(),
        "95 views fit with a snapshot"
    );

    // Two written streams cost two head updates: 94 views fit with one event each.
    let (a, b) = (
        Stream::new(&ns, "a", "ledger"),
        Stream::new(&ns, "b", "ledger"),
    );
    let pk = format!("{ns}-two");
    let transaction = |views: Vec<ProjectedView>| {
        TransactionCommit::new(vec![
            Commit::new(a.id.clone(), a.events(1..=1)),
            Commit::new(b.id.clone(), b.events(1..=1)),
        ])
        .with_views(views)
    };
    let result = repo
        .commit_transaction(transaction(bulk(&pk, 95, "{}")))
        .await;
    ensure!(
        matches!(&result, Err(Error::CommitTooLarge { actions: 101, .. })),
        "95 views are one action too many beside two streams: {result:?}"
    );
    ensure!(repo.events(&a.id).await?.is_empty(), "no event was written");
    repo.commit_transaction(transaction(bulk(&pk, 94, "{}")))
        .await?;
    ensure!(
        row(target, &pk, "row-093").await?.is_some(),
        "94 views fit beside two streams"
    );

    Ok(())
}

async fn view_byte_budget<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();
    let payload = |bytes: usize| format!("\"{}\"", "x".repeat(bytes - 2));

    // Twelve views of 330,000 bytes are individually under 380,000 and together over 3,800,000.
    let stream = Stream::new(&ns, "bytes", "ledger");
    let pk = format!("{ns}-bytes");
    let result = repo
        .commit(
            Commit::new(stream.id.clone(), stream.events(1..=1)).with_views(bulk(
                &pk,
                12,
                &payload(330_000),
            )),
        )
        .await;
    ensure!(
        matches!(&result, Err(Error::CommitTooLarge { actions: 16, bytes, largest_item })
            if *bytes > 3_800_000 && *largest_item < 380_000),
        "the aggregate byte budget: {result:?}"
    );

    // One view over 380,000 bytes.
    let result = repo
        .commit(
            Commit::new(stream.id.clone(), stream.events(1..=1)).with_views(bulk(
                &pk,
                1,
                &payload(400_000),
            )),
        )
        .await;
    ensure!(
        matches!(&result, Err(Error::CommitTooLarge { largest_item, .. }) if *largest_item > 380_000),
        "the single-item byte budget: {result:?}"
    );
    ensure!(
        repo.events(&stream.id).await?.is_empty(),
        "no event was written"
    );
    ensure!(
        row(target, &pk, "row-000").await?.is_none(),
        "no view was written"
    );

    // A few of them fit.
    repo.commit(
        Commit::new(stream.id.clone(), stream.events(1..=1)).with_views(bulk(
            &pk,
            4,
            &payload(330_000),
        )),
    )
    .await?;
    ensure!(
        row(target, &pk, "row-003").await?.is_some(),
        "four 330,000-byte views fit"
    );

    Ok(())
}

async fn view_recompute<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();
    let pk = format!("{ns}-report");

    // A multi-stream command: the failed attempt decided on a stale read, and its operations are
    // derived again with the rest of the commit.
    let watched = ledger_id();
    let destination = ledger_id();
    seed(repo, &watched, 1).await?;

    let report = {
        let (pk, destination) = (pk.clone(), destination.clone());

        Scripted::new("report", move |view| {
            let Some(stream) = view.streams.iter().find(|s| s.entity_id == destination) else {
                return Vec::new();
            };
            let total = serde_json::from_slice::<Ledger>(&stream.state)
                .map(|ledger| ledger.total)
                .unwrap_or_default();

            let owner = stream.entity_id.clone();

            vec![
                ViewOperation::put(
                    pk.clone(),
                    format!("total-{total}"),
                    owner.clone(),
                    "",
                    "{}",
                ),
                ViewOperation::put(pk.clone(), "latest", owner, "", stream.state.clone()),
            ]
        })
    };
    let meddling = Meddling::new(repo, &watched, Trigger::Fences, 1);
    let store = retry_store(&meddling, 3).with_commit_projector(report.clone());
    let copy = CopyCommand::new(&watched, &destination);
    let identity = CommandIdentity::new("ledger:copy").with_targets([destination.clone()]);

    let outcome = store
        .execute_transaction(&identity, &command_metadata(&command_id()), |tx| {
            Box::pin(copy.prepare(tx))
        })
        .await?;
    ensure!(
        outcome.is_accepted(),
        "the re-prepared command committed: {outcome:?}"
    );
    ensure_eq!(copy.calls(), 2, "the command was prepared twice");
    ensure_eq!(report.calls(), 2, "the projector ran once per attempt");
    let sort_keys: Vec<Vec<String>> = report
        .emitted()
        .iter()
        .map(|(_, operations)| operations.iter().map(|op| op.sk.clone()).collect())
        .collect();
    ensure_eq!(
        sort_keys,
        vec![
            vec!["total-1".to_owned(), "latest".to_owned()],
            vec!["total-2".to_owned(), "latest".to_owned()]
        ],
        "each attempt derived its operations from its own state"
    );
    ensure!(
        row(target, &pk, "total-1").await?.is_none(),
        "the failed attempt's operations were not written"
    );
    ensure_eq!(
        row(target, &pk, "latest").await?.map(|row| row.payload),
        Some(json::to_vec(&Ledger {
            total: 2,
            entries: 1
        })?),
        "the rows reflect the final attempt's state"
    );
    ensure!(
        row(target, &pk, "total-2").await?.is_some(),
        "the committing attempt's operations were written"
    );

    // The single-aggregate shortcut re-prepares the same way, and exhausted attempts leave no row.
    let single = ledger_id();
    seed(repo, &single, 1).await?;
    let spk = format!("{ns}-single");
    let totals = {
        let spk = spk.clone();

        Scripted::new("totals", move |view| {
            let total = view
                .streams
                .first()
                .and_then(|stream| serde_json::from_slice::<Ledger>(&stream.state).ok())
                .map(|ledger| ledger.total)
                .unwrap_or_default();

            vec![ViewOperation::put(
                spk.clone(),
                format!("total-{total}"),
                view.streams[0].entity_id.clone(),
                "",
                "{}",
            )]
        })
    };
    let meddling = Meddling::new(repo, &single, Trigger::Events, 1);
    let store = retry_store(&meddling, 3).with_commit_projector(totals.clone());
    let outcome = store
        .execute_with_outcome(&single, Add(1), command_metadata(&command_id()))
        .await?;
    ensure!(outcome.is_accepted(), "the retry committed: {outcome:?}");
    ensure_eq!(totals.calls(), 2, "the projector ran once per attempt");
    ensure!(
        row(target, &spk, "total-2").await?.is_none(),
        "the failed attempt wrote no row"
    );
    ensure!(
        row(target, &spk, "total-3").await?.is_some(),
        "the committing attempt wrote its row (1 + the rival's 1 + 1)"
    );

    let meddling = Meddling::new(repo, &single, Trigger::Events, u32::MAX);
    let store = retry_store(&meddling, 3).with_commit_projector(totals.clone());
    let calls = totals.calls();
    let exhausted = store
        .execute_with_outcome(&single, Add(1), command_metadata(&command_id()))
        .await?;
    ensure!(
        exhausted.is_rejected(),
        "the attempts ran out: {exhausted:?}"
    );
    ensure_eq!(totals.calls(), calls + 3, "one projection per attempt");
    for total in 5..=9 {
        ensure!(
            row(target, &spk, &format!("total-{total}"))
                .await?
                .is_none(),
            "no attempt of an exhausted command wrote total-{total}"
        );
    }

    Ok(())
}

async fn view_rejections<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();
    let pk = format!("{ns}-log");
    let id = ledger_id();

    // One row per written stream, keyed by the commit and listing the commit's event types.
    let log = {
        let pk = pk.clone();

        Scripted::reading("log", &[Wallet::TYPE], move |view| {
            view.streams
                .iter()
                .map(|stream| {
                    let types: Vec<&str> = stream
                        .events
                        .iter()
                        .map(|event| event.event_type.as_str())
                        .collect();

                    ViewOperation::put(
                        pk.clone(),
                        format!("{}-{}", view.commit_id, stream.entity_id),
                        stream.entity_id.clone(),
                        "",
                        types.join(","),
                    )
                })
                .collect()
        })
    };
    let store = wallet_store(repo, 0).with_commit_projector(log.clone());

    let funded = store
        .execute_with_outcome(&id, WalletCommand::Fund(2), command_metadata(&command_id()))
        .await?;
    ensure!(funded.is_accepted(), "the seed commits: {funded:?}");
    ensure_eq!(log.calls(), 1, "an accepted command projects");

    // A rejection that carries events projects them, like an accepted command.
    let rejected = store
        .execute_with_outcome(
            &id,
            WalletCommand::Spend(5),
            command_metadata(&command_id()),
        )
        .await?;
    ensure!(
        rejected.is_rejected(),
        "a rejection with events: {rejected:?}"
    );
    ensure_eq!(log.calls(), 2, "a rejection that carries events projects");
    let commit_id = rejected.events[0].metadata.commit_id.clone();
    ensure_eq!(
        row(target, &pk, &format!("{commit_id}-{id}"))
            .await?
            .map(|row| String::from_utf8_lossy(&row.payload).into_owned()),
        Some("wallet:refused".to_owned()),
        "the rejection's event is projected with its commit ID"
    );

    // A plain rejection writes no event, so it projects nothing.
    let plain = store
        .execute_with_outcome(
            &id,
            WalletCommand::Spend(0),
            command_metadata(&command_id()),
        )
        .await?;
    ensure!(
        plain.is_rejected() && plain.events.is_empty(),
        "a plain rejection: {plain:?}"
    );
    ensure_eq!(log.calls(), 2, "a plain rejection projects nothing");

    // The transaction API: reject_with_events projects, reject and an accepted no-op do not.
    let identity = CommandIdentity::new("wallet:apply").with_targets([id.clone()]);
    let marked = store
        .execute_transaction(&identity, &command_metadata(&command_id()), |tx| {
            Box::pin(refuse_staged(tx, &id, true))
        })
        .await?;
    ensure_eq!(log.calls(), 3, "reject_with_events projects its events");
    let commit_id = marked.events[0].metadata.commit_id.clone();
    ensure!(
        row(target, &pk, &format!("{commit_id}-{id}"))
            .await?
            .is_some(),
        "its row was written"
    );

    store
        .execute_transaction(&identity, &command_metadata(&command_id()), |tx| {
            Box::pin(refuse_staged(tx, &id, false))
        })
        .await?;
    store
        .execute_transaction(&identity, &command_metadata(&command_id()), |tx| {
            Box::pin(async move {
                tx.read::<Wallet>(&ledger_id()).await?;

                Ok(Decision::accept())
            })
        })
        .await?;
    ensure_eq!(
        log.calls(),
        3,
        "a plain reject and an accepted no-op project nothing"
    );

    Ok(())
}

/// The state of an aggregate after the events up to `through`, folded from stored events.
fn fold<A: Aggregate>(
    events: &[SerializedEvent],
    through: EventSequence,
) -> Result<Vec<u8>, Failure> {
    let mut aggregate = A::default();
    for event in events.iter().filter(|event| event.sequence <= through) {
        aggregate.apply(<A::Event as crate::DomainEvent>::from_payload(
            event.event_type.as_str(),
            event.version,
            &event.payload,
        )?);
    }

    Ok(json::to_vec(&aggregate)?)
}

async fn view_determinism<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();
    let (ledger, claim) = (ledger_id(), claim_id());

    let live = directory_reading(&ns);
    let store = Store::<Ledger, _>::new(repo, ())
        .with_snapshot_interval(2)
        .with_commit_projector(live.clone());

    // A two-stream commit, then single-stream commits, one of which writes a snapshot.
    let mut tx = store.transaction();
    let mut ledger_read = tx.read::<Ledger>(&ledger).await?;
    let mut claim_read = tx.read::<Claim>(&claim).await?;
    tx.append(&mut ledger_read, added(5))?;
    tx.append(
        &mut claim_read,
        vec![
            Taken {
                holder: "ada".into(),
            }
            .into(),
        ],
    )?;
    tx.commit(&metadata()).await?;
    store.execute(&ledger, Add(3), metadata()).await?;
    store.execute(&ledger, Add(4), metadata()).await?;

    let recorded = live.emitted();
    ensure_eq!(recorded.len(), 3, "three commits were projected");

    let mut stored = repo.events(&ledger).await?;
    stored.extend(repo.events(&claim).await?);
    let mut groups = group_commits(stored.clone())?;
    ensure_eq!(
        groups.len(),
        3,
        "the stored events regroup into the three commits"
    );

    // The replay runs a fresh projector over each regrouped commit, with the state each stream
    // had after it, through the engine's own visibility and ownership rules.
    let replay: Arc<dyn CommitProjector> = directory_reading(&ns);
    for group in &mut groups {
        for stream in &mut group.streams {
            let events: Vec<SerializedEvent> = stored
                .iter()
                .filter(|event| event.entity_id == stream.entity_id)
                .cloned()
                .collect();
            let through = stream
                .events
                .last()
                .map(|event| event.sequence)
                .unwrap_or_default();

            stream.state = if stream.entity_type.as_str() == Claim::TYPE {
                fold::<Claim>(&events, through)?
            } else {
                fold::<Ledger>(&events, through)?
            };
        }

        let (view, operations) = recorded
            .iter()
            .find(|(view, _)| view.commit_id == group.commit_id)
            .ok_or_else(|| Failure(format!("no live commit {}", group.commit_id)))?;
        ensure_eq!(group, view, "the regrouped view is the live view");

        let replayed: Vec<ViewOperation> = project_commit(std::slice::from_ref(&replay), group)?
            .into_iter()
            .map(|item| item.op)
            .collect();
        ensure_eq!(
            &replayed,
            operations,
            "the replayed operations are the live operations"
        );
    }

    // The rebuilt rows are the rows the live commits left, field for field.
    let rebuilt = replay_views(std::slice::from_ref(&replay), &groups)?;
    ensure_eq!(rebuilt.len(), 5, "two directory rows and three summaries");
    for want in &rebuilt {
        let live = row(target, &want.pk, &want.sk).await?;
        ensure_eq!(
            live.as_ref(),
            Some(want),
            format!("the live row {}/{}", want.pk, want.sk)
        );
    }

    Ok(())
}

async fn view_history_declaration<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();
    let ledger = ledger_id();

    // The projector reads Ledger events, so the store declares the type durably before its first
    // commit. A snapshot after every event makes each one coverable by a compaction.
    let reader = Scripted::reading("counter", &[Ledger::TYPE], {
        let pk = format!("{ns}-counter");

        move |view| {
            view.streams
                .iter()
                .map(|stream| {
                    ViewOperation::put(
                        pk.clone(),
                        stream.entity_id.as_str(),
                        stream.entity_id.clone(),
                        "",
                        stream.events.len().to_string(),
                    )
                })
                .collect()
        }
    });
    let store = Store::<Ledger, _>::new(repo, ())
        .with_snapshot_interval(1)
        .with_commit_projector(reader.clone());

    store.execute(&ledger, Add(1), metadata()).await?;
    store.execute(&ledger, Add(1), metadata()).await?;

    ensure_eq!(
        row(target, &format!("{ns}-counter"), ledger.as_str())
            .await?
            .map(|row| String::from_utf8_lossy(&row.payload).into_owned()),
        Some("1".to_owned()),
        "the projector saw its type's events"
    );

    let error = error_of(
        repo.compact_below(&ledger, EventSequence(1)).await,
        "compaction of a history-dependent type",
    )?;
    ensure!(
        matches!(error, Error::HistoryDependentProjection { .. }),
        "the declaration refuses compaction: {error:?}"
    );
    ensure_eq!(repo.events(&ledger).await?.len(), 2, "nothing was deleted");

    Ok(())
}

async fn view_ownership<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();
    let pk = format!("{ns}-plots");
    let (a, b, c) = (ledger_id(), ledger_id(), ledger_id());

    // Every projector below writes the one row `plots/plot`, owned by the stream it commits.
    let plot = |name: &str, kind: ViewOperationKind, present: bool| {
        let pk = pk.clone();

        Scripted::new(name, move |view| {
            let stream = &view.streams[0];
            let op = match kind {
                ViewOperationKind::Put => ViewOperation::put(
                    pk.clone(),
                    "plot",
                    stream.entity_id.clone(),
                    "ledger",
                    stream.state.clone(),
                ),
                ViewOperationKind::Delete => {
                    ViewOperation::delete(pk.clone(), "plot", stream.entity_id.clone())
                }
            };

            vec![if present { op.if_present() } else { op }]
        })
    };
    let owner_of = |row: Option<ViewRow>| row.map(|row| (row.entity_id, row.payload));
    let state = |total: i64| json::to_vec(&Ledger { total, entries: 1 });

    // A owns the row.
    let putting = plot("put", ViewOperationKind::Put, false);
    store_of(repo, &putting)
        .execute(&a, Add(1), metadata())
        .await?;
    ensure_eq!(
        owner_of(row(target, &pk, "plot").await?),
        Some((a.to_string(), state(1)?)),
        "the first stream owns the row"
    );

    // B can neither overwrite it nor delete it as a present row: a typed failure, nothing written.
    let error = store_failed(
        store_of(repo, &putting)
            .execute(&b, Add(2), metadata())
            .await,
        "another stream's put",
    )?;
    let failed = expect_operation(
        error,
        "another stream's put",
        ("put", 0),
        0,
        ViewOperationKind::Put,
    )?;
    ensure_eq!(
        failed.reason.as_str(),
        REASON_CONDITION_FAILED,
        "the reason"
    );

    let present = plot("present", ViewOperationKind::Delete, true);
    let error = store_failed(
        store_of(repo, &present)
            .execute(&b, Add(2), metadata())
            .await,
        "another stream's present delete",
    )?;
    expect_operation(
        error,
        "another stream's present delete",
        ("present", 0),
        0,
        ViewOperationKind::Delete,
    )?;
    ensure!(repo.events(&b).await?.is_empty(), "B wrote nothing");
    ensure_eq!(
        owner_of(row(target, &pk, "plot").await?),
        Some((a.to_string(), state(1)?)),
        "the row is unchanged"
    );

    // An unconditional delete of another stream's row fails too, though it would succeed on an
    // absent row.
    let blind = plot("blind", ViewOperationKind::Delete, false);
    let error = store_failed(
        store_of(repo, &blind).execute(&c, Add(3), metadata()).await,
        "another stream's blind delete",
    )?;
    expect_operation(
        error,
        "another stream's blind delete",
        ("blind", 0),
        0,
        ViewOperationKind::Delete,
    )?;
    ensure!(repo.events(&c).await?.is_empty(), "C wrote nothing");
    ensure_eq!(
        owner_of(row(target, &pk, "plot").await?),
        Some((a.to_string(), state(1)?)),
        "the row is still A's"
    );

    // The owner overwrites its own row, and deletes it.
    store_of(repo, &putting)
        .execute(&a, Add(2), metadata())
        .await?;
    ensure_eq!(
        owner_of(row(target, &pk, "plot").await?),
        Some((
            a.to_string(),
            json::to_vec(&Ledger {
                total: 3,
                entries: 2
            })?
        )),
        "the owner overwrote its row"
    );
    store_of(repo, &present)
        .execute(&a, Add(1), metadata())
        .await?;
    ensure!(
        row(target, &pk, "plot").await?.is_none(),
        "the owner deleted its row"
    );

    // Once the row is gone, any stream may write the key.
    store_of(repo, &putting)
        .execute(&b, Add(2), metadata())
        .await?;
    ensure_eq!(
        owner_of(row(target, &pk, "plot").await?).map(|(owner, _)| owner),
        Some(b.to_string()),
        "an absent row is claimed by whoever writes it"
    );

    Ok(())
}

/// A store of ledgers over `repo` with one commit-wide projector.
fn store_of<'r, R: Repository>(repo: &'r R, projector: &Arc<Scripted>) -> Store<Ledger, &'r R> {
    Store::new(repo, ())
        .with_snapshot_interval(0)
        .with_commit_projector(projector.clone())
}

async fn view_visibility<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();
    let (ledger, claim) = (ledger_id(), claim_id());

    // Two projectors that write nothing and record what they were given.
    let current = Scripted::new("current", |_| Vec::new());
    let reading = Scripted::reading("reading", &[Ledger::TYPE], |_| Vec::new());
    let store = Store::<Ledger, _>::new(repo, ())
        .with_snapshot_interval(0)
        .with_commit_projector(current.clone())
        .with_commit_projector(reading.clone());

    let mut tx = store.transaction();
    let mut ledger_read = tx.read::<Ledger>(&ledger).await?;
    let mut claim_read = tx.read::<Claim>(&claim).await?;
    tx.append(&mut ledger_read, added(5))?;
    tx.append(
        &mut claim_read,
        vec![
            Taken {
                holder: "ada".into(),
            }
            .into(),
        ],
    )?;
    let committed = tx.commit(&metadata()).await?;
    let commit_id = committed[0].metadata.commit_id.clone();

    // A current-state projector sees no commit ID and no events, and every stream's state.
    let seen = current.emitted();
    let [(view, _)] = &seen[..] else {
        return Err(Failure(format!("one commit was projected: {seen:?}")));
    };
    ensure_eq!(
        view.commit_id.as_str(),
        "",
        "a current-state projector sees no commit ID"
    );
    ensure_eq!(view.streams.len(), 2, "it sees both streams");
    ensure!(
        view.streams
            .iter()
            .all(|stream| stream.events.is_empty() && !stream.state.is_empty()),
        "no events, every state: {view:?}"
    );

    // A history-dependent projector sees the commit ID, and events and state only of its types.
    let seen = reading.emitted();
    let [(view, _)] = &seen[..] else {
        return Err(Failure(format!("one commit was projected: {seen:?}")));
    };
    ensure_eq!(
        view.commit_id,
        commit_id,
        "a history projector sees the commit ID"
    );
    let by_id = |id: &EntityId| view.streams.iter().find(|stream| &stream.entity_id == id);
    let declared = by_id(&ledger).ok_or_else(|| Failure("the ledger stream".into()))?;
    ensure!(
        declared.events.len() == 1 && !declared.state.is_empty(),
        "a declared type's events and state: {declared:?}"
    );
    let undeclared = by_id(&claim).ok_or_else(|| Failure("the claim stream".into()))?;
    ensure!(
        undeclared.events.is_empty() && undeclared.state.is_empty(),
        "an undeclared type's events and state are hidden: {undeclared:?}"
    );

    // A history projector may not own a row by a stream of a type it did not declare, and no
    // projector by a stream the commit did not write. Nothing of either commit is written.
    let pk = format!("{ns}-owned");
    let claim_owner = {
        let (pk, claim) = (pk.clone(), claim.clone());

        Scripted::reading("claim-owner", &[Ledger::TYPE], move |_| {
            vec![ViewOperation::put(
                pk.clone(),
                "row",
                claim.clone(),
                "",
                "{}",
            )]
        })
    };
    let ghost_owner = {
        let pk = pk.clone();

        Scripted::new("ghost-owner", move |_| {
            vec![ViewOperation::put(pk.clone(), "row", "ghost", "", "{}")]
        })
    };
    for (what, projector) in [
        ("an owner of an undeclared type", claim_owner),
        ("an owner the commit did not write", ghost_owner),
    ] {
        let store = Store::<Ledger, _>::new(repo, ())
            .with_snapshot_interval(0)
            .with_commit_projector(projector);
        let (other_ledger, other_claim) = (ledger_id(), claim_id());

        let mut tx = store.transaction();
        let mut ledger_read = tx.read::<Ledger>(&other_ledger).await?;
        let mut claim_read = tx.read::<Claim>(&other_claim).await?;
        tx.append(&mut ledger_read, added(1))?;
        tx.append(
            &mut claim_read,
            vec![
                Taken {
                    holder: "grace".into(),
                }
                .into(),
            ],
        )?;
        let error = error_of(tx.commit(&metadata()).await, what)?;
        ensure!(
            matches!(&error, Error::InvalidViewOperation(message) if message.contains("owner")),
            "{what}: {error:?}"
        );
        ensure!(
            repo.events(&other_ledger).await?.is_empty()
                && repo.events(&other_claim).await?.is_empty(),
            "{what}: nothing was written"
        );
    }
    ensure!(
        row(target, &pk, "row").await?.is_none(),
        "no refused row was written"
    );

    Ok(())
}

/// A projector that always fails.
struct Failing {
    calls: AtomicUsize,
}

impl CommitProjector for Failing {
    fn name(&self) -> &str {
        "failing"
    }

    fn project(&self, _: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
        self.calls.fetch_add(1, Ordering::SeqCst);

        Err(Box::new(NameTaken("projector".into())))
    }
}

async fn view_projector_failure<R, T>(repo: &R, _target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let failing = Arc::new(Failing {
        calls: AtomicUsize::new(0),
    });
    let id = ledger_id();

    let plain = Store::<Ledger, _>::new(repo, ())
        .with_snapshot_interval(0)
        .with_commit_projector(failing.clone());
    let error = store_failed(
        plain.execute(&id, Add(1), metadata()).await,
        "a failing projector",
    )?;
    ensure!(
        matches!(&error, Error::Projector(_)) && error.to_string().contains("failing"),
        "the failure names the projector: {error:?}"
    );
    ensure!(!error.is_retryable(), "it is not retryable");

    let outcomes = Store::<Ledger, _>::new(repo, ())
        .with_snapshot_interval(0)
        .with_outcomes(outcome_config())
        .with_commit_projector(failing.clone());
    let command = command_id();
    let error = error_of(
        outcomes
            .execute_with_outcome(&id, Add(1), command_metadata(&command))
            .await,
        "a failing projector with outcomes",
    )?;
    ensure!(matches!(&error, Error::Projector(_)), "{error:?}");
    ensure_eq!(
        failing.calls.load(Ordering::SeqCst),
        2,
        "once per command, never retried"
    );
    ensure!(repo.events(&id).await?.is_empty(), "nothing was written");
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "an internal failure records no outcome"
    );

    Ok(())
}

async fn view_reserved_partitions<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    for prefix in crate::RESERVED_VIEW_PREFIXES {
        let id = ledger_id();
        let pk = format!("{prefix}{id}");
        let (key, owner) = (pk.clone(), id.clone());
        let projector = Scripted::new("sneaky", move |_| {
            vec![
                ViewOperation::put(format!("{owner}-allowed"), "VIEW", owner.clone(), "t", "{}"),
                ViewOperation::put(
                    key.clone(),
                    "00000000000000000001",
                    owner.clone(),
                    "journal",
                    "{}",
                ),
            ]
        });

        let error = store_failed(
            Store::<Ledger, _>::new(repo, ())
                .with_snapshot_interval(0)
                .with_commit_projector(projector)
                .execute(&id, Add(1), metadata())
                .await,
            &format!("a projector writing {pk}"),
        )?;
        ensure!(
            matches!(
                &error,
                Error::ReservedViewKey { pk: refused, projector, operation: 1 }
                    if refused == &pk && projector == "sneaky"
            ),
            "the refusal names the key, the projector and the operation: {error:?}"
        );
        ensure!(!error.is_retryable(), "it is not retryable");
        ensure!(repo.events(&id).await?.is_empty(), "no event was written");
        ensure!(
            row(target, &format!("{id}-allowed"), "VIEW")
                .await?
                .is_none(),
            "no row was written"
        );
    }

    Ok(())
}

/// Runs the journal counter's condition (an expected payload, ADR 0005) through a repository that
/// says it applies it ([`Repository::applies_expected_payload`]): a put that expects the payload it
/// read replaces exactly that row, and fails as a condition when the row moved, is gone, or belongs
/// to another stream. The condition is crate-private, so only the engine can set it; these cases
/// set it directly on committed view operations. Like [`commit_view_cases`], it panics after
/// running every case if any failed.
pub async fn expected_payload_cases<R, T>(repo: &R, target: &T)
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    assert!(
        repo.applies_expected_payload(),
        "the repository applies the expected payload"
    );

    if let Err(Failure(message)) = Box::pin(view_expected_payload(repo, target)).await {
        println!("FAIL an expected payload replaces exactly the row the commit read");
        panic!("expected-payload case failed: {message}");
    }

    println!("ok   an expected payload replaces exactly the row the commit read");
}

async fn view_expected_payload<R, T>(repo: &R, target: &T) -> CaseResult
where
    R: Repository,
    T: CommitViewTarget<R>,
{
    let ns = namespace();
    let (owner, other) = (
        EntityId::new(format!("{ns}-owner")),
        EntityId::new(format!("{ns}-other")),
    );
    let (pk, sk) = (format!("{ns}-counter"), "COUNTER".to_owned());
    let event = |id: &EntityId, sequence: i64| SerializedEvent {
        id: EventId::for_event(id, EventSequence(sequence)),
        sequence: EventSequence(sequence),
        event_type: EventType::new("counter:bumped"),
        version: EventVersion(1),
        entity_id: id.clone(),
        entity_type: "counter".into(),
        payload: b"{}".to_vec(),
        metadata: Metadata::new("local"),
    };
    let put = |owner: &EntityId, payload: &str, expected: Option<&str>| {
        let mut op = ViewOperation::put(
            pk.clone(),
            sk.clone(),
            owner.clone(),
            "journal-counter",
            payload,
        );
        match expected {
            Some(expected) => op.expected_payload = Some(expected.as_bytes().to_vec()),
            None => op = op.if_absent(),
        }

        ProjectedView::new("counter", 0, 0, op)
    };
    let commit = |id: &EntityId, sequence: i64, view: ProjectedView| {
        repo.commit(Commit::new(id.clone(), vec![event(id, sequence)]).with_views([view]))
    };
    let payload =
        |row: Option<ViewRow>| row.map(|row| String::from_utf8(row.payload).unwrap_or_default());
    let condition_failed = |result: Result<(), Error>, label: &str| match result {
        Err(Error::ProjectorOperation(failure)) if failure.reason == REASON_CONDITION_FAILED => {
            Ok(())
        }
        other => Err(Failure(format!(
            "{label}: expected a failed condition, got {other:?}"
        ))),
    };

    // The first counter is claimed with an absent put, as when the commit read none.
    commit(&owner, 1, put(&owner, r#"{"n":1}"#, None)).await?;
    ensure_eq!(
        payload(row(target, &pk, &sk).await?),
        Some(r#"{"n":1}"#.to_owned()),
        "claimed"
    );

    // Expecting what is stored replaces it.
    commit(&owner, 2, put(&owner, r#"{"n":2}"#, Some(r#"{"n":1}"#))).await?;
    ensure_eq!(
        payload(row(target, &pk, &sk).await?),
        Some(r#"{"n":2}"#.to_owned()),
        "replaced"
    );

    // A stale expectation fails, and writes nothing: no event, no row change.
    condition_failed(
        commit(&owner, 3, put(&owner, r#"{"n":3}"#, Some(r#"{"n":1}"#))).await,
        "a stale payload",
    )?;
    ensure_eq!(
        payload(row(target, &pk, &sk).await?),
        Some(r#"{"n":2}"#.to_owned()),
        "unchanged"
    );
    ensure_eq!(
        repo.events(&owner).await?.len(),
        2,
        "the failed commit wrote no event"
    );

    // Another stream cannot replace the row, even with the right payload.
    condition_failed(
        commit(&other, 1, put(&other, r#"{"n":3}"#, Some(r#"{"n":2}"#))).await,
        "another owner",
    )?;

    // An expectation on a row that is not there fails.
    let missing = format!("{ns}-missing");
    let mut op = ViewOperation::put(
        missing.clone(),
        sk.clone(),
        owner.clone(),
        "journal-counter",
        "{}",
    );
    op.expected_payload = Some(b"{}".to_vec());
    condition_failed(
        commit(&owner, 3, ProjectedView::new("counter", 0, 0, op)).await,
        "an absent row",
    )?;
    ensure!(
        row(target, &missing, &sk).await?.is_none(),
        "nothing was created"
    );

    Ok(())
}

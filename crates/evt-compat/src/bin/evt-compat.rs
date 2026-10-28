//! The Rust driver for the cross-language integration run (`scripts/compat-integration.sh`).
//!
//! ```text
//! evt-compat steps     [--only NAME]
//! evt-compat exclusive
//! evt-compat step      --backend dynamo|postgres --run RUN --index N [--only NAME]
//! evt-compat verify    --backend dynamo|postgres --run RUN
//! evt-compat enumerate --backend dynamo|postgres --run RUN
//! ```
//!
//! `step` executes step N of every interleaved scenario whose writer is `rust`, against streams the
//! Go driver also writes to. A scenario marked exclusive holds store-wide state across its steps (a
//! rebuild lock refuses every other scenario's commits), so the interleaved run skips it and
//! `--only NAME` runs that scenario alone; `steps` prints the step count of the interleaved
//! scenarios, or of the named one, and `exclusive` lists the exclusive scenarios. `verify` checks every stream against the golden fixtures: the raw stored items
//! or rows, the events and snapshot read through the repository, and the loaded state. `enumerate`
//! prints the run's events as `stream_all_events` yields them (unfiltered and filtered by entity
//! type), one sorted line each, for the integration script to compare with the Go driver's output.

use std::process::ExitCode;

use evt_compat::drivers::{self, DynamoHeads, DynamoViews, PostgresHeads, PostgresViews};
use evt_compat::scenario::{Expectation, WRITER};
use evt_compat::{HeadAccess, RebuildDriver, Runner, ScenarioFixtures, Scenarios, ViewAccess};
use futures_core::Stream;
use photon_evt::dynamodb::DynamoRepository;
use photon_evt::postgres::PostgresRepository;
use photon_evt::wire::dynamo;
use photon_evt::wire::postgres::{EventRow, OutcomeRow, SnapshotRow};
use photon_evt::{
    CommandId, EntityId, EventLog, Repository, SerializedEvent, SerializedSnapshot, StreamFilter,
};

type Failures = Vec<String>;
type BoxError = Box<dyn std::error::Error>;

#[derive(Clone)]
enum Backend {
    Dynamo(DynamoRepository),
    Postgres(PostgresRepository),
}

struct Args {
    command: String,
    backend: String,
    run: String,
    index: usize,
    /// The one exclusive scenario to run, when set.
    only: String,
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let command = args
        .next()
        .ok_or("missing command: steps | exclusive | step | verify | enumerate")?;
    let mut parsed = Args {
        command,
        backend: String::new(),
        run: String::new(),
        index: 0,
        only: String::new(),
    };

    while let Some(flag) = args.next() {
        let value = args.next().ok_or(format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--backend" => parsed.backend = value,
            "--run" => parsed.run = value,
            "--only" => parsed.only = value,
            "--index" => {
                parsed.index = value
                    .parse()
                    .map_err(|_| format!("invalid index {value}"))?
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }

    Ok(parsed)
}

async fn connect(backend: &str) -> Result<Backend, BoxError> {
    match backend {
        "dynamo" => Ok(Backend::Dynamo(drivers::dynamo().await)),
        "postgres" => Ok(Backend::Postgres(
            drivers::postgres().await.map_err(|err| err as BoxError)?,
        )),
        other => Err(format!("unknown backend {other}").into()),
    }
}

/// Reports whether a step run takes the scenario: the interleaved run takes every scenario that is
/// not exclusive, and a run with `--only` that one exclusive scenario alone.
fn selected(scenario: &evt_compat::Scenario, only: &str) -> bool {
    if only.is_empty() {
        return !scenario.exclusive;
    }

    scenario.exclusive && scenario.name == only
}

async fn step<R, H, V, F>(
    repository: R,
    access: (H, V, F),
    run: &str,
    index: usize,
    only: &str,
) -> Result<Failures, BoxError>
where
    R: Repository + EventLog,
    H: HeadAccess + Clone + 'static,
    V: ViewAccess + Clone + 'static,
    F: photon_evt::RebuildFence + photon_evt::FeatureMarker + Clone + 'static,
{
    let (heads, views, fence) = access;
    let scenarios = Scenarios::load()?;
    let mut failures = Vec::new();

    for scenario in scenarios.scenarios.iter().filter(|s| selected(s, only)) {
        let Some(step) = scenario.steps.get(index).filter(|step| step.by == WRITER) else {
            continue;
        };

        // A live `wait` really sleeps.
        let runner = Runner::for_scenario(&repository, scenario, run)
            .with_head_access(heads.clone())
            .with_view_access(views.clone())
            .with_rebuild_access(RebuildDriver::sleeping(fence.clone()))
            .with_marker_access(fence.clone());
        let outcome = runner.execute(&scenario.entity_id(run), step).await?;

        if outcome == step.expect {
            println!("rust step {index} {}: {}", scenario.name, outcome.result);
        } else {
            failures.push(format!(
                "{} step {index}: got {outcome:?}, want {:?}",
                scenario.name, step.expect
            ));
        }
    }

    Ok(failures)
}

fn check<T: PartialEq + std::fmt::Debug>(
    failures: &mut Failures,
    scenario: &str,
    what: &str,
    got: T,
    want: T,
) {
    if got != want {
        failures.push(format!(
            "{scenario}: {what}\n  got:  {got:?}\n  want: {want:?}"
        ));
    }
}

async fn verify_common<R: Repository + EventLog>(
    repository: &R,
    heads: impl HeadAccess + 'static,
    expected: &Expectation,
    scenario: &evt_compat::Scenario,
    views: impl ViewAccess + 'static,
    failures: &mut Failures,
) -> Result<(), BoxError> {
    let snapshot_interval = scenario.snapshot_interval;
    let runner = Runner::new(repository, snapshot_interval)
        .with_head_access(heads)
        .with_view_access(views);
    let observed = runner
        .observe(&expected.name, &EntityId::new(&expected.entity_id))
        .await?;
    let name = &expected.name;

    check(failures, name, "state", &observed.state, &expected.state);
    check(
        failures,
        name,
        "sequence",
        observed.sequence,
        expected.sequence,
    );
    check(
        failures,
        name,
        "snapshot sequence",
        observed.snapshot_sequence,
        expected.snapshot_sequence,
    );
    check(failures, name, "events", &observed.events, &expected.events);
    check(
        failures,
        name,
        "snapshot",
        &observed.snapshot,
        &expected.snapshot,
    );
    check(
        failures,
        name,
        "sns messages",
        &observed.sns_messages,
        &expected.sns_messages,
    );
    // The stream's head: its `sk = -1` item on DynamoDB, its row in the heads table on PostgreSQL.
    check(failures, name, "head", observed.head, expected.head);

    // The view rows commit-wide projectors left in the scenario's partitions, filtered to the rows
    // this run owns (a live table is shared between runs): the raw items on DynamoDB, the rows on
    // PostgreSQL. A scenario with no view partitions is skipped.
    let entity_id = EntityId::new(&expected.entity_id);
    if !scenario.view_partitions(&entity_id).is_empty() {
        let observed = runner.observe_views(scenario, &entity_id).await?;

        check(failures, name, "view rows", &observed, &expected.views);
    }

    // Every other stream the scenario touches: written siblings and fenced read-only streams.
    let sibling_ids: Vec<EntityId> = expected
        .siblings
        .iter()
        .map(|sibling| EntityId::new(&sibling.entity_id))
        .collect();
    let observed_siblings = runner.observe_siblings(&sibling_ids).await?;
    check(
        failures,
        name,
        "sibling streams",
        observed_siblings.len(),
        expected.siblings.len(),
    );

    for (observed, wanted) in observed_siblings.iter().zip(&expected.siblings) {
        let what = |part: &str| format!("sibling {} {part}", wanted.entity_id);

        check(
            failures,
            name,
            &what("events"),
            &observed.events,
            &wanted.events,
        );
        check(
            failures,
            name,
            &what("snapshot"),
            &observed.snapshot,
            &wanted.snapshot,
        );
        check(failures, name, &what("head"), observed.head, wanted.head);
    }

    Ok(())
}

fn expected_rows(
    events: &[String],
    snapshot: &Option<String>,
) -> Result<(Vec<EventRow>, Option<SnapshotRow>), BoxError> {
    let events = events
        .iter()
        .map(|json| {
            Ok(EventRow::from_event(&SerializedEvent::from_json(
                json.as_bytes(),
            )?)?)
        })
        .collect::<Result<Vec<_>, BoxError>>()?;
    let snapshot = snapshot
        .as_ref()
        .map(|json| {
            SerializedSnapshot::from_json(json.as_bytes()).map(|s| SnapshotRow::from_snapshot(&s))
        })
        .transpose()?;

    Ok((events, snapshot))
}

/// Compares rows with metadata parsed as JSON: PostgreSQL stores it as jsonb, which normalizes
/// whitespace and key order.
fn comparable(rows: Vec<EventRow>) -> Result<Vec<(EventRow, serde_json::Value)>, BoxError> {
    rows.into_iter()
        .map(|mut row| {
            let metadata = serde_json::from_str(&row.metadata)?;
            row.metadata.clear();
            Ok((row, metadata))
        })
        .collect()
}

/// Compares an outcome row with its JSON columns parsed: PostgreSQL stores `events` and
/// `rejection` as jsonb, which normalizes whitespace and key order.
type ComparableOutcome = (
    String,
    String,
    String,
    Option<Vec<u8>>,
    serde_json::Value,
    Option<serde_json::Value>,
    Option<String>,
);

fn comparable_outcome(row: OutcomeRow) -> Result<ComparableOutcome, BoxError> {
    Ok((
        row.command_id,
        row.status,
        row.digest,
        row.response,
        serde_json::from_str(&row.events)?,
        row.rejection
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        row.code,
    ))
}

async fn verify(backend: Backend, run: &str) -> Result<Failures, BoxError> {
    let scenarios = Scenarios::load()?;
    let fixtures = ScenarioFixtures::load()?;
    let mut failures = Vec::new();

    for scenario in &scenarios.scenarios {
        let expected = fixtures
            .for_run(&scenario.name, run)
            .ok_or_else(|| format!("no fixture for {}", scenario.name))?;
        let entity_id = scenario.entity_id(run);
        let before = failures.len();

        match &backend {
            Backend::Dynamo(repository) => {
                verify_common(
                    repository,
                    DynamoHeads::of(repository),
                    &expected,
                    scenario,
                    DynamoViews::of(repository),
                    &mut failures,
                )
                .await?;

                let items = repository
                    .partition_items(&entity_id)
                    .await?
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<_>, _>>()?;
                check(
                    &mut failures,
                    &scenario.name,
                    "dynamo items",
                    &items,
                    &expected.dynamo_items,
                );

                // Every outcome row the scenario leaves behind: the stored item must be the one Go
                // stored, byte for byte.
                for outcome in &expected.outcomes {
                    let items = repository
                        .partition_items(&EntityId::new(dynamo::outcome_key(&CommandId::new(
                            &outcome.command_id,
                        ))))
                        .await?
                        .into_iter()
                        .map(serde_json::to_value)
                        .collect::<Result<Vec<_>, _>>()?;
                    check(
                        &mut failures,
                        &scenario.name,
                        &format!("outcome {} dynamo item", outcome.command_id),
                        &items,
                        &vec![outcome.dynamo_item.clone()],
                    );
                }

                for sibling in &expected.siblings {
                    let items = repository
                        .partition_items(&EntityId::new(&sibling.entity_id))
                        .await?
                        .into_iter()
                        .map(serde_json::to_value)
                        .collect::<Result<Vec<_>, _>>()?;
                    check(
                        &mut failures,
                        &scenario.name,
                        &format!("sibling {} dynamo items", sibling.entity_id),
                        &items,
                        &sibling.dynamo_items,
                    );
                }
            }
            Backend::Postgres(repository) => {
                verify_common(
                    repository,
                    PostgresHeads::default_tables(repository.pool().clone()),
                    &expected,
                    scenario,
                    PostgresViews::new(repository.pool().clone()),
                    &mut failures,
                )
                .await?;

                let (want_events, want_snapshot) =
                    expected_rows(&expected.events, &expected.snapshot)?;
                let rows = comparable(repository.event_rows(&entity_id).await?)?;
                check(
                    &mut failures,
                    &scenario.name,
                    "event rows",
                    rows,
                    comparable(want_events)?,
                );

                let snapshot = repository.snapshot_row(&entity_id).await?;
                check(
                    &mut failures,
                    &scenario.name,
                    "snapshot row",
                    snapshot,
                    want_snapshot,
                );

                // Every outcome row the scenario leaves behind, compared field by field with the
                // row Go's item encodes (jsonb columns as parsed JSON).
                for outcome in &expected.outcomes {
                    let item: dynamo::Item = serde_json::from_value(outcome.dynamo_item.clone())?;
                    let want = OutcomeRow::from_outcome(&dynamo::outcome_from_item(&item)?)?;
                    let got = repository
                        .outcome_row(&CommandId::new(&outcome.command_id))
                        .await?;
                    check(
                        &mut failures,
                        &scenario.name,
                        &format!("outcome {} row", outcome.command_id),
                        got.map(comparable_outcome).transpose()?,
                        Some(comparable_outcome(want)?),
                    );
                }

                for sibling in &expected.siblings {
                    let sibling_id = EntityId::new(&sibling.entity_id);
                    let (want_events, want_snapshot) =
                        expected_rows(&sibling.events, &sibling.snapshot)?;
                    check(
                        &mut failures,
                        &scenario.name,
                        &format!("sibling {} event rows", sibling.entity_id),
                        comparable(repository.event_rows(&sibling_id).await?)?,
                        comparable(want_events)?,
                    );
                    check(
                        &mut failures,
                        &scenario.name,
                        &format!("sibling {} snapshot row", sibling.entity_id),
                        repository.snapshot_row(&sibling_id).await?,
                        want_snapshot,
                    );
                }
            }
        }

        if failures.len() == before {
            println!("rust verified {}", scenario.name);
        }
    }

    Ok(failures)
}

/// Prints every event of the run that `stream_all_events` yields, once unfiltered and once
/// narrowed to the account entity type, as sorted `section entity sequence type` lines.
async fn enumerate<R: EventLog>(repository: &R, run: &str) -> Result<Failures, BoxError> {
    let mut lines = Vec::new();

    for (section, filter) in [
        ("all", StreamFilter::all()),
        ("typed", StreamFilter::of_type("account")),
    ] {
        let stream = repository.stream_all_events(filter);
        let mut stream = std::pin::pin!(stream);

        while let Some(batch) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
            for event in batch? {
                if event.entity_id.as_str().contains(run) {
                    lines.push(format!(
                        "{section} {} {} {}",
                        event.entity_id, event.sequence, event.event_type
                    ));
                }
            }
        }
    }

    lines.sort();
    for line in lines {
        println!("{line}");
    }

    Ok(Vec::new())
}

async fn run(args: Args) -> Result<Failures, BoxError> {
    if args.command == "steps" {
        let steps = Scenarios::load()?
            .scenarios
            .iter()
            .filter(|scenario| selected(scenario, &args.only))
            .map(|scenario| scenario.steps.len())
            .max()
            .unwrap_or_default();
        println!("{steps}");

        return Ok(Vec::new());
    }

    if args.command == "exclusive" {
        for scenario in Scenarios::load()?.scenarios {
            if scenario.exclusive {
                println!("{}", scenario.name);
            }
        }

        return Ok(Vec::new());
    }

    if args.run.is_empty() {
        return Err("--run is required".into());
    }

    let backend = connect(&args.backend).await?;

    match (args.command.as_str(), backend) {
        ("step", Backend::Dynamo(repository)) => {
            let access = (
                DynamoHeads::of(&repository),
                DynamoViews::of(&repository),
                repository.clone(),
            );

            step(repository, access, &args.run, args.index, &args.only).await
        }
        ("step", Backend::Postgres(repository)) => {
            let access = (
                PostgresHeads::default_tables(repository.pool().clone()),
                PostgresViews::new(repository.pool().clone()),
                repository.clone(),
            );

            step(repository, access, &args.run, args.index, &args.only).await
        }
        ("verify", backend) => verify(backend, &args.run).await,
        ("enumerate", Backend::Dynamo(repository)) => enumerate(&repository, &args.run).await,
        ("enumerate", Backend::Postgres(repository)) => enumerate(&repository, &args.run).await,
        (other, _) => Err(format!("unknown command {other}").into()),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(err) => {
            eprintln!("evt-compat: {err}");
            return ExitCode::from(2);
        }
    };

    match run(args).await {
        Ok(failures) if failures.is_empty() => ExitCode::SUCCESS,
        Ok(failures) => {
            for failure in failures {
                eprintln!("FAIL {failure}");
            }
            ExitCode::FAILURE
        }
        Err(err) => {
            eprintln!("evt-compat: {err}");
            ExitCode::FAILURE
        }
    }
}

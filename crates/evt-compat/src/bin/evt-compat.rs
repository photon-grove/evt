//! The Rust driver for the cross-language integration run (`scripts/compat-integration.sh`).
//!
//! ```text
//! evt-compat steps
//! evt-compat step   --backend dynamo|postgres --run RUN --index N
//! evt-compat verify --backend dynamo|postgres --run RUN
//! ```
//!
//! `step` executes step N of every scenario whose writer is `rust`, against streams the Go driver
//! also writes to. `verify` checks every stream against the golden fixtures: the raw stored items
//! or rows, the events and snapshot read through the repository, and the loaded state.

use std::process::ExitCode;

use evt_compat::drivers;
use evt_compat::scenario::{Expectation, WRITER};
use evt_compat::{Runner, ScenarioFixtures, Scenarios};
use photon_evt::dynamodb::DynamoRepository;
use photon_evt::postgres::PostgresRepository;
use photon_evt::wire::postgres::{EventRow, SnapshotRow};
use photon_evt::{EntityId, Repository, SerializedEvent, SerializedSnapshot};

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
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let command = args
        .next()
        .ok_or("missing command: steps | step | verify")?;
    let mut parsed = Args {
        command,
        backend: String::new(),
        run: String::new(),
        index: 0,
    };

    while let Some(flag) = args.next() {
        let value = args.next().ok_or(format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--backend" => parsed.backend = value,
            "--run" => parsed.run = value,
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

async fn step<R: Repository>(repository: R, run: &str, index: usize) -> Result<Failures, BoxError> {
    let scenarios = Scenarios::load()?;
    let mut failures = Vec::new();

    for scenario in &scenarios.scenarios {
        let Some(step) = scenario.steps.get(index).filter(|step| step.by == WRITER) else {
            continue;
        };

        let runner = Runner::new(&repository, scenario.snapshot_interval);
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

async fn verify_common<R: Repository>(
    repository: &R,
    expected: &Expectation,
    snapshot_interval: usize,
    failures: &mut Failures,
) -> Result<(), BoxError> {
    let runner = Runner::new(repository, snapshot_interval);
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

    if let Some(expected_sibling) = &expected.sibling {
        let sibling_id = EntityId::new(&expected_sibling.entity_id);
        let observed_events = repository.events(&sibling_id).await?;
        let observed_snapshot = repository.snapshot(&sibling_id).await?;

        let observed_events: Vec<String> = observed_events
            .iter()
            .map(|event| {
                event
                    .to_json()
                    .map(|bytes| String::from_utf8(bytes).expect("JSON encoders write UTF-8"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let observed_snapshot = observed_snapshot
            .as_ref()
            .map(|snapshot| {
                snapshot
                    .to_json()
                    .map(|bytes| String::from_utf8(bytes).expect("JSON encoders write UTF-8"))
            })
            .transpose()?;

        check(
            failures,
            name,
            "sibling events",
            &observed_events,
            &expected_sibling.events,
        );
        check(
            failures,
            name,
            "sibling snapshot",
            &observed_snapshot,
            &expected_sibling.snapshot,
        );
    }

    Ok(())
}

fn expected_rows(expected: &Expectation) -> Result<(Vec<EventRow>, Option<SnapshotRow>), BoxError> {
    let events = expected
        .events
        .iter()
        .map(|json| {
            Ok(EventRow::from_event(&SerializedEvent::from_json(
                json.as_bytes(),
            )?)?)
        })
        .collect::<Result<Vec<_>, BoxError>>()?;
    let snapshot = expected
        .snapshot
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
                    &expected,
                    scenario.snapshot_interval,
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
            }
            Backend::Postgres(repository) => {
                verify_common(
                    repository,
                    &expected,
                    scenario.snapshot_interval,
                    &mut failures,
                )
                .await?;

                let (want_events, want_snapshot) = expected_rows(&expected)?;
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
            }
        }

        if failures.len() == before {
            println!("rust verified {}", scenario.name);
        }
    }

    Ok(failures)
}

async fn run(args: Args) -> Result<Failures, BoxError> {
    if args.command == "steps" {
        println!("{}", Scenarios::load()?.max_steps());
        return Ok(Vec::new());
    }

    if args.run.is_empty() {
        return Err("--run is required".into());
    }

    let backend = connect(&args.backend).await?;

    match (args.command.as_str(), backend) {
        ("step", Backend::Dynamo(repository)) => step(repository, &args.run, args.index).await,
        ("step", Backend::Postgres(repository)) => step(repository, &args.run, args.index).await,
        ("verify", backend) => verify(backend, &args.run).await,
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

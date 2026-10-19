//! The Rust half of the cross-language compatibility harness between `photon-evt` and the Go
//! `evt` framework.
//!
//! It implements the shared account domain, runs the shared scenarios in `compat/scenarios.json`,
//! and checks the results against the golden fixtures the Go implementation generates in
//! `compat/fixtures/`. The `drivers` feature adds the `evt-compat` binary that the cross-language
//! integration script runs against live DynamoDB and PostgreSQL backends.

pub mod account;
#[cfg(feature = "drivers")]
pub mod drivers;
pub mod scenario;

pub use account::{Account, AccountCommand, AccountEvent};
pub use scenario::{
    Expectation, HeadAccess, Outcome, OutcomeExpectation, Runner, Scenario, ScenarioFixtures,
    Scenarios, Step, ViewAccess, ViewExpectation,
};

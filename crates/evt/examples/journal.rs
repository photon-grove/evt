//! The sync journal: map events to scopes, journal a store's commits, and poll a scope.
//!
//! The block between the `README` markers is the example in the crate README, which a test keeps
//! in step with this file. Run it with `cargo run --example journal --features journal,testing`.

// README:begin
use photon_evt::conformance::{Add, Ledger};
use photon_evt::journal::{Journal, JournalRepository, Scope};
use photon_evt::{Error, MemRepository, Metadata, SerializedEvent, Store};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Each event goes to its ledger's scope and to a scope shared by every ledger. The resolver is
    // a pure function of the stored event; it may return a `Result` and use `?`.
    let journal = Journal::new(
        ["ledger"],
        |event: &SerializedEvent| -> Result<Vec<Scope>, Error> {
            Ok(vec![
                Scope::new(format!("ledger:{}", event.entity_id))?,
                Scope::new("all")?,
            ])
        },
    )?
    .with_max_row_bytes(30_000)? // the default
    .with_retry(photon_evt::RetryPolicy::new(5)); // the default

    // Wrap a repository that writes views: in memory here, DynamoDB with a views table in
    // production.
    let repository = JournalRepository::new(MemRepository::new(), journal.clone());
    let store = Store::<Ledger, _>::new(&repository, ());

    store.execute("a", Add(5), Metadata::new("local")).await?;
    store.execute("b", Add(7), Metadata::new("local")).await?;
    store.execute("a", Add(1), Metadata::new("local")).await?;

    // A poller keeps the last position it saw per scope.
    let scope = Scope::new("all")?;
    let mut after = 0;
    loop {
        let page = repository.page(&scope, after, 100).await?;
        // ... deliver page.rows (each row.event is the stored event) ...
        after = page.next_after();

        if let Some(gap) = page.gap {
            // Positions gap.from..=gap.to are missing: retry the page, escalate if it persists.
            return Err(format!("journal gap {gap:?}").into());
        }
        if !page.has_more() {
            break; // caught up with repository.high_watermark(&scope)
        }
    }
    assert_eq!(after, 3);

    Ok(())
}
// README:end

#[cfg(test)]
mod tests {
    #[test]
    fn it_runs() {
        super::main().unwrap();
    }
}

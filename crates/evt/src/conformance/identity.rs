//! The recorded-identity cases: the identity an outcome row records beside its digest, and the
//! lookup by command ID and principal it backs
//! ([`Store::resolve_command_for_principal`](crate::Store::resolve_command_for_principal)). They
//! run in [`outcome_cases`] on every backend.

use super::*;
use crate::ids::PrincipalId;
use crate::outcome::RecordedIdentity;

/// Runs the recorded-identity cases. Kept out of [`outcome_cases`]' own future, so a debug test
/// thread's stack holds one case at a time.
pub(super) async fn identity_cases<R: Repository>(repo: &R) -> Vec<(&'static str, CaseResult)> {
    vec![
        (
            "every outcome records its identity: single-aggregate, multi-stream, rejected, no-op, replayed",
            Box::pin(identity_recorded(repo)).await,
        ),
        (
            "the engine's stale-fence and stale-version rejections record the identity",
            Box::pin(identity_recorded_by_engine_rejections(repo)).await,
        ),
        (
            "an outcome resolves by command ID for its own principal only",
            Box::pin(identity_resolve_for_principal(repo)).await,
        ),
        (
            "a row without a recorded identity decodes with none and fails closed",
            Box::pin(identity_legacy_row(repo)).await,
        ),
        (
            "the recorded identity is bounded before any write",
            Box::pin(identity_bound(repo)).await,
        ),
    ]
}

fn with_principal(command: &CommandId, principal: &str) -> Metadata {
    command_metadata(command).with_principal_id(principal)
}

fn recorded(
    command_type: &str,
    targets: &[&EntityId],
    principal: Option<&str>,
) -> Option<RecordedIdentity> {
    Some(RecordedIdentity {
        command_type: command_type.into(),
        targets: targets.iter().map(|&target| target.clone()).collect(),
        principal: principal.map(PrincipalId::new),
    })
}

async fn stored_identity<R: Repository>(
    repo: &R,
    command: &CommandId,
) -> Result<Option<RecordedIdentity>, Failure> {
    let stored = repo
        .outcome(command)
        .await?
        .ok_or_else(|| Failure(format!("an outcome row for {command}")))?;

    Ok(stored.identity)
}

async fn identity_recorded<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let ns = namespace();
    let id = EntityId::new(format!("{ns}-ledger"));

    // A single-aggregate command: its entity is its one target.
    let single = command_id();
    let first = store
        .execute_with_outcome(&id, Add(1), with_principal(&single, "user:42"))
        .await?;
    let want = recorded("ledger:add", &[&id], Some("user:42"));
    ensure_eq!(
        stored_identity(repo, &single).await?,
        want,
        "a single-aggregate command's identity"
    );
    ensure_eq!(
        first.outcome.identity,
        want,
        "the first execution returns it"
    );

    // A transaction's targets are deduplicated and sorted, as the digest takes them, whatever it
    // writes; a command without a principal records none.
    let (b, a) = (
        EntityId::new(format!("{ns}-b")),
        EntityId::new(format!("{ns}-a")),
    );
    let multi = command_id();
    let identity =
        CommandIdentity::new("ledger:copy").with_targets([b.clone(), a.clone(), b.clone()]);
    store
        .execute_transaction(&identity, &command_metadata(&multi), |tx| {
            let b = b.clone();

            Box::pin(async move {
                let mut read = tx.read::<Ledger>(&b).await?;
                tx.append(&mut read, added(1))?;

                Ok(Decision::accept())
            })
        })
        .await?;
    ensure_eq!(
        stored_identity(repo, &multi).await?,
        recorded("ledger:copy", &[&a, &b], None),
        "a transaction's identity"
    );

    // A rejection and an outcome-only no-op record theirs too.
    let rejected = command_id();
    store
        .execute_transaction(
            &CommandIdentity::new("ledger:claim").with_targets([b.clone()]),
            &with_principal(&rejected, "user:7"),
            |_| Box::pin(async { Ok(Decision::reject(Rejection::new("taken", "taken"))) }),
        )
        .await?;
    ensure_eq!(
        stored_identity(repo, &rejected).await?,
        recorded("ledger:claim", &[&b], Some("user:7")),
        "a rejection's identity"
    );

    let noop = command_id();
    store
        .execute_transaction(
            &CommandIdentity::new("ledger:audit"),
            &command_metadata(&noop),
            |_| Box::pin(async { Ok(Decision::accept()) }),
        )
        .await?;
    ensure_eq!(
        stored_identity(repo, &noop).await?,
        recorded("ledger:audit", &[], None),
        "a no-op's identity"
    );

    // So does a command driven by hand.
    let manual = command_id();
    let mut tx = store.transaction();
    match tx
        .begin_command(
            &CommandIdentity::new("ledger:touch").with_targets([a.clone()]),
            &command_metadata(&manual),
        )
        .await?
    {
        Begin::Proceed => {}
        Begin::Replayed(replayed) => {
            return Err(Failure(format!(
                "a new command is not replayed: {replayed:?}"
            )));
        }
    }
    tx.commit_with_outcome(None).await?;
    ensure_eq!(
        stored_identity(repo, &manual).await?,
        recorded("ledger:touch", &[&a], None),
        "a hand-driven command's identity"
    );

    // A replay returns the identity it recorded.
    let replayed = store
        .execute_with_outcome(&id, Add(1), with_principal(&single, "user:42"))
        .await?;
    ensure!(replayed.replayed, "the repeat is replayed: {replayed:?}");
    ensure_eq!(
        replayed.outcome.identity,
        stored_identity(repo, &single).await?,
        "the replayed identity"
    );

    Ok(())
}

/// The engine's own rejections record the identity too, through the outcome-only transaction.
async fn identity_recorded_by_engine_rejections<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let ns = namespace();
    let (a, b) = (
        EntityId::new(format!("{ns}-a")),
        EntityId::new(format!("{ns}-b")),
    );
    store
        .execute_with_outcome(&b, Add(1), command_metadata(&command_id()))
        .await?;

    // A caller-stamped fence already stale persists the stale-fence rejection.
    let stale = command_id();
    let outcome = store
        .execute_transaction(
            &CommandIdentity::new("ledger:rename").with_targets([b.clone()]),
            &with_principal(&stale, "user:9"),
            |tx| {
                let a = a.clone();

                Box::pin(async move {
                    tx.fence(ReadFence::new(
                        a,
                        Ledger::TYPE,
                        EventSequence(9),
                        FenceProvenance::Caller,
                    ))?;

                    Ok(Decision::accept())
                })
            },
        )
        .await?;
    ensure_eq!(
        outcome.rejection().map(|rejection| rejection.code.clone()),
        Some(crate::RejectionCode::STALE_FENCE),
        "a stale caller fence is the stale-fence rejection"
    );
    ensure_eq!(
        stored_identity(repo, &stale).await?,
        recorded("ledger:rename", &[&b], Some("user:9")),
        "a stale-fence rejection's identity"
    );

    // And a client-expected version already stale persists the stale-version rejection the same
    // way.
    let expected = command_id();
    let outcome = store
        .execute_transaction(
            &CommandIdentity::new("ledger:retitle").with_targets([b.clone()]),
            &with_principal(&expected, "user:9"),
            |tx| {
                let b = b.clone();

                Box::pin(async move {
                    tx.read::<Ledger>(&b).await?;
                    tx.expect(b, EventSequence(0))?;

                    Ok(Decision::accept())
                })
            },
        )
        .await?;
    ensure_eq!(
        outcome.rejection().map(|rejection| rejection.code.clone()),
        Some(crate::RejectionCode::STALE_VERSION),
        "a stale expected version is the stale-version rejection"
    );
    ensure_eq!(
        stored_identity(repo, &expected).await?,
        recorded("ledger:retitle", &[&b], Some("user:9")),
        "a stale-version rejection's identity"
    );

    Ok(())
}

async fn identity_resolve_for_principal<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let id = ledger_id();
    let (owned, anonymous) = (command_id(), command_id());
    let user_42 = PrincipalId::new("user:42");
    let user_7 = PrincipalId::new("user:7");

    let first = store
        .execute_with_outcome(&id, Add(1), with_principal(&owned, "user:42"))
        .await?;
    store
        .execute_with_outcome(&id, Add(2), command_metadata(&anonymous))
        .await?;

    let found = store
        .resolve_command_for_principal(&owned, Some(&user_42))
        .await?
        .ok_or_else(|| Failure("the owner finds its outcome".into()))?;
    ensure!(found.replayed, "a resolved outcome is replayed");
    ensure_eq!(found.outcome, first.outcome, "the stored outcome, exactly");

    let found = store
        .resolve_command_for_principal(&anonymous, None)
        .await?
        .ok_or_else(|| Failure("no principal finds a command without one".into()))?;
    ensure!(found.replayed, "no principal matches no principal");

    for (what, command, principal) in [
        ("another principal", &owned, Some(&user_7)),
        ("no principal for a principal's", &owned, None),
        (
            "a principal for one without one",
            &anonymous,
            Some(&user_42),
        ),
    ] {
        // Nothing of the stored outcome is returned: the error carries only the command ID.
        match store
            .resolve_command_for_principal(command, principal)
            .await
        {
            Err(Error::OutcomePrincipalMismatch {
                command_id,
                recorded: true,
            }) => ensure_eq!(&command_id, command, what),
            other => return Err(Failure(format!("{what}: got {other:?}"))),
        }
    }

    let absent = store
        .resolve_command_for_principal(&command_id(), Some(&user_42))
        .await?;
    ensure!(
        absent.is_none(),
        "an absent command is not found: {absent:?}"
    );

    // An empty principal is no principal, as the row records it.
    let found = store
        .resolve_command_for_principal(&anonymous, Some(&PrincipalId::new("")))
        .await?
        .ok_or_else(|| Failure("an empty principal finds a command without one".into()))?;
    ensure!(found.replayed, "an empty principal matches no principal");

    // The command ID and principal are checked like a write's, before the read.
    expect_err(
        store
            .resolve_command_for_principal(&CommandId::new("x".repeat(513)), None)
            .await,
        "an over-long command ID",
        |err| matches!(err, Error::CommandIdTooLong { .. }),
    )?;
    expect_err(
        store
            .resolve_command_for_principal(&owned, Some(&PrincipalId::new("user\0")))
            .await,
        "a principal with U+0000",
        |err| matches!(err, Error::InvalidArgument(_)),
    )?;
    expect_err(
        Store::<Ledger, _>::new(repo, ())
            .resolve_command_for_principal(&owned, None)
            .await,
        "a store without outcomes",
        |err| matches!(err, Error::InvalidConfig(_)),
    )?;

    Ok(())
}

async fn identity_legacy_row<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);

    // A row written before outcomes recorded their identity: the repository stores what it is
    // given, so an outcome-only commit without an identity stands in for one.
    let legacy = command_id();
    repo.commit_transaction(
        TransactionCommit::new(Vec::new())
            .with_outcome(Outcome::accepted(legacy.clone(), "legacy-digest")),
    )
    .await?;

    let stored = repo
        .outcome(&legacy)
        .await?
        .ok_or_else(|| Failure("the legacy row is stored".into()))?;
    ensure!(
        stored.identity.is_none(),
        "a row without the fields decodes with no identity: {stored:?}"
    );

    for (what, principal) in [
        ("a principal", Some(PrincipalId::new("user:42"))),
        ("no principal", None),
    ] {
        match store
            .resolve_command_for_principal(&legacy, principal.as_ref())
            .await
        {
            Err(Error::OutcomePrincipalMismatch {
                recorded: false, ..
            }) => {}
            other => {
                return Err(Failure(format!(
                    "{what}: an unrecorded identity fails closed, got {other:?}"
                )));
            }
        }
    }

    Ok(())
}

async fn identity_bound<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let ns = namespace();

    // Forty targets of 2,000 bytes each: well past the 64 KiB the recorded identity may take.
    let targets: Vec<EntityId> = (0..40u8)
        .map(|i| {
            let fill = char::from(b'a' + i % 26).to_string().repeat(2000);

            EntityId::new(format!("{ns}-{fill}{}", char::from(b'A' + i / 26)))
        })
        .collect();

    let command = command_id();
    let result = store
        .execute_transaction(
            &CommandIdentity::new("ledger:audit").with_targets(targets),
            &command_metadata(&command),
            |_| Box::pin(async { Ok(Decision::accept()) }),
        )
        .await;
    expect_err(result, "an oversized identity", |err| {
        matches!(
            err,
            Error::OutcomeTooLarge {
                what: "identity",
                ..
            }
        )
    })?;

    let stored = repo.outcome(&command).await?;
    ensure!(stored.is_none(), "nothing is written: {stored:?}");

    Ok(())
}

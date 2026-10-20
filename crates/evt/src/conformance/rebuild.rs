//! The rebuild-fence cases (v0.9, C10): the lock and generation lifecycle, the commit-time lock
//! check and its precedence, the reader epoch check, guarded view writes, and fence-aware retry
//! around a rebuild. The Rust counterpart of Go's `conformance/rebuild.go`.

use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use super::*;
use crate::projection::{ProjectedView, ViewOperation, ViewRow};
use crate::rebuild::{
    MAX_REBUILD_OWNER_BYTES, MAX_REBUILD_VIEW_BATCH, RebuildFence, RebuildGeneration, RebuildLease,
    RebuildState, observe_rebuild_epoch, read_at_rebuild_epoch, verify_rebuild_epoch,
};

/// A settable clock in whole Unix seconds for the rebuild cases: a repository reads it through
/// `with_clock` ([`reader`](TestClock::reader)), and the cases [`advance`](TestClock::advance) it
/// to expire a lease without waiting.
#[derive(Clone, Debug)]
pub struct TestClock(Arc<AtomicI64>);

impl TestClock {
    /// A clock that reads `seconds`.
    pub fn at(seconds: i64) -> Self {
        Self(Arc::new(AtomicI64::new(seconds)))
    }

    /// A clock that starts at the system time.
    pub fn starting_now() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs());

        Self::at(i64::try_from(now).unwrap_or(i64::MAX))
    }

    /// The clock's time, in whole Unix seconds.
    pub fn seconds(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }

    /// The clock's time.
    pub fn now(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(u64::try_from(self.seconds()).unwrap_or_default())
    }

    /// Moves the clock forward.
    pub fn advance(&self, seconds: u64) {
        self.0
            .fetch_add(i64::try_from(seconds).unwrap_or(i64::MAX), Ordering::SeqCst);
    }

    /// The clock as a function, for a repository's `with_clock`.
    pub fn reader(&self) -> impl Fn() -> SystemTime + Send + Sync + 'static {
        let clock = self.clone();

        move || clock.now()
    }
}

/// The placeholder view storage for [`RebuildSuite`]s without one: the guarded-write case is
/// skipped.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoViewRows;

impl<R: Repository> CommitViewTarget<R> for NoViewRows {
    async fn row(&self, _: &str, _: &str) -> Result<Option<ViewRow>, BoxError> {
        Err("no view rows".into())
    }
}

/// Runs the rebuild-fence cases against a repository that keeps the fence, reads `clock` for its
/// leases, and records command outcomes. The fence is store-wide state, so every case starts from
/// a settled store and leaves it settled: run it on a table no concurrent test commits to. Like
/// [`outcome_cases`], it panics after running every case if any failed.
///
/// ```
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// use photon_evt::MemRepository;
/// use photon_evt::conformance::{RebuildSuite, TestClock};
///
/// let clock = TestClock::starting_now();
/// let repository = MemRepository::new().with_clock(clock.reader());
///
/// RebuildSuite::new(&repository, &clock)
///     .with_views(repository.clone())
///     .with_head_fixtures(repository.clone())
///     .run()
///     .await;
/// # });
/// ```
pub struct RebuildSuite<'a, R, T = NoViewRows, H = NoHeadFixtures> {
    repo: &'a R,
    clock: &'a TestClock,
    views: Option<T>,
    heads: Option<H>,
}

impl<'a, R: Repository> RebuildSuite<'a, R> {
    /// The cases over `repo`, which must read `clock`.
    pub fn new(repo: &'a R, clock: &'a TestClock) -> Self {
        Self {
            repo,
            clock,
            views: None,
            heads: None,
        }
    }
}

impl<'a, R, T, H> RebuildSuite<'a, R, T, H>
where
    R: Repository + RebuildFence + FeatureMarker,
    T: CommitViewTarget<R>,
    H: HeadFixtures<R>,
{
    /// Reads view rows back through `views`, which enables the guarded-write case. The repository
    /// must be configured to write views.
    pub fn with_views<V: CommitViewTarget<R>>(self, views: V) -> RebuildSuite<'a, R, V, H> {
        RebuildSuite {
            repo: self.repo,
            clock: self.clock,
            views: Some(views),
            heads: self.heads,
        }
    }

    /// Writes past the head through `heads`, which adds drift to the precedence case.
    pub fn with_head_fixtures<G: HeadFixtures<R>>(self, heads: G) -> RebuildSuite<'a, R, T, G> {
        RebuildSuite {
            repo: self.repo,
            clock: self.clock,
            views: self.views,
            heads: Some(heads),
        }
    }

    /// Runs every case.
    pub async fn run(self) {
        let env = Env {
            repo: self.repo,
            clock: self.clock,
            ns: namespace(),
            leases: Mutex::new(BTreeMap::new()),
        };
        let mut failures = Vec::new();

        let mut record = |name: &str, result: CaseResult| match result {
            Ok(()) => println!("ok   {name}"),
            Err(Failure(message)) => {
                println!("FAIL {name}");
                failures.push(format!("{name}: {message}"));
            }
        };

        macro_rules! case {
            ($name:expr, $case:expr) => {{
                let result = match env.settle().await {
                    Ok(()) => Box::pin($case).await,
                    Err(failure) => Err(failure),
                };
                record($name, result);
            }};
        }

        case!(
            "a settled store accepts commits and they never write the fence",
            settled_store(&env)
        );
        case!(
            "begin, renew, and end move the lock and the generation",
            lifecycle(&env)
        );
        case!(
            "an expired lease is taken over, bumping the generation by two",
            takeover(&env)
        );
        case!(
            "a force-release ends the lease but leaves commits refused and readers fenced",
            force_release(&env)
        );
        case!(
            "a stale incarnation of an owner token can act on no later lease",
            stale_incarnation(&env, self.views.as_ref())
        );
        case!(
            "bad owners and leases are refused before any request",
            arguments(&env)
        );
        case!(
            "every commit is refused while a rebuild holds the lock",
            refuses_commits(&env)
        );
        case!(
            "a failed, force-released rebuild refuses commits until a rebuild ends",
            force_released_refuses_commits(&env, self.views.as_ref())
        );
        case!(
            "the lock check outranks every failure but the marker",
            precedence(&env, self.heads.as_ref())
        );
        case!("readers see odd and changed generations", epochs(&env));
        case!(
            "snapshot writes, compaction, and entity heads carry no lock check",
            maintenance(&env)
        );
        if let Some(views) = &self.views {
            case!(
                "guarded view writes need an unexpired lease",
                guarded_views(&env, views)
            );
        }
        case!(
            "fence-aware retry waits out a rebuild and never records a rejection for it",
            engine_retry(&env)
        );

        if let Err(Failure(message)) = env.settle().await {
            println!("FAIL cleanup");
            failures.push(format!("cleanup: {message}"));
        }

        if !failures.is_empty() {
            panic!(
                "{} rebuild-fence case(s) failed:\n\n{}",
                failures.len(),
                failures.join("\n\n")
            );
        }
    }
}

/// One repository handle with a controllable clock. The fence is store-wide, so every case
/// settles it first.
struct Env<'a, R> {
    repo: &'a R,
    clock: &'a TestClock,
    ns: String,
    /// The last lease each name began.
    leases: Mutex<BTreeMap<String, RebuildLease>>,
}

impl<R: Repository + RebuildFence> Env<'_, R> {
    /// Ends whatever rebuild is in progress and brings the generation back to an even value.
    async fn settle(&self) -> CaseResult {
        let state = self.repo.rebuild_state().await?;
        if let Some(lock) = state.lock {
            self.repo.force_release_rebuild(&lock.owner).await?;
        }

        if !self.repo.rebuild_generation().await?.is_settled() {
            let lease = self
                .repo
                .begin_rebuild(&self.owner("settle"), Duration::from_secs(60))
                .await?;
            self.repo.end_rebuild(&lease).await?;
        }

        Ok(())
    }

    fn owner(&self, name: &str) -> String {
        format!("{}-{name}", self.ns)
    }

    /// Begins a rebuild as `name` and remembers its lease.
    async fn begin(&self, name: &str, seconds: u64) -> Result<RebuildLease, Failure> {
        let lease = self
            .repo
            .begin_rebuild(&self.owner(name), Duration::from_secs(seconds))
            .await?;
        self.leases().insert(name.to_owned(), lease.clone());

        Ok(lease)
    }

    /// The last lease `name` began, or one that holds nothing (generation 0) when it began none.
    fn lease(&self, name: &str) -> RebuildLease {
        self.leases()
            .get(name)
            .cloned()
            .unwrap_or_else(|| RebuildLease {
                owner: self.owner(name),
                generation: 0,
                lease_expires_at: 0,
            })
    }

    fn leases(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, RebuildLease>> {
        self.leases
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn end(&self, name: &str) -> Result<RebuildGeneration, Failure> {
        Ok(self.repo.end_rebuild(&self.lease(name)).await?)
    }

    async fn generation(&self) -> Result<RebuildGeneration, Failure> {
        Ok(self.repo.rebuild_generation().await?)
    }

    /// Writes one event to a fresh stream of the case.
    async fn commit(&self, name: &str) -> Result<(), Error> {
        let stream = Stream::new(&self.ns, name, "ledger");

        stream.commit(self.repo, stream.events(1..=1), None).await
    }
}

fn refused<T: fmt::Debug>(result: Result<T, Error>, what: &str) -> CaseResult {
    match result {
        Err(error @ Error::RebuildInProgress) => {
            ensure!(
                error.is_retryable() && !error.is_conflict(),
                "{what}: a refused commit is transient and never a conflict"
            );

            Ok(())
        }
        other => Err(Failure(format!(
            "{what}: want RebuildInProgress, got {other:?}"
        ))),
    }
}

fn lease_lost<T: fmt::Debug>(result: Result<T, Error>, what: &str) -> CaseResult {
    expect_err(result, what, |err| {
        matches!(err, Error::RebuildLeaseLost { .. })
    })
}

fn lock_held<T: fmt::Debug>(result: Result<T, Error>, what: &str) -> CaseResult {
    expect_err(result, what, |err| {
        matches!(err, Error::RebuildLockHeld { .. })
    })
}

async fn lifecycle<R: Repository + RebuildFence>(env: &Env<'_, R>) -> CaseResult {
    let repo = env.repo;
    let before = env.generation().await?;
    ensure!(
        before.is_settled() && !before.force_released,
        "a settled start: {before:?}"
    );
    let state = repo.rebuild_state().await?;
    ensure!(!state.in_progress(), "no lock: {state:?}");
    ensure_eq!(state.generation, before, "the state's generation");

    // Begin acquires: the generation grows by one and is odd, and the lock names the owner.
    let started = env.begin("a", 60).await?;
    ensure_eq!(started.owner, env.owner("a"), "the lease's owner");
    ensure_eq!(started.generation, before.value + 1, "one more, odd");
    ensure_eq!(
        started.lease_expires_at,
        env.clock.seconds() + 60,
        "the expiry"
    );
    let state = repo.rebuild_state().await?;
    ensure_eq!(
        state,
        RebuildState {
            generation: RebuildGeneration {
                value: before.value + 1,
                force_released: false
            },
            lock: Some(crate::RebuildLock {
                owner: env.owner("a"),
                generation: started.generation,
                lease_expires_at: started.lease_expires_at
            }),
        },
        "the state while the rebuild holds the lock"
    );

    // Another owner is refused while the lease is held, naming the holder; so is the holder.
    match repo
        .begin_rebuild(&env.owner("b"), Duration::from_secs(60))
        .await
    {
        Err(Error::RebuildLockHeld {
            owner,
            lease_expires_at,
        }) => {
            ensure_eq!(owner, env.owner("a"), "the holder");
            ensure_eq!(lease_expires_at, started.lease_expires_at, "its expiry");
        }
        other => return Err(Failure(format!("a held lock: {other:?}"))),
    }
    ensure_eq!(
        env.generation().await?.value,
        before.value + 1,
        "a refused begin changes nothing"
    );
    lock_held(
        repo.begin_rebuild(&env.owner("a"), Duration::from_secs(60))
            .await,
        "the same owner cannot begin twice",
    )?;

    // Renew extends the lease to the new duration from now, and only for the holder.
    env.clock.advance(30);
    let renewed = repo
        .renew_rebuild(&started, Duration::from_secs(120))
        .await?;
    let expiry = renewed.lease_expires_at;
    ensure_eq!(
        renewed,
        RebuildLease {
            lease_expires_at: env.clock.seconds() + 120,
            ..started.clone()
        },
        "the renewed lease"
    );
    let state = repo.rebuild_state().await?;
    ensure_eq!(
        state.lock.map(|lock| lock.lease_expires_at),
        Some(expiry),
        "the stored expiry"
    );
    ensure_eq!(
        state.generation.value,
        before.value + 1,
        "renewing does not touch the generation"
    );
    lease_lost(
        repo.renew_rebuild(&env.lease("b"), Duration::from_secs(60))
            .await,
        "another owner's renew",
    )?;
    lease_lost(
        repo.end_rebuild(&env.lease("b")).await,
        "another owner's end",
    )?;

    // End releases: even again, no lock, nothing flagged.
    let settled = env.end("a").await?;
    ensure_eq!(
        settled,
        RebuildGeneration {
            value: before.value + 2,
            force_released: false
        },
        "the settled generation"
    );
    ensure_eq!(env.generation().await?, settled, "the stored generation");
    ensure!(
        !repo.rebuild_state().await?.in_progress(),
        "the lock is gone"
    );
    lease_lost(repo.end_rebuild(&env.lease("a")).await, "a second end")?;
    lease_lost(
        repo.renew_rebuild(&env.lease("a"), Duration::from_secs(60))
            .await,
        "a renew after the end",
    )?;

    // A new rebuild starts from the settled value, whoever ran the last one.
    ensure_eq!(
        env.begin("b", 60).await?.generation,
        before.value + 3,
        "the next rebuild"
    );
    env.end("b").await?;

    Ok(())
}

/// Turns a case helper's failure back into the error it wrapped, for the error matchers.
fn failure_error(failure: Failure) -> Error {
    Error::Backend(failure.0.into())
}

async fn takeover<R: Repository + RebuildFence>(env: &Env<'_, R>) -> CaseResult {
    let repo = env.repo;
    let before = env.generation().await?;
    let first = env.begin("a", 10).await?;

    // A lease is still held at the second it expires.
    env.clock.advance(10);
    lock_held(
        repo.begin_rebuild(&env.owner("b"), Duration::from_secs(60))
            .await,
        "the lease is held through its expiry second",
    )?;

    // An expired lease still blocks commits until it is taken over.
    env.clock.advance(1);
    refused(
        env.commit("expired").await,
        "a commit under an expired lease",
    )?;

    // Taking over replaces the owner and bumps the generation by two: odd, and changed.
    let taken = env.begin("b", 60).await?;
    ensure_eq!(taken.generation, first.generation + 2, "two more");
    ensure_eq!(taken.generation, before.value + 3, "from the start");
    ensure!(
        !env.generation().await?.is_settled(),
        "the takeover leaves the generation odd"
    );
    ensure_eq!(
        repo.rebuild_state().await?.lock.map(|lock| lock.owner),
        Some(env.owner("b")),
        "the new owner"
    );

    // The displaced worker can do nothing.
    lease_lost(
        repo.renew_rebuild(&env.lease("a"), Duration::from_secs(60))
            .await,
        "the displaced worker's renew",
    )?;
    lease_lost(
        repo.end_rebuild(&env.lease("a")).await,
        "the displaced worker's end",
    )?;
    refused(env.commit("taken").await, "a commit after the takeover")?;

    // The new owner ends the rebuild: odd to even, one step.
    ensure_eq!(
        env.end("b").await?,
        RebuildGeneration {
            value: before.value + 4,
            force_released: false
        },
        "the end after a takeover"
    );
    env.commit("after-takeover").await?;

    // A renewal keeps a lease from being taken over.
    env.begin("c", 10).await?;
    env.clock.advance(8);
    repo.renew_rebuild(&env.lease("c"), Duration::from_secs(10))
        .await?;
    env.clock.advance(8);
    lock_held(
        repo.begin_rebuild(&env.owner("d"), Duration::from_secs(60))
            .await,
        "a renewed lease",
    )?;
    env.end("c").await?;

    Ok(())
}

async fn force_release<R: Repository + RebuildFence>(env: &Env<'_, R>) -> CaseResult {
    let repo = env.repo;
    let before = env.generation().await?;
    env.begin("a", 3600).await?;

    // Only the owner the operator names is released.
    lease_lost(
        repo.force_release_rebuild(&env.owner("b")).await,
        "another owner's force-release",
    )?;
    refused(env.commit("locked").await, "a commit under the lock")?;

    let forced = repo.force_release_rebuild(&env.owner("a")).await?;
    ensure_eq!(
        forced,
        RebuildGeneration {
            value: before.value + 3,
            force_released: true
        },
        "odd and flagged"
    );
    ensure_eq!(env.generation().await?, forced, "the stored generation");
    ensure!(
        !repo.rebuild_state().await?.in_progress(),
        "no rebuild holds the lock"
    );

    // Readers stay fenced and commits stay refused: the derived state the released rebuild left
    // may be half-written.
    refused(env.commit("force-released").await, "a commit after it")?;
    expect_err(
        observe_rebuild_epoch(repo).await,
        "a reader after a force-release",
        |err| {
            matches!(
                err,
                Error::RebuildUnavailable {
                    force_released: true,
                    ..
                }
            )
        },
    )?;
    lease_lost(
        repo.force_release_rebuild(&env.owner("a")).await,
        "no lock is left to release",
    )?;
    lease_lost(repo.end_rebuild(&env.lease("a")).await, "nor to end")?;

    // A new rebuild acquires from the force-released generation, adding two; the flag stays until
    // it ends.
    ensure_eq!(
        env.begin("c", 3600).await?.generation,
        before.value + 5,
        "an acquire from the force-released generation"
    );
    ensure!(
        env.generation().await?.force_released,
        "the flag stays while the rebuild runs"
    );
    refused(env.commit("rebuilding").await, "a commit during it")?;

    // Ending it settles the generation and clears the flag.
    ensure_eq!(
        env.end("c").await?,
        RebuildGeneration {
            value: before.value + 6,
            force_released: false
        },
        "the end clears the flag"
    );
    ensure_eq!(
        observe_rebuild_epoch(repo).await?.generation(),
        before.value + 6,
        "readers trust derived state again"
    );
    env.commit("force-released").await?;

    // A later rebuild can be force-released again.
    env.begin("d", 3600).await?;
    ensure_eq!(
        repo.force_release_rebuild(&env.owner("d")).await?,
        RebuildGeneration {
            value: before.value + 9,
            force_released: true
        },
        "a second force-release"
    );

    Ok(())
}

/// A lease is fenced by its generation, not just its owner token: a worker incarnation whose lease
/// was taken over by a new incarnation under the same token can renew, write, and end nothing.
async fn stale_incarnation<R, T>(env: &Env<'_, R>, views: Option<&T>) -> CaseResult
where
    R: Repository + RebuildFence,
    T: CommitViewTarget<R>,
{
    let repo = env.repo;
    let pk = format!("{}#incarnations", env.ns);
    let put = |sk: &str| {
        [ProjectedView::new(
            "rebuild",
            0,
            0,
            ViewOperation::put(pk.clone(), sk, "owner", "test", "{}"),
        )]
    };
    let row = |sk: &'static str| {
        let pk = pk.clone();

        async move {
            match views {
                Some(views) => views
                    .row(&pk, sk)
                    .await
                    .map_err(|err| Failure(err.to_string())),
                None => Ok(None),
            }
        }
    };
    let token = env.owner("worker");

    let stale = repo.begin_rebuild(&token, Duration::from_secs(10)).await?;
    env.clock.advance(11);
    let fresh = repo.begin_rebuild(&token, Duration::from_secs(60)).await?;
    ensure_eq!(fresh.owner, stale.owner, "the same owner token");
    ensure_eq!(
        fresh.generation,
        stale.generation + 2,
        "an expired lease is taken over, even by the same owner token"
    );
    let before = repo.rebuild_state().await?;
    ensure_eq!(
        before.lock.as_ref().map(|lock| lock.generation),
        Some(fresh.generation),
        "the lock stores the new lease's fencing token"
    );

    // The stale incarnation changes nothing.
    lease_lost(
        repo.renew_rebuild(&stale, Duration::from_secs(3600)).await,
        "the stale incarnation's renew",
    )?;
    if views.is_some() {
        lease_lost(
            repo.write_rebuild_views(&stale, &put("stale")).await,
            "the stale incarnation's write",
        )?;
        ensure!(
            row("stale").await?.is_none(),
            "the stale write wrote nothing"
        );
    }
    lease_lost(
        repo.end_rebuild(&stale).await,
        "the stale incarnation's end",
    )?;
    ensure_eq!(
        repo.rebuild_state().await?,
        before,
        "the new holder's lock, expiry, and generation are untouched"
    );

    // The new incarnation can.
    let renewed = repo.renew_rebuild(&fresh, Duration::from_secs(120)).await?;
    ensure_eq!(
        renewed.lease_expires_at,
        env.clock.seconds() + 120,
        "the renewed expiry"
    );
    if views.is_some() {
        repo.write_rebuild_views(&renewed, &put("fresh")).await?;
        ensure!(row("fresh").await?.is_some(), "the new holder's row");
    }
    let settled = repo.end_rebuild(&renewed).await?;
    ensure_eq!(
        settled.value,
        fresh.generation + 1,
        "the settled generation"
    );

    // A lease does not outlive its rebuild: after the end, the same token begins again under a
    // new generation, and both earlier leases are dead.
    let again = repo.begin_rebuild(&token, Duration::from_secs(60)).await?;
    ensure_eq!(again.generation, settled.value + 1, "the next lease");
    for (name, old) in [("stale", &stale), ("ended", &fresh)] {
        lease_lost(
            repo.renew_rebuild(old, Duration::from_secs(3600)).await,
            &format!("the {name} lease's renew"),
        )?;
        lease_lost(
            repo.end_rebuild(old).await,
            &format!("the {name} lease's end"),
        )?;
    }
    repo.end_rebuild(&again).await?;

    Ok(())
}

async fn arguments<R: Repository + RebuildFence>(env: &Env<'_, R>) -> CaseResult {
    let repo = env.repo;
    let before = env.generation().await?;
    let minute = Duration::from_secs(60);
    let oversized = "o".repeat(MAX_REBUILD_OWNER_BYTES + 1);
    let invalid = |err: &Error| matches!(err, Error::InvalidArgument(_));

    expect_err(repo.begin_rebuild("", minute).await, "empty owner", invalid)?;
    expect_err(
        repo.begin_rebuild(&oversized, minute).await,
        "oversized owner",
        invalid,
    )?;
    expect_err(
        repo.begin_rebuild("job\0", minute).await,
        "owner with U+0000",
        invalid,
    )?;
    expect_err(
        repo.begin_rebuild(&env.owner("a"), Duration::ZERO).await,
        "zero lease",
        invalid,
    )?;
    expect_err(
        repo.begin_rebuild(&env.owner("a"), Duration::from_millis(999))
            .await,
        "sub-second lease",
        invalid,
    )?;
    expect_err(
        repo.renew_rebuild(&env.lease("a"), Duration::ZERO).await,
        "renew with a zero lease",
        invalid,
    )?;
    let nobody = RebuildLease {
        owner: String::new(),
        generation: 1,
        lease_expires_at: 0,
    };
    expect_err(
        repo.end_rebuild(&nobody).await,
        "end with an empty owner",
        invalid,
    )?;
    expect_err(
        repo.force_release_rebuild("").await,
        "force-release with an empty owner",
        invalid,
    )?;
    expect_err(
        repo.write_rebuild_views(&nobody, &[]).await,
        "a guarded write with an empty owner",
        invalid,
    )?;
    ensure_eq!(
        env.generation().await?,
        before,
        "refused calls change nothing"
    );

    // A lease is rounded up to whole seconds.
    let started = repo
        .begin_rebuild(&env.owner("a"), Duration::from_millis(1500))
        .await?;
    ensure_eq!(
        started.lease_expires_at,
        env.clock.seconds() + 2,
        "a lease rounded up"
    );

    Ok(())
}

async fn refuses_commits<R: Repository + RebuildFence>(env: &Env<'_, R>) -> CaseResult {
    let repo = env.repo;

    // A stream that exists before the rebuild, a fence target.
    let existing = Stream::new(&env.ns, "existing", "ledger");
    existing.commit(repo, existing.events(1..=1), None).await?;

    env.begin("worker", 3600).await?;

    // A plain commit, a multi-stream commit, fenced commits, an outcome-only commit, and an outcome
    // with events are all refused, and write nothing.
    let plain = Stream::new(&env.ns, "plain", "ledger");
    refused(
        plain.commit(repo, plain.events(1..=1), None).await,
        "a plain commit",
    )?;

    let (left, right) = (
        Stream::new(&env.ns, "left", "ledger"),
        Stream::new(&env.ns, "right", "ledger"),
    );
    refused(
        repo.commit_transaction(TransactionCommit::new(vec![
            write(&left, 1..=1),
            write(&right, 1..=1),
        ]))
        .await,
        "a multi-stream commit",
    )?;

    // A fence is never a stale-fence verdict during a rebuild, held or stale, caller-stamped or
    // engine-filled.
    let fenced = Stream::new(&env.ns, "fenced", "ledger");
    for (head, provenance) in [
        (1, FenceProvenance::Engine),
        (7, FenceProvenance::Engine),
        (1, FenceProvenance::Caller),
        (7, FenceProvenance::Caller),
    ] {
        refused(
            repo.commit_transaction(
                TransactionCommit::new(vec![write(&fenced, 1..=1)]).with_fences([ReadFence::new(
                    existing.id.clone(),
                    existing.entity_type.clone(),
                    EventSequence(head),
                    provenance,
                )]),
            )
            .await,
            &format!("a commit fenced at head {head} ({provenance:?})"),
        )?;
    }

    let only = CommandId::new(format!("{}-only", env.ns));
    refused(
        repo.commit_transaction(
            TransactionCommit::new(Vec::new())
                .with_outcome(Outcome::accepted(only.clone(), "digest")),
        )
        .await,
        "an outcome-only commit",
    )?;
    ensure!(
        repo.outcome(&only).await?.is_none(),
        "a refused outcome-only commit records nothing"
    );
    let with_events = CommandId::new(format!("{}-events", env.ns));
    refused(
        repo.commit(
            write(&plain, 1..=1).with_outcome(Outcome::accepted(with_events.clone(), "digest")),
        )
        .await,
        "an accepted outcome with events",
    )?;
    let rejected = CommandId::new(format!("{}-rejected", env.ns));
    refused(
        repo.commit(write(&plain, 1..=1).with_outcome(Outcome::rejected(
            rejected.clone(),
            "digest",
            Rejection::new("no", "refused"),
        )))
        .await,
        "a rejection with events",
    )?;
    for id in [&plain.id, &left.id, &right.id, &fenced.id] {
        ensure!(
            repo.events(id).await?.is_empty(),
            "a refused commit wrote nothing to {id}"
        );
    }
    for id in [&with_events, &rejected] {
        ensure!(repo.outcome(id).await?.is_none(), "no outcome for {id}");
    }

    // A refusal that needs no read of the lock keeps its own error: a commit over budget, a
    // reserved entity ID, and a projector-only commit.
    let over = Stream::new(&env.ns, "over", "ledger");
    expect_err(
        over.commit(repo, over.events(1..=120), None).await,
        "an oversized commit",
        |err| matches!(err, Error::CommitTooLarge { .. }),
    )?;
    expect_err(
        repo.commit(
            Commit::new(format!("SYSTEM#{}", env.ns), Vec::new())
                .with_outcome(Outcome::accepted(only.clone(), "digest")),
        )
        .await,
        "a reserved entity ID",
        |err| matches!(err, Error::ReservedEntityId { .. }),
    )?;
    expect_err(
        repo.commit_transaction(TransactionCommit::new(Vec::new()).with_views([
            ProjectedView::new("p", 0, 0, ViewOperation::put("p", "s", "o", "", "{}")),
        ]))
        .await,
        "a projector-only commit",
        |err| matches!(err, Error::ProjectorWithoutEvents { .. }),
    )?;

    // Released: commits resume, and the same streams commit as if nothing happened.
    env.end("worker").await?;
    plain.commit(repo, plain.events(1..=1), None).await?;
    ensure_eq!(repo.events(&plain.id).await?.len(), 1, "the plain stream");

    // An expired lock still refuses commits, and so does a force-release of it: only a rebuild
    // that ends settles the generation.
    env.begin("short", 1).await?;
    env.clock.advance(5);
    refused(env.commit("expired-lock").await, "an expired lock")?;
    repo.force_release_rebuild(&env.owner("short")).await?;
    refused(
        env.commit("after-force-release").await,
        "a commit after the force-release",
    )?;

    Ok(())
}

/// A settled generation accepts commits, a store that never ran a rebuild included: the commit's
/// lock check passes against the absent fence rows, and the commit writes neither of them.
async fn settled_store<R: Repository + RebuildFence>(env: &Env<'_, R>) -> CaseResult {
    let before = env.repo.rebuild_state().await?;
    ensure!(before.generation.is_settled(), "{before:?}");
    ensure!(before.lock.is_none(), "{before:?}");

    env.commit("settled").await?;
    ensure_eq!(
        env.repo.rebuild_state().await?,
        before,
        "a commit never writes the fence"
    );

    Ok(())
}

/// A rebuild that fails after its first write leaves derived state half-written. The operator
/// force-releases it, and every commit — a plain one, one with commit-wide views, an outcome-only
/// one — is still refused and writes nothing, so nothing derived from the half-written state (a
/// journal position allocated from a wiped counter, say) can be committed. A rebuild that then
/// runs to its end settles the generation, and the same commits succeed.
async fn force_released_refuses_commits<R, T>(env: &Env<'_, R>, views: Option<&T>) -> CaseResult
where
    R: Repository + RebuildFence,
    T: CommitViewTarget<R>,
{
    let repo = env.repo;
    let pk = format!("{}#half-built", env.ns);
    let put = |sk: &str| {
        ProjectedView::new(
            "rebuild",
            0,
            0,
            ViewOperation::put(pk.clone(), sk, "owner", "test", "{}"),
        )
    };
    let row = |pk: String, sk: &'static str| async move {
        match views {
            Some(views) => views
                .row(&pk, sk)
                .await
                .map_err(|err| Failure(err.to_string())),
            None => Ok(None),
        }
    };

    // The rebuild writes its first batch, then fails and never ends.
    env.begin("failed", 3600).await?;
    if views.is_some() {
        repo.write_rebuild_views(&env.lease("failed"), &[put("first")])
            .await?;
    }

    let forced = repo.force_release_rebuild(&env.owner("failed")).await?;
    ensure!(
        forced.force_released && !forced.is_settled(),
        "odd and flagged: {forced:?}"
    );
    ensure!(
        repo.rebuild_state().await?.lock.is_none(),
        "no rebuild holds the lock"
    );

    // Every commit is refused with the error a held lock gives, and writes nothing.
    let plain = Stream::new(&env.ns, "released-plain", "ledger");
    refused(
        plain.commit(repo, plain.events(1..=1), None).await,
        "a plain commit",
    )?;

    let viewed = Stream::new(&env.ns, "released-viewed", "ledger");
    let live_pk = format!("{}#released-live", env.ns);
    let live = || {
        TransactionCommit::new(vec![write(&viewed, 1..=1)]).with_views([ProjectedView::new(
            "live",
            0,
            0,
            ViewOperation::put(live_pk.clone(), "row", viewed.id.as_str(), "ledger", "{}"),
        )])
    };
    if views.is_some() {
        refused(repo.commit_transaction(live()).await, "a commit with views")?;
        ensure!(
            row(live_pk.clone(), "row").await?.is_none(),
            "the refused commit wrote no view row"
        );
    }

    let only = CommandId::new(format!("{}-released-only", env.ns));
    let outcome_only = || {
        TransactionCommit::new(Vec::new()).with_outcome(Outcome::accepted(only.clone(), "digest"))
    };
    refused(
        repo.commit_transaction(outcome_only()).await,
        "an outcome-only commit",
    )?;
    ensure!(
        repo.outcome(&only).await?.is_none(),
        "a refused outcome-only commit records nothing"
    );
    for id in [&plain.id, &viewed.id] {
        ensure!(
            repo.events(id).await?.is_empty(),
            "a refused commit wrote nothing to {id}"
        );
    }

    // The failed worker writes nothing more: its lease is gone.
    if views.is_some() {
        lease_lost(
            repo.write_rebuild_views(&env.lease("failed"), &[put("second")])
                .await,
            "the failed worker's next batch",
        )?;
        ensure!(
            row(pk.clone(), "second").await?.is_none(),
            "the lost lease wrote nothing"
        );
    }

    // A new rebuild runs to its end: commits stay refused while it runs, and resume once it ends.
    env.begin("recovery", 3600).await?;
    refused(
        plain.commit(repo, plain.events(1..=1), None).await,
        "a commit during the recovery",
    )?;
    if views.is_some() {
        repo.write_rebuild_views(&env.lease("recovery"), &[put("first"), put("second")])
            .await?;
    }
    let settled = env.end("recovery").await?;
    ensure!(
        settled.is_settled() && !settled.force_released,
        "settled: {settled:?}"
    );

    plain.commit(repo, plain.events(1..=1), None).await?;
    ensure_eq!(repo.events(&plain.id).await?.len(), 1, "the plain stream");
    if views.is_some() {
        repo.commit_transaction(live()).await?;
        ensure_eq!(repo.events(&viewed.id).await?.len(), 1, "the viewed stream");
        ensure!(
            row(live_pk.clone(), "row").await?.is_some(),
            "the commit's view row"
        );
        ensure!(
            row(pk.clone(), "second").await?.is_some(),
            "the recovery's view row"
        );
    }
    repo.commit_transaction(outcome_only()).await?;
    ensure!(
        repo.outcome(&only).await?.is_some(),
        "the outcome is recorded once the rebuild ends"
    );

    Ok(())
}

/// The lock check outranks every other failed item of a commit, and a refusal the manifest alone
/// decides precedes it.
async fn precedence<R, H>(env: &Env<'_, R>, heads: Option<&H>) -> CaseResult
where
    R: Repository + RebuildFence + FeatureMarker,
    H: HeadFixtures<R>,
{
    let repo = env.repo;

    // A head conflict, an existing outcome row, a gap, and drift.
    let moved = Stream::new(&env.ns, "moved", "ledger");
    moved.commit(repo, moved.events(1..=1), None).await?;
    let duplicate = CommandId::new(format!("{}-duplicate", env.ns));
    let duplicate_commit = || {
        TransactionCommit::<R::Operation>::new(Vec::new())
            .with_outcome(Outcome::accepted(duplicate.clone(), "digest"))
    };
    repo.commit_transaction(duplicate_commit()).await?;
    let gap = Stream::new(&env.ns, "gap", "ledger");
    gap.commit(repo, gap.events(1..=1), None).await?;
    let drifted = Stream::new(&env.ns, "drifted", "ledger");
    drifted.commit(repo, drifted.events(1..=1), None).await?;
    if let Some(heads) = heads {
        heads.append_bypassing_head(&drifted.event(2, 2)).await?;
    }

    env.begin("worker", 3600).await?;

    // Without the lock each of these would fail with its own error; with it, all report the lock.
    refused(
        moved.commit(repo, moved.events(1..=1), None).await,
        "a head conflict",
    )?;
    refused(
        gap.commit(repo, gap.events(5..=5), None).await,
        "a sequence gap",
    )?;
    refused(
        repo.commit_transaction(duplicate_commit()).await,
        "an existing outcome row",
    )?;
    if heads.is_some() {
        refused(
            drifted.commit(repo, drifted.events(3..=3), None).await,
            "drift",
        )?;
    }
    let other = Stream::new(&env.ns, "other", "ledger");
    refused(
        repo.commit_transaction(
            TransactionCommit::new(vec![write(&other, 1..=1)]).with_fences([ReadFence::new(
                moved.id.clone(),
                moved.entity_type.clone(),
                EventSequence(9),
                FenceProvenance::Caller,
            )]),
        )
        .await,
        "a stale caller-stamped fence",
    )?;

    // A fence on a retained type is refused by the manifest alone, before the lock is read. The
    // declaration bumps the manifest, so a writer that cached the old version meets its marker
    // check first, which outranks the lock: it revalidates and is refused for the retained type.
    let retained = EntityType::new(format!("{}-retained", env.ns));
    let declared = retained.as_str().to_owned();
    repo.update_manifest(|manifest| {
        manifest.retention.insert(declared.clone());
    })
    .await?;
    let result = repo
        .commit_transaction(
            TransactionCommit::new(Vec::new()).with_fences([ReadFence::new(
                format!("{}-r", env.ns),
                retained.clone(),
                EventSequence(0),
                FenceProvenance::Engine,
            )]),
        )
        .await;
    repo.update_manifest(|manifest| {
        manifest.retention.remove(&declared);
    })
    .await?;
    expect_err(result, "a fence on a retained type", |err| {
        matches!(err, Error::RetentionConflict { .. })
    })?;

    // Once the lock is gone each commit reports the failure it would have had.
    env.end("worker").await?;
    expect_err(
        gap.commit(repo, gap.events(5..=5), None).await,
        "the gap after the rebuild",
        |err| matches!(err, Error::SequenceGap { .. }),
    )?;
    expect_err(
        repo.commit_transaction(duplicate_commit()).await,
        "the outcome conflict after the rebuild",
        |err| matches!(err, Error::OutcomeConflict { .. }),
    )?;
    if heads.is_some() {
        expect_err(
            drifted.commit(repo, drifted.events(3..=3), None).await,
            "the drift after the rebuild",
            |err| matches!(err, Error::StreamHeadDrift { .. }),
        )?;
    }

    Ok(())
}

async fn epochs<R: Repository + RebuildFence>(env: &Env<'_, R>) -> CaseResult {
    let repo = env.repo;
    let before = env.generation().await?;

    // A settled generation is an epoch, and a read under it verifies.
    let epoch = observe_rebuild_epoch(repo).await?;
    ensure_eq!(epoch.generation(), before.value, "the epoch");
    verify_rebuild_epoch(repo, epoch).await?;
    let ran = AtomicUsize::new(0);
    read_at_rebuild_epoch(repo, |_| async {
        ran.fetch_add(1, Ordering::SeqCst);

        Ok::<_, Error>(())
    })
    .await?;
    ensure_eq!(ran.load(Ordering::SeqCst), 1, "the read ran once");

    // A rebuild that starts during the read fails the verify: the generation is odd.
    let result = read_at_rebuild_epoch(repo, |_| async {
        env.begin("a", 3600).await.map_err(failure_error)?;

        Ok::<_, Error>(())
    })
    .await;
    match result {
        Err(error @ Error::RebuildUnavailable { .. }) => {
            ensure!(
                matches!(error, Error::RebuildUnavailable { generation, force_released: false }
                    if generation == before.value + 1),
                "the odd generation: {error}"
            );
            ensure!(error.is_retryable(), "it is transient");
        }
        other => return Err(Failure(format!("a rebuild during the read: {other:?}"))),
    }

    // While it runs, observing fails and the read is skipped.
    ran.store(0, Ordering::SeqCst);
    expect_err(
        read_at_rebuild_epoch(repo, |_| async {
            ran.fetch_add(1, Ordering::SeqCst);

            Ok::<_, Error>(())
        })
        .await,
        "a read during a rebuild",
        |err| matches!(err, Error::RebuildUnavailable { .. }),
    )?;
    ensure_eq!(
        ran.load(Ordering::SeqCst),
        0,
        "a failed observe skips the read"
    );

    // A rebuild that started and finished during the read leaves a settled but changed generation.
    env.end("a").await?;
    let result = read_at_rebuild_epoch(repo, |_| async {
        env.begin("b", 3600).await.map_err(failure_error)?;
        repo.end_rebuild(&env.lease("b")).await?;

        Ok::<_, Error>(())
    })
    .await;
    match result {
        Err(Error::RebuildEpochChanged { observed, current }) => {
            ensure_eq!(
                (observed, current),
                (before.value + 2, before.value + 4),
                "the two generations"
            );
        }
        other => return Err(Failure(format!("a finished rebuild: {other:?}"))),
    }

    // A failed read is returned as it is, without verifying.
    expect_err(
        read_at_rebuild_epoch(repo, |_| async {
            Err::<(), _>(Error::InvalidArgument("boom".into()))
        })
        .await,
        "a failed read",
        |err| matches!(err, Error::InvalidArgument(message) if message == "boom"),
    )?;

    // A takeover during a read is a changed epoch too: odd to a different odd.
    env.begin("c", 1).await?;
    let stalled = env.generation().await?;
    env.clock.advance(3);
    ensure_eq!(
        env.begin("d", 3600).await?.generation,
        stalled.value + 2,
        "the takeover's generation"
    );
    env.end("d").await?;

    Ok(())
}

/// The lock fences commits only: an out-of-band snapshot write and compaction carry no lock check,
/// and the two rows, in the reserved namespace, never show up as streams.
async fn maintenance<R: Repository + RebuildFence>(env: &Env<'_, R>) -> CaseResult {
    let repo = env.repo;
    let stream = Stream::new(&env.ns, "maintained", "ledger");
    stream.commit(repo, stream.events(1..=3), None).await?;

    env.begin("worker", 3600).await?;

    ensure!(
        repo.put_snapshot(&stream.snapshot(1, 3, "{}")).await?,
        "a snapshot write during a rebuild"
    );
    ensure_eq!(
        repo.compact_below(&stream.id, EventSequence(3)).await?,
        3,
        "compaction during a rebuild"
    );
    let heads = repo.entity_heads(None).await?;
    ensure_eq!(
        heads.get(&stream.id),
        Some(&EventSequence(3)),
        "the stream's head"
    );
    ensure!(
        heads
            .keys()
            .all(|id| !id.as_str().starts_with(crate::features::SYSTEM_PREFIX)),
        "no reserved row is a stream: {heads:?}"
    );
    refused(
        stream.commit(repo, stream.events(4..=4), None).await,
        "the stream's next commit",
    )?;

    env.end("worker").await?;
    stream.commit(repo, stream.events(4..=4), None).await?;

    Ok(())
}

async fn guarded_views<R, T>(env: &Env<'_, R>, views: &T) -> CaseResult
where
    R: Repository + RebuildFence,
    T: CommitViewTarget<R>,
{
    let repo = env.repo;
    let pk = format!("{}#rebuilt", env.ns);
    let put = |sk: &str| {
        ProjectedView::new(
            "rebuild",
            0,
            0,
            ViewOperation::put(pk.clone(), sk, "owner", "test", "{}"),
        )
    };
    let absent = |sk: &'static str| {
        let pk = pk.clone();

        async move {
            let row = views
                .row(&pk, sk)
                .await
                .map_err(|err| Failure(err.to_string()))?;
            ensure!(row.is_none(), "no row {sk}: {row:?}");

            Ok::<_, Failure>(())
        }
    };

    // Without a lock there is nothing to guard the write: it is refused and writes nothing.
    lease_lost(
        repo.write_rebuild_views(&env.lease("a"), &[put("none")])
            .await,
        "a write with no lock",
    )?;
    absent("none").await?;

    env.begin("a", 10).await?;
    repo.write_rebuild_views(&env.lease("a"), &[put("one"), put("two")])
        .await?;
    for sk in ["one", "two"] {
        let row = views
            .row(&pk, sk)
            .await
            .map_err(|err| Failure(err.to_string()))?;
        ensure!(
            row.is_some_and(|row| row.payload == b"{}"),
            "the row {sk} was written"
        );
    }

    // Another owner's batch is refused, whole.
    lease_lost(
        repo.write_rebuild_views(&env.lease("b"), &[put("foreign")])
            .await,
        "another owner's write",
    )?;
    absent("foreign").await?;

    // A batch beyond the limit and an empty one are refused before any write.
    let batch: Vec<_> = (0..=MAX_REBUILD_VIEW_BATCH)
        .map(|index| put(&format!("bulk-{index:03}")))
        .collect();
    expect_err(
        repo.write_rebuild_views(&env.lease("a"), &batch).await,
        "a batch over the limit",
        |err| matches!(err, Error::CommitTooLarge { actions: 101, .. }),
    )?;
    repo.write_rebuild_views(&env.lease("a"), &batch[..MAX_REBUILD_VIEW_BATCH])
        .await?;
    expect_err(
        repo.write_rebuild_views(&env.lease("a"), &[]).await,
        "an empty batch",
        |err| matches!(err, Error::InvalidArgument(_)),
    )?;

    // A lapsed lease is no guard, even before anyone takes over.
    env.clock.advance(11);
    lease_lost(
        repo.write_rebuild_views(&env.lease("a"), &[put("lapsed")])
            .await,
        "a write under a lapsed lease",
    )?;
    absent("lapsed").await?;

    // The worker that lost its lock to a takeover writes nothing; the new owner can.
    env.begin("b", 3600).await?;
    lease_lost(
        repo.write_rebuild_views(&env.lease("a"), &[put("displaced")])
            .await,
        "the displaced worker's write",
    )?;
    absent("displaced").await?;
    repo.write_rebuild_views(&env.lease("b"), &[put("taken-over")])
        .await?;
    ensure!(
        views
            .row(&pk, "taken-over")
            .await
            .map_err(|err| Failure(err.to_string()))?
            .is_some(),
        "the new owner's row"
    );
    env.end("b").await?;

    Ok(())
}

/// Wraps a repository so a rebuild ends during the engine's `n`-th backoff, and counts them.
struct EndsDuringBackoff<'a, R> {
    inner: &'a R,
    owner: Mutex<Option<(RebuildLease, usize)>>,
    waits: AtomicUsize,
}

impl<'a, R: Repository + RebuildFence> EndsDuringBackoff<'a, R> {
    fn new(inner: &'a R) -> Self {
        Self {
            inner,
            owner: Mutex::new(None),
            waits: AtomicUsize::new(0),
        }
    }

    /// Ends the lease's rebuild during the `wait`-th backoff from now.
    fn end_during(&self, owner: Option<(RebuildLease, usize)>) {
        *self.owner.lock().unwrap_or_else(|p| p.into_inner()) = owner;
        self.waits.store(0, Ordering::SeqCst);
    }
}

impl<R: Repository + RebuildFence> Repository for EndsDuringBackoff<'_, R> {
    type Operation = R::Operation;

    fn writes_views(&self) -> bool {
        self.inner.writes_views()
    }

    async fn commit(&self, commit: Commit<R::Operation>) -> Result<(), Error> {
        self.inner.commit(commit).await
    }

    async fn commit_transaction(
        &self,
        commit: TransactionCommit<R::Operation>,
    ) -> Result<(), Error> {
        self.inner.commit_transaction(commit).await
    }

    fn backoff(&self, _: Duration) -> impl Future<Output = ()> + Send {
        let waits = self.waits.fetch_add(1, Ordering::SeqCst) + 1;
        let due = self
            .owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .filter(|(_, at)| *at == waits)
            .map(|(lease, _)| lease);

        async move {
            if let Some(lease) = due {
                self.inner
                    .end_rebuild(&lease)
                    .await
                    .expect("end the rebuild during the backoff");
            }
        }
    }

    async fn outcome(&self, command_id: &CommandId) -> Result<Option<Outcome>, Error> {
        self.inner.outcome(command_id).await
    }

    async fn events(&self, entity_id: &EntityId) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.events(entity_id).await
    }

    async fn events_after(
        &self,
        entity_id: &EntityId,
        after: EventSequence,
    ) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.events_after(entity_id, after).await
    }

    async fn latest(&self, entity_id: &EntityId, n: usize) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.latest(entity_id, n).await
    }

    async fn snapshot(&self, entity_id: &EntityId) -> Result<Option<SerializedSnapshot>, Error> {
        self.inner.snapshot(entity_id).await
    }

    async fn put_snapshot(&self, snapshot: &SerializedSnapshot) -> Result<bool, Error> {
        self.inner.put_snapshot(snapshot).await
    }

    async fn compact_below(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> Result<usize, Error> {
        self.inner.compact_below(entity_id, through).await
    }

    async fn entity_heads(
        &self,
        entity_type: Option<&EntityType>,
    ) -> Result<BTreeMap<EntityId, EventSequence>, Error> {
        self.inner.entity_heads(entity_type).await
    }
}

/// A refused commit is transient, and the engine retries it: a command that meets a rebuild waits
/// out the backoff and succeeds when the rebuild ends; one that never does gets the error back, not
/// a stale-fence rejection and not an outcome; a rejection decided during a rebuild, a
/// caller-stamped fence, and a reader's epoch error are never recorded either.
async fn engine_retry<R: Repository + RebuildFence>(env: &Env<'_, R>) -> CaseResult {
    let repo = env.repo;
    let wrapper = EndsDuringBackoff::new(repo);
    let store = Store::<Ledger, _>::new(&wrapper, ())
        .with_snapshot_interval(0)
        .with_outcomes(outcome_config().with_retry(RetryPolicy::new(4)));
    let id = EntityId::new(format!("{}-ledger", env.ns));

    // The lock is released during the second backoff: the third attempt commits.
    env.begin("worker", 3600).await?;
    wrapper.end_during(Some((env.lease("worker"), 2)));
    let command = command_id();
    let outcome = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await?;
    ensure!(outcome.is_accepted(), "the command commits: {outcome:?}");
    ensure_eq!(wrapper.waits.load(Ordering::SeqCst), 2, "two backoffs");
    ensure_eq!(repo.events(&id).await?.len(), 1, "one event");

    // A rebuild that never ends: the attempts run out and the error comes back as it is.
    env.begin("stuck", 3600).await?;
    wrapper.end_during(None);
    let stuck = command_id();
    refused(
        store
            .execute_with_outcome(&id, Add(7), command_metadata(&stuck))
            .await,
        "a command that never gets past the rebuild",
    )?;
    ensure_eq!(
        wrapper.waits.load(Ordering::SeqCst),
        3,
        "four attempts, three backoffs"
    );
    ensure!(repo.outcome(&stuck).await?.is_none(), "no outcome recorded");
    ensure_eq!(repo.events(&id).await?.len(), 1, "nothing was written");

    // A rejection decided while a rebuild runs cannot be recorded: the outcome-only transaction
    // carries the lock check too.
    let rejected = command_id();
    refused(
        store
            .execute_with_outcome(&id, Add(0), command_metadata(&rejected))
            .await,
        "a rejection during a rebuild",
    )?;
    ensure!(
        repo.outcome(&rejected).await?.is_none(),
        "no rejection recorded"
    );

    // A caller-stamped fence does not turn the refusal into a stale-fence rejection either.
    let referral = EntityId::new(format!("{}-referral", env.ns));
    let identity = CommandIdentity::new("ledger:rename").with_targets([id.clone()]);
    let stamped = command_id();
    let result = store
        .execute_transaction(&identity, &command_metadata(&stamped), |tx| {
            let (id, referral) = (id.clone(), referral.clone());

            Box::pin(async move {
                let mut ledger = tx.read::<Ledger>(&id).await?;
                tx.append(&mut ledger, added(1))?;
                tx.fence(ReadFence::new(
                    referral,
                    Ledger::TYPE,
                    EventSequence(3),
                    FenceProvenance::Caller,
                ))?;

                Ok(Decision::accept())
            })
        })
        .await;
    refused(result, "a caller-stamped fence during a rebuild")?;
    ensure!(
        repo.outcome(&stamped).await?.is_none(),
        "no stale-fence rejection"
    );

    // A decision made on derived state returns the reader's epoch error as its own: returned at
    // once, with no attempt repeated and no outcome recorded.
    wrapper.end_during(None);
    let reader = command_id();
    let calls = AtomicUsize::new(0);
    let result = store
        .execute_transaction(&identity, &command_metadata(&reader), |tx| {
            let calls = &calls;

            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                read_at_rebuild_epoch(repo, |_| async { Ok::<_, Error>(()) }).await?;

                let _ = tx;

                Ok(Decision::accept())
            })
        })
        .await;
    expect_err(result, "a decision made during a rebuild", |err| {
        matches!(err, Error::RebuildUnavailable { .. })
    })?;
    ensure_eq!(calls.load(Ordering::SeqCst), 1, "it is not retried");
    ensure_eq!(
        wrapper.waits.load(Ordering::SeqCst),
        0,
        "and nothing waited"
    );
    ensure!(
        repo.outcome(&reader).await?.is_none(),
        "no outcome recorded"
    );

    // Once the rebuild ends the same command IDs run: the stuck command commits, the rejection is
    // recorded.
    env.end("stuck").await?;
    ensure!(
        store
            .execute_with_outcome(&id, Add(7), command_metadata(&stuck))
            .await?
            .is_accepted(),
        "the stuck command commits"
    );
    ensure!(
        store
            .execute_with_outcome(&id, Add(0), command_metadata(&rejected))
            .await?
            .is_rejected(),
        "the rejection is recorded"
    );

    Ok(())
}

export const repoUrl = 'https://github.com/photon-grove/evt'

// Lang is the language the reader picked; every code sample on the landing page has both.
export type Lang = 'go' | 'rust'

export const langs: {id: Lang; label: string}[] = [
  {id: 'rust', label: 'Rust'},
  {id: 'go', label: 'Go'},
]

export type PerLang<T> = Record<Lang, T>

export const installCommand: PerLang<string> = {
  rust: 'cargo add photon-evt --features dynamodb',
  go: 'go get github.com/photon-grove/evt/go',
}

// The quick start is the root README's counter example, trimmed to its moving parts. The README's
// copies run in CI (go/internal/docexamples and the photon-evt doctests), so keep these in step.
export const quickStart: PerLang<{file: string; code: string}> = {
  go: {
    file: 'counter.go',
    code: `func (c *Counter) Handle(_ context.Context, cmd evt.Command) (evt.CommandResult, error) {
	inc, ok := cmd.(*Increment)
	if !ok {
		return evt.CommandResult{}, evt.NewBadCommandError(cmd)
	}
	return evt.CommandResult{Events: []evt.Event{Incremented{ID: c.ID, By: inc.By}}}, nil
}

func (c *Counter) Apply(event evt.Event) error {
	c.Total += event.(Incremented).By
	return nil
}

func main() {
	ctx := context.Background()
	store := mem.NewStore() // DynamoDB or PostgreSQL in production

	counter := &Counter{ID: "counter-1"}
	err := store.Execute(ctx, counter, "counter-1", &Increment{By: 2}, evt.Metadata{})
	// counter.Total == 2, and the log holds one counter:incremented event
}`,
  },
  rust: {
    file: 'counter.rs',
    code: `domain_events! {
    pub enum CounterEvent {
        Incremented(Incremented) => ("counter:incremented", 1),
    }
}

impl Aggregate for Counter {
    const TYPE: &'static str = "counter";

    type Command = i64;
    type Event = CounterEvent;
    type Error = std::convert::Infallible;
    type Services = ();

    async fn handle(&self, by: i64, _: &()) -> Result<Vec<CounterEvent>, Self::Error> {
        Ok(vec![Incremented { by }.into()])
    }

    fn apply(&mut self, event: CounterEvent) {
        match event {
            CounterEvent::Incremented(e) => self.total += e.by,
        }
    }
}

let store = Store::<Counter, _>::new(MemRepository::new(), ());
let executed = store.execute("counter-1", 2, Metadata::new("us-west-2")).await?;
assert_eq!(executed.state.aggregate().total, 2);`,
  },
}

// One stop on the concepts tour: what the piece does, why it matters, and the code for it. The
// snippets are trimmed from go/examples/banking, crates/evt/examples, and the guides.
export interface TourStop {
  id: string
  title: string
  tagline: string
  body: string
  doc: string
  code: PerLang<string>
}

export const tour: TourStop[] = [
  {
    id: 'aggregate',
    title: 'Aggregate',
    tagline: 'State is a fold over events',
    body: 'An aggregate is one stream in the log. Its state is whatever you get by applying its events in order, so loading it is a replay from the last snapshot. Apply trusts the event and never validates.',
    doc: 'guide/concepts',
    code: {
      go: `func (a *Account) Apply(event evt.Event) error {
	switch e := event.(type) {
	case AccountOpened:
		a.ID, a.Balance, a.Opened = e.AccountID, e.Balance, true
	case MoneyDeposited:
		a.Balance += e.Amount
	}
	return nil
}`,
      rust: `fn apply(&mut self, event: AccountEvent) {
    match event {
        AccountEvent::Opened(e) => {
            self.id = e.account_id;
            self.balance = e.balance;
            self.opened = true;
        }
        AccountEvent::Deposited(e) => self.balance += e.amount,
        AccountEvent::Withdrawn(e) => self.balance -= e.amount,
    }
}`,
    },
  },
  {
    id: 'command',
    title: 'Command',
    tagline: 'Decide, then record',
    body: 'Handle validates a command against current state and returns the events it decides on, without changing anything. An error that is a rejection is stored too, so a retry gets the same answer.',
    doc: 'guide/commands',
    code: {
      go: `func (a *Account) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
	switch cmd := command.(type) {
	case *OpenAccount:
		if a.Opened {
			return evt.CommandResult{}, evt.NewConflictError("account already opened")
		}
		return evt.CommandResult{Events: []evt.Event{AccountOpened{
			AccountID: cmd.AccountID,
			Balance:   cmd.InitialBalance,
		}}}, nil
	}
	return evt.CommandResult{}, evt.NewBadCommandError(command)
}`,
      rust: `async fn handle(&self, command: AccountCommand, _: &()) -> Result<Vec<AccountEvent>, AccountError> {
    let account_id = self.id.clone();

    match command {
        AccountCommand::Open { .. } if self.opened => Err(AccountError::AlreadyOpened),
        AccountCommand::Open { account_id, initial_balance } => {
            Ok(vec![AccountOpened { account_id, balance: initial_balance }.into()])
        }
        AccountCommand::Deposit { amount } => {
            self.check_open(amount)?;
            Ok(vec![MoneyDeposited { account_id, amount }.into()])
        }
        // ...
    }
}`,
    },
  },
  {
    id: 'commit',
    title: 'Commit',
    tagline: 'One conditional write',
    body: 'The store loads the stream, runs the command, and commits the events with an optimistic check on the stream head. Events, head, view rows and the command outcome land together or not at all.',
    doc: 'guide/getting-started',
    code: {
      go: `repo := dynamo.NewRepository(client, "event-log")
store := snapshots.NewStore(repo, 25) // snapshot every 25 events

metadata := evt.NewMetadata(ctx, nil, evt.WithCommandID("cmd-7f3a"))
err := store.Execute(ctx, NewAccount("acct-1"), "acct-1",
	&Deposit{AccountID: "acct-1", Amount: 50}, metadata)
// a racing writer gets a conflict, never a silent overwrite,
// and a retry with the same command ID is caught as a duplicate`,
      rust: `let repository = DynamoRepository::new(client, "event-log");
let store = Store::<Account, _>::new(repository, ());

let executed = store
    .execute("acct-1", AccountCommand::Deposit { amount: 50 }, Metadata::new("us-west-2"))
    .await?;
// a racing writer gets Error::Conflict, never a silent overwrite`,
    },
  },
  {
    id: 'projector',
    title: 'Projector',
    tagline: 'Views in the same transaction',
    body: 'A commit-wide projector is a pure function over everything one commit wrote. Its view rows go into the same transaction as the events, so a read model never drifts from the log behind it.',
    doc: 'guide/projections',
    code: {
      go: `type directory struct{}

func (directory) Name() string              { return "directory" }
func (directory) History() []evt.EntityType { return nil } // current state, no events

func (directory) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	var operations []evt.ViewOperation

	for _, stream := range commit.Streams {
		// stream.State is the aggregate's JSON after the commit; the stream owns its row.
		operations = append(operations, evt.PutView("directory",
			string(stream.EntityID), stream.EntityID, stream.EntityType, stream.State))
	}

	return operations, nil
}

store := snapshots.NewStore(repo, 25).WithCommitProjectors(directory{})`,
      rust: `impl CommitProjector for Directory {
    fn name(&self) -> &str {
        "directory"
    }

    fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
        Ok(commit.streams.iter().map(|stream| {
            ViewOperation::put("directory", stream.entity_id.as_str(),
                stream.entity_id.clone(), stream.entity_type.as_str(), stream.state.clone())
        }).collect())
    }
}

let store = Store::<Account, _>::new(repository, ()).with_commit_projector(Arc::new(Directory));`,
    },
  },
  {
    id: 'derived',
    title: 'Rebuild and sync',
    tagline: 'Derived state you can throw away',
    body: 'Views are derived, so the log can reproduce them. In Rust, the sync journal hands each scope an ordered, gap-checked feed for realtime clients. In Go, RebuildProjections replays the log into per-entity views behind a rebuild fence; the Rust port is in progress.',
    doc: 'guide/projections',
    code: {
      go: `lease, err := fence.Begin(ctx, owner, 10*time.Minute) // fence out writers and readers

result, err := evt.RebuildProjections(ctx, repo, applyEvent, cfg)
if err == nil && len(result.Errors) == 0 {
	_, err = fence.End(ctx, lease) // readers trust the views again
}`,
      rust: `let scope = Scope::new("all")?;
let mut after = 0;
loop {
    let page = repository.page(&scope, after, 100).await?;
    // deliver page.rows to the client, in order
    after = page.next_after();
    if !page.has_more() {
        break; // caught up with repository.high_watermark(&scope)
    }
}`,
    },
  },
]

export type FeatureIcon =
  | 'commit'
  | 'streams'
  | 'outcome'
  | 'view'
  | 'snapshot'
  | 'fence'
  | 'journal'
  | 'lambda'
  | 'compat'

// A linked content card. `doc`, when set, is the slug of the in-site guide it links to.
export interface ContentCard {
  title: string
  body: string
  doc?: string
  icon?: FeatureIcon
  tag?: string
}

export const capabilities: ContentCard[] = [
  {
    icon: 'commit',
    title: 'Optimistic, atomic commits',
    body: 'Each command commits in one conditional write against the stream head. A writer that raced you gets a conflict, never a silent overwrite.',
    doc: 'guide/getting-started',
  },
  {
    icon: 'streams',
    title: 'Multi-stream transactions',
    body: 'Append to several aggregates in one commit, and put read fences on the streams you only looked at.',
    doc: 'guide/commands',
  },
  {
    icon: 'outcome',
    title: 'Durable command outcomes',
    body: 'Every command gets a stored outcome, accepted or rejected. A retry gets the original answer back instead of running twice.',
    doc: 'guide/commands',
  },
  {
    icon: 'view',
    title: 'Commit-wide projectors',
    body: 'View rows commit in the same transaction as the events that caused them, at any key, across every stream the commit touched.',
    doc: 'guide/projections',
  },
  {
    icon: 'snapshot',
    title: 'Snapshots and upcasters',
    body: 'Inline snapshots keep loads short, verified compaction trims old history, and upcasters let payloads change shape safely.',
    doc: 'guide/concepts',
  },
  {
    icon: 'journal',
    title: 'Sync journal',
    tag: 'Rust',
    body: 'Per-scope, gap-checked delivery rows written in the commit, with compaction and a 410 Gone handoff for clients that fall behind.',
    doc: 'reference/storage-format',
  },
  {
    icon: 'lambda',
    title: 'Lambda stream helpers',
    tag: 'Rust',
    body: 'photon-evt-lambda decodes DynamoDB Streams records into events, builds filter patterns, and returns partial-batch failures.',
    doc: 'guide/streams',
  },
  {
    icon: 'fence',
    title: 'Fenced rebuilds',
    tag: 'Go',
    body: 'RebuildProjections replays the log into per-entity views while a rebuild fence keeps writers out and tells readers when the views are settled. The Rust port is in progress.',
    doc: 'guide/projections',
  },
  {
    icon: 'compat',
    title: 'Rust and Go, one format',
    body: 'Both languages read and write the same bytes, checked by golden fixtures and live interleaved runs against each backend.',
    doc: 'reference/feature-matrix',
  },
]

export interface Backend {
  name: string
  body: string
  detail: string
  doc: string
}

export const backends: Backend[] = [
  {
    name: 'DynamoDB',
    body: 'A single-table event log with stable pk/sk keys, the snapshot inline at sk = 0, and one transaction per commit.',
    detail: 'pk = entity id · sk = sequence',
    doc: 'guide/dynamodb',
  },
  {
    name: 'PostgreSQL',
    body: 'An events table and a stream-heads table in one SQL transaction, with a schema the repository creates for you.',
    detail: 'EnsureSchema · view writers',
    doc: 'guide/postgres',
  },
  {
    name: 'In-memory',
    body: 'The same contract with no infrastructure. Unit tests run fast and deterministic, then the aggregate moves to production unchanged.',
    detail: 'mem.NewStore() · MemRepository',
    doc: 'guide/getting-started',
  },
]

export interface PackageEntry {
  name: string
  body: string
  href: string
}

const pkgGo = 'https://pkg.go.dev/github.com/photon-grove/evt/go'

export const packages: PerLang<PackageEntry[]> = {
  go: [
    {name: 'evt/go', body: 'The core contracts: aggregates, commands, events, metadata, transactions, repositories and rebuilds.', href: pkgGo},
    {name: 'backends/mem', body: 'In-memory repository and store for unit tests.', href: `${pkgGo}/backends/mem`},
    {name: 'backends/dynamo', body: 'DynamoDB event log, snapshots, views, stream helpers and transaction groups.', href: `${pkgGo}/backends/dynamo`},
    {name: 'backends/postgres', body: 'PostgreSQL event log and snapshots behind the same evt.Repository contract.', href: `${pkgGo}/backends/postgres`},
    {name: 'snapshots', body: 'The store that loads from snapshots and writes them on a threshold.', href: `${pkgGo}/snapshots`},
    {name: 'stream', body: 'The stream envelope and the DynamoDB Streams handler.', href: `${pkgGo}/stream`},
    {name: 'viewcodec', body: 'A typed JSON helper around evt.ViewRepository.', href: `${pkgGo}/viewcodec`},
    {name: 'conformance', body: 'The backend-neutral suite a custom evt.Repository should pass.', href: `${pkgGo}/conformance`},
    {name: 'cmd/evt-doctor', body: 'A CLI that initializes and inspects an event log and runs the stream-heads migration.', href: `${pkgGo}/cmd/evt-doctor`},
  ],
  rust: [
    {name: 'photon-evt', body: 'The aggregate trait, store, transactions, outcomes and views. In-memory by default.', href: 'https://docs.rs/photon-evt'},
    {name: 'feature dynamodb', body: 'The DynamoDB event log and view repository on the AWS SDK.', href: 'https://docs.rs/photon-evt'},
    {name: 'feature postgres', body: 'The PostgreSQL event log on tokio-postgres with a deadpool pool.', href: 'https://docs.rs/photon-evt'},
    {name: 'feature journal', body: 'The sync journal: scopes, high watermarks, gap-checked paging and compaction.', href: 'https://docs.rs/photon-evt'},
    {name: 'feature testing', body: 'Given/when/then helpers over MemRepository, plus the conformance fixtures.', href: 'https://docs.rs/photon-evt'},
    {name: 'photon-evt-lambda', body: 'DynamoDB Streams decode, event-source filters, journal wake-ups and partial-batch failures.', href: 'https://docs.rs/photon-evt-lambda'},
  ],
}

export interface ExampleEntry {
  title: string
  body: string
  lang: 'Go' | 'Rust'
  command: string
  href: string
}

export const examples: ExampleEntry[] = [
  {
    title: 'Banking',
    lang: 'Rust',
    body: 'Typed commands and rejections, a transfer across two accounts in one transaction, a balance view, and given/when/then tests.',
    command: 'cargo run -p photon-evt --example banking --features testing',
    href: `${repoUrl}/blob/main/crates/evt/examples/banking.rs`,
  },
  {
    title: 'Todo with a rebuildable view',
    lang: 'Rust',
    body: 'A CommitProjector keeps each list\'s open and done counts in the commit, and group_commits plus replay_views reproduce the row from the log.',
    command: 'cargo run -p photon-evt --example todo',
    href: `${repoUrl}/blob/main/crates/evt/examples/todo.rs`,
  },
  {
    title: 'Lambda stream handler',
    lang: 'Rust',
    body: 'Decode a DynamoDB Streams batch, project it idempotently, and answer a failed record with a partial batch failure.',
    command: 'cargo run -p photon-evt-lambda --example stream_handler',
    href: `${repoUrl}/blob/main/crates/evt-lambda/examples/stream_handler.rs`,
  },
  {
    title: 'Sync journal',
    lang: 'Rust',
    body: 'Map events to scopes, journal every commit, and poll a scope page by page with gap detection.',
    command: 'cargo run -p photon-evt --example journal --features journal,testing',
    href: `${repoUrl}/blob/main/crates/evt/examples/journal.rs`,
  },
  {
    title: 'Banking',
    lang: 'Go',
    body: 'Open an account and deposit to it on the in-memory store, with conflicts and not-found errors.',
    command: 'go -C go test ./examples/banking',
    href: `${repoUrl}/tree/main/go/examples/banking`,
  },
  {
    title: 'Todo with a rebuildable view',
    lang: 'Go',
    body: 'A commit-wide projector keeps each list\'s open and done counts in the commit, then replays the commits in memory and checks the rows match.',
    command: 'go -C go run ./examples/todo',
    href: `${repoUrl}/tree/main/go/examples/todo`,
  },
  {
    title: 'Transfer with outcomes',
    lang: 'Go',
    body: 'Debit one account and credit another in one commit, replay a retry from its stored outcome, and store an overdraft as a rejection.',
    command: 'go -C go run ./examples/transfer',
    href: `${repoUrl}/tree/main/go/examples/transfer`,
  },
]

export const cookbook: ContentCard[] = [
  {
    title: 'Single-table event log',
    body: 'One DynamoDB table holds the events and their inline snapshots, with pk as the entity ID and sk as the sequence number.',
    doc: 'guide/dynamodb',
  },
  {
    title: 'Command deduplication',
    body: 'Put a stable command ID in the metadata, and a retried command is caught as a duplicate instead of recording the same fact twice.',
    doc: 'guide/integration',
  },
  {
    title: 'Schema evolution',
    body: 'Bump the event version when a payload changes shape, and register an upcaster with a fixture for every old shape.',
    doc: 'guide/concepts',
  },
  {
    title: 'Stream fan-out',
    body: 'Forward only the event-log INSERT records. Drop malformed rows on purpose and return partial-batch failures for the rest.',
    doc: 'guide/streams',
  },
  {
    title: 'Moving from v0.8',
    body: 'Initialize the table, migrate to per-stream head items, and move off the deprecated surfaces before the next minor.',
    doc: 'guide/upgrading-to-v0.9',
  },
  {
    title: 'Local integration',
    body: 'Run Moto or PostgreSQL in Docker, apply the local Terraform, and point the integration suites at it.',
    doc: 'guide/integration',
  },
]

# evt

evt is an event-sourcing library for Go, with a native Rust implementation that reads and writes
the same log. Event sourcing means the record of truth is an append-only log of events, and an
aggregate's state (an account, an order, a document) is whatever you get by folding its events in
order. You don't adopt a runtime or inherit from a base type. You implement a handful of interfaces
and use the packages you need.

You'd reach for it when the history matters as much as the current state: an audit trail you can
trust, read models you can delete and rebuild, and retries that never record the same fact twice.
And because the Go and Rust code share one storage format byte for byte, a Go service and a Rust
service can append to the same streams.

Here's the core idea. A command loads an aggregate from its stream, decides which events to emit,
and commits them in one conditional write. On DynamoDB that's a transaction that puts each event
item under `pk = <entity id>` and `sk = <sequence>`, and raises the stream's head item only if it
still holds the sequence the command loaded. On PostgreSQL it's one SQL transaction over the events
table and its stream-heads table. If another writer got there first the commit fails as a
conflict, and nothing is written. Projectors can add view rows to that same commit, so a read model
stays in step with the events behind it, and a rebuild can reproduce it from the log.

## Go

```sh
go get github.com/photon-grove/evt/go
```

An aggregate handles commands and applies events. This counter emits an `Incremented` event for
each `Increment` command, and `mem.NewStore()` runs it against an in-memory log, which is what
you'd use in tests. Swapping in `dynamo.NewRepository` or `postgres.NewRepository` behind
`snapshots.NewStore` changes nothing in the aggregate.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/mem"
)

type Increment struct{ By int }

func (Increment) Type() evt.CommandType      { return "counter:increment" }
func (Increment) EntityType() evt.EntityType { return "counter" }

type Incremented struct {
	ID evt.EntityID `json:"id"`
	By int          `json:"by"`
}

func (Incremented) Type() evt.EventType        { return "counter:incremented" }
func (Incremented) Version() evt.EventVersion  { return 1 }
func (Incremented) EntityType() evt.EntityType { return "counter" }
func (e Incremented) EntityID() evt.EntityID   { return e.ID }

type Counter struct {
	ID    evt.EntityID
	Total int
}

func (c *Counter) Type() evt.EntityType                { return "counter" }
func (c *Counter) GetID() evt.EntityID                 { return c.ID }
func (c *Counter) EventUpcasters() []evt.EventUpcaster { return nil }
func (c *Counter) Projectors() []evt.EventProjector    { return nil }

func (c *Counter) Handle(_ context.Context, cmd evt.Command) (evt.CommandResult, error) {
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

func (c *Counter) DeserializeEvent(e evt.SerializedEvent) (evt.Event, error) {
	var event Incremented
	err := json.Unmarshal(e.Payload, &event)
	return event, err
}

func main() {
	ctx := context.Background()
	store := mem.NewStore()

	counter := &Counter{ID: "counter-1"}
	if err := store.Execute(ctx, counter, "counter-1", &Increment{By: 2}, evt.Metadata{}); err != nil {
		panic(err)
	}

	// LoadAggregate replays the stream into a fresh instance and returns its CommandContext.
	reloaded := &Counter{ID: "counter-1"}
	cc, err := store.LoadAggregate(ctx, reloaded, "counter-1")
	if err != nil {
		panic(err)
	}
	fmt.Println(reloaded.Total, *cc.CurrentSequence) // 2 1
}
```

`Execute` replays history onto the instance you pass, so give it a fresh one per command. The
`CommandContext` that `LoadAggregate` returns is where the stream stood when it was loaded, and
it's what a commit checks against. [`go/examples/banking`](go/examples/banking) is a fuller
version with conflicts and not-found errors, and [`go/README.md`](go/README.md) lists the
packages.

## Rust

```sh
cargo add photon-evt                      # add --features dynamodb or postgres for a durable backend
```

The Rust side has the same shape. `domain_events!` declares the event enum with its stored type
names and versions, and the `Aggregate` trait carries the command, event and error types.

```rust
use photon_evt::{Aggregate, MemRepository, Metadata, Store, domain_events};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Incremented {
    pub by: i64,
}

domain_events! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum CounterEvent {
        Incremented(Incremented) => ("counter:incremented", 1),
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Counter {
    pub total: i64,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::<Counter, _>::new(MemRepository::new(), ());
    let executed = store.execute("counter-1", 2, Metadata::new("us-west-2")).await?;
    assert_eq!(executed.state.aggregate().total, 2);
    Ok(())
}
```

`Store::execute` loads the stream, runs `handle`, and commits the events with the same optimistic
check the Go store makes. The [`photon-evt` README](crates/evt/README.md) covers transactions,
command outcomes, the journal and testing, and
[`photon-evt-lambda`](crates/evt-lambda/README.md) is its AWS Lambda side.

## What's in the repo

The Go module lives in [`go/`](go/) and is tagged `go/vX.Y.Z`, because Go requires that prefix for
a module in a subdirectory. The Rust crates live in [`crates/`](crates/): `photon-evt`,
`photon-evt-lambda`, and `evt-compat`, the cross-language test harness. [`packages/`](packages/)
holds the TypeScript packages the website uses, and [`website/`](website/) is the docs site.
[`compat/`](compat/) holds the shared scenarios and golden fixtures that keep the two languages
writing the same bytes. And [`docs/`](docs/) has the guide, the reference, and the architecture
decision records.

## Where to go next

Start with [Getting started](docs/guide/getting-started.md), which builds an aggregate and moves
it from memory to DynamoDB, then read [Concepts](docs/guide/concepts.md) for the vocabulary. The
backend pages cover [DynamoDB](docs/guide/dynamodb.md) and [PostgreSQL](docs/guide/postgres.md),
and [Projections and rebuilds](docs/guide/projections.md) and
[Command outcomes and retries](docs/guide/commands.md) cover the parts most people need next.
Upgrading from v0.8 has its own page, [Upgrading to v0.9](docs/guide/upgrading-to-v0.9.md), and so
does the [stream-heads migration](docs/guide/migrating-to-stream-heads.md). The
[storage format](docs/reference/storage-format.md) is the normative spec both languages follow, and
the [feature matrix](docs/reference/feature-matrix.md) shows what each language implements. The
same docs are on the [docs site](https://photon-grove.github.io/evt/), and
[CONTRIBUTING.md](CONTRIBUTING.md) explains how to build and test locally.

## License

Apache-2.0. See [`LICENSE`](LICENSE).

Built by [Photon Grove](https://photon-grove.com), a Colorado software studio.

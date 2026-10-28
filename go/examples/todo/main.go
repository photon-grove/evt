// Command todo is a runnable example of a read model that commits with its events and can be
// rebuilt from the log.
//
// A TodoList aggregate runs on the in-memory repository. A commit-wide projector writes each list's
// open and done counts as a view row in the same commit as the events, so the row never disagrees
// with the stream behind it. Then the example rebuilds every row from the stored events alone:
// evt.GroupCommits regroups the events into the commits that wrote them, each list's state is
// folded again, and evt.ReplayViews runs the projector over that history into an empty set of rows,
// which is what a wiped view table holds. The rebuilt rows equal the live ones.
//
// Run it with `go -C go run ./examples/todo`.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/mem"
	"github.com/photon-grove/evt/go/snapshots"
)

const entityType evt.EntityType = "todo_list"

// summaryPrefix starts the partition key of a list's summary row.
const summaryPrefix = "todo_summary#"

// ---- Aggregate ----

// Item is one entry of a list.
type Item struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Done  bool   `json:"done"`
}

// TodoList is the aggregate. Its JSON is the state a commit-wide projector reads, so every field
// Apply changes is exported.
type TodoList struct {
	ID    evt.EntityID `json:"id"`
	Items []Item       `json:"items"`
}

// NewTodoList returns an empty list. Execute replays the stream onto the instance it is given, so
// each command needs a fresh one.
func NewTodoList(id evt.EntityID) *TodoList {
	return &TodoList{ID: id}
}

// Type returns the list's entity type.
func (l *TodoList) Type() evt.EntityType { return entityType }

// GetID returns the list's ID.
func (l *TodoList) GetID() evt.EntityID { return l.ID }

// Handle validates a command and decides its events, without changing state.
func (l *TodoList) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
	switch cmd := command.(type) {
	case *AddItem:
		if l.find(cmd.ItemID) >= 0 {
			return evt.CommandResult{}, evt.NewConflictError("item already added")
		}

		return evt.CommandResult{Events: []evt.Event{ItemAdded{ListID: l.ID, ItemID: cmd.ItemID, Title: cmd.Title}}}, nil

	case *CompleteItem:
		index := l.find(cmd.ItemID)
		if index < 0 {
			return evt.CommandResult{}, evt.NewNotFoundError("no such item")
		}

		if l.Items[index].Done {
			return evt.CommandResult{}, evt.NewConflictError("item already done")
		}

		return evt.CommandResult{Events: []evt.Event{ItemCompleted{ListID: l.ID, ItemID: cmd.ItemID}}}, nil

	default:
		return evt.CommandResult{}, evt.NewBadCommandError(command)
	}
}

// Apply folds a recorded event into state, without checking it again.
func (l *TodoList) Apply(event evt.Event) error {
	switch e := event.(type) {
	case ItemAdded:
		l.ID = e.ListID
		l.Items = append(l.Items, Item{ID: e.ItemID, Title: e.Title})

	case ItemCompleted:
		if index := l.find(e.ItemID); index >= 0 {
			l.Items[index].Done = true
		}

	default:
		return evt.NewBadEventError(event)
	}

	return nil
}

// DeserializeEvent decodes a stored event of this list.
func (l *TodoList) DeserializeEvent(serialized evt.SerializedEvent) (evt.Event, error) {
	switch serialized.Type {
	case ItemAdded{}.Type():
		var event ItemAdded
		err := json.Unmarshal(serialized.Payload, &event)

		return event, err

	case ItemCompleted{}.Type():
		var event ItemCompleted
		err := json.Unmarshal(serialized.Payload, &event)

		return event, err

	default:
		return nil, fmt.Errorf("unrecognized event: %s", serialized.Type)
	}
}

// EventUpcasters returns none: every stored event is at its current version.
func (l *TodoList) EventUpcasters() []evt.EventUpcaster { return nil }

// Projectors returns no per-entity projectors; the summary is a commit-wide projector.
func (l *TodoList) Projectors() []evt.EventProjector { return nil }

func (l *TodoList) find(itemID string) int {
	return slices.IndexFunc(l.Items, func(item Item) bool { return item.ID == itemID })
}

// ---- Commands ----

// AddItem adds an item to a list.
type AddItem struct {
	ItemID string
	Title  string
}

// Type returns the command type.
func (c *AddItem) Type() evt.CommandType { return "todo_list.add_item" }

// EntityType returns the list's entity type.
func (c *AddItem) EntityType() evt.EntityType { return entityType }

// CompleteItem marks an item done.
type CompleteItem struct {
	ItemID string
}

// Type returns the command type.
func (c *CompleteItem) Type() evt.CommandType { return "todo_list.complete_item" }

// EntityType returns the list's entity type.
func (c *CompleteItem) EntityType() evt.EntityType { return entityType }

// ---- Events ----

// ItemAdded records an item added to a list.
type ItemAdded struct {
	ListID evt.EntityID `json:"listId"`
	ItemID string       `json:"itemId"`
	Title  string       `json:"title"`
}

// Type returns the stored event type.
func (e ItemAdded) Type() evt.EventType { return "todo_list.item_added" }

// Version returns the payload version.
func (e ItemAdded) Version() evt.EventVersion { return 1 }

// EntityID returns the list's ID.
func (e ItemAdded) EntityID() evt.EntityID { return e.ListID }

// EntityType returns the list's entity type.
func (e ItemAdded) EntityType() evt.EntityType { return entityType }

// ItemCompleted records an item marked done.
type ItemCompleted struct {
	ListID evt.EntityID `json:"listId"`
	ItemID string       `json:"itemId"`
}

// Type returns the stored event type.
func (e ItemCompleted) Type() evt.EventType { return "todo_list.item_completed" }

// Version returns the payload version.
func (e ItemCompleted) Version() evt.EventVersion { return 1 }

// EntityID returns the list's ID.
func (e ItemCompleted) EntityID() evt.EntityID { return e.ListID }

// EntityType returns the list's entity type.
func (e ItemCompleted) EntityType() evt.EntityType { return entityType }

// ---- Summary view ----

// Summary is the payload of a list's summary row.
type Summary struct {
	Open int `json:"open"`
	Done int `json:"done"`
}

// SummaryProjector writes one summary row per list, keyed todo_summary#<list ID>, owned by the
// list's stream. It is a current-state projector: History returns nil, so it is given each written
// stream's state after the commit and nothing else, and its rows are a function of that state.
type SummaryProjector struct{}

// Name identifies the projector in errors.
func (SummaryProjector) Name() string { return "todo_summary" }

// History returns nil: the projector reads state, not events.
func (SummaryProjector) History() []evt.EntityType { return nil }

// ProjectCommit returns a put of the summary row of every list the commit wrote. It is a pure
// function of the commit view: no clock, no store reads, no state kept between calls.
func (SummaryProjector) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	operations := make([]evt.ViewOperation, 0, len(commit.Streams))

	for _, stream := range commit.Streams {
		if stream.EntityType != entityType {
			continue
		}

		var list TodoList
		if err := json.Unmarshal(stream.State, &list); err != nil {
			return nil, fmt.Errorf("decoding list %s: %w", stream.EntityID, err)
		}

		payload, err := json.Marshal(summarize(list))
		if err != nil {
			return nil, err
		}

		operations = append(operations,
			evt.PutView(summaryPrefix+string(stream.EntityID), evt.DefaultViewSK, stream.EntityID, entityType, payload))
	}

	return operations, nil
}

func summarize(list TodoList) Summary {
	var summary Summary

	for _, item := range list.Items {
		if item.Done {
			summary.Done++
		} else {
			summary.Open++
		}
	}

	return summary
}

// ---- Wiring ----

// newRepository returns the in-memory repository, which keeps the rows commit-wide projectors
// write (View, Views). On DynamoDB the same projector writes to the table WithViewsTable names.
func newRepository() (*mem.Repository, error) {
	repo, ok := mem.NewRepository().(*mem.Repository)
	if !ok {
		return nil, errors.New("mem.NewRepository returned an unexpected type")
	}

	return repo, nil
}

// newStore wraps the repository in the store with the summary projector registered.
func newStore(repo *mem.Repository) *snapshots.Store {
	return snapshots.NewStore(repo, 5).WithCommitProjectors(SummaryProjector{})
}

// step is one command against one list.
type step struct {
	list    evt.EntityID
	command evt.Command
}

// seed runs a short history: two lists, three items, one completed.
func seed(ctx context.Context, store *snapshots.Store) error {
	steps := []step{
		{"list-1", &AddItem{ItemID: "milk", Title: "Buy milk"}},
		{"list-1", &AddItem{ItemID: "bread", Title: "Buy bread"}},
		{"list-2", &AddItem{ItemID: "taxes", Title: "File taxes"}},
		{"list-1", &CompleteItem{ItemID: "milk"}},
	}

	for _, s := range steps {
		if err := store.Execute(ctx, NewTodoList(s.list), s.list, s.command, evt.Metadata{}); err != nil {
			return fmt.Errorf("%s %s: %w", s.list, s.command.Type(), err)
		}
	}

	return nil
}

// ReadSummary returns a list's summary row, or false when the list has none.
func ReadSummary(repo *mem.Repository, list evt.EntityID) (Summary, bool, error) {
	row, ok := repo.View(summaryPrefix+string(list), evt.DefaultViewSK)
	if !ok {
		return Summary{}, false, nil
	}

	var summary Summary
	if err := json.Unmarshal(row.Payload, &summary); err != nil {
		return Summary{}, false, err
	}

	return summary, true, nil
}

// ---- Rebuild ----

// RebuildSummaries rebuilds every summary row from the event log alone, into an empty set of rows.
// It reads the stored list events (StreamAllEvents is the rebuild and diagnostic path, not a hot
// read), regroups them into the commits that wrote them, folds each stream's state as it stood
// after each commit, and replays the projector over that history.
func RebuildSummaries(ctx context.Context, repo evt.Repository) ([]evt.SerializedView, int, error) {
	var stored []evt.SerializedEvent

	for batch := range repo.StreamAllEvents(ctx, evt.StreamFilter{EntityType: entityType}) {
		events, err := batch.Unwrap()
		if err != nil {
			return nil, 0, err
		}

		stored = append(stored, events...)
	}

	commits, err := evt.GroupCommits(stored)
	if err != nil {
		return nil, 0, err
	}

	if err = foldStates(commits, stored); err != nil {
		return nil, 0, err
	}

	rows, err := evt.ReplayViews(ctx, []evt.CommitProjector{SummaryProjector{}}, commits)
	if err != nil {
		return nil, 0, err
	}

	return rows, len(commits), nil
}

// foldStates gives every stream of every commit its state as it stood after that commit.
func foldStates(commits []evt.CommitView, stored []evt.SerializedEvent) error {
	for _, commit := range commits {
		for i, stream := range commit.Streams {
			through := stream.Events[len(stream.Events)-1].Sequence

			state, err := stateAfter(stored, stream.EntityID, through)
			if err != nil {
				return err
			}

			commit.Streams[i].State = state
		}
	}

	return nil
}

// stateAfter folds a list's stored events up to and including the given sequence and returns its
// JSON, the state the store handed the projector when that commit was live.
func stateAfter(stored []evt.SerializedEvent, id evt.EntityID, through evt.EventSequence) ([]byte, error) {
	var events []evt.SerializedEvent

	for _, event := range stored {
		if event.EntityID == id && event.Sequence <= through {
			events = append(events, event)
		}
	}

	slices.SortFunc(events, func(a, b evt.SerializedEvent) int { return int(a.Sequence - b.Sequence) })

	list := NewTodoList(id)

	for _, serialized := range events {
		event, err := list.DeserializeEvent(serialized)
		if err != nil {
			return nil, err
		}

		if err = list.Apply(event); err != nil {
			return nil, err
		}
	}

	return json.Marshal(list)
}

// run seeds the lists, prints their summary rows, and rebuilds the rows from the log.
func run(ctx context.Context, out io.Writer) error {
	repo, err := newRepository()
	if err != nil {
		return err
	}

	if err = seed(ctx, newStore(repo)); err != nil {
		return err
	}

	if err = printSummaries(out, repo, "list-1", "list-2"); err != nil {
		return err
	}

	rebuilt, commits, err := RebuildSummaries(ctx, repo)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(out, "rebuilt %d rows from %d commits; equal to the live rows: %t\n",
		len(rebuilt), commits, reflect.DeepEqual(rebuilt, repo.Views()))

	return err
}

// printSummaries prints each list's summary row.
func printSummaries(out io.Writer, repo *mem.Repository, lists ...evt.EntityID) error {
	for _, list := range lists {
		summary, _, err := ReadSummary(repo, list)
		if err != nil {
			return err
		}

		if _, err = fmt.Fprintf(out, "%s: open=%d done=%d\n", list, summary.Open, summary.Done); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		panic(err)
	}
}

package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/snapshots"
)

// This case drives snapshots.Store over the backend under test with an aggregate whose state
// outgrows the per-item commit budget while its events stay small (storage spec, Snapshot policy).

const hoardType evt.EntityType = "hoard"

// hoardPiece is the size of the state one stashed event adds: two pieces fit in one snapshot item,
// three do not.
const hoardPiece = 150_000

// hoard keeps everything it is given, so its state grows with every event.
type hoard struct {
	id    evt.EntityID
	Items []string `json:"items"`
}

func (h *hoard) Type() evt.EntityType                { return hoardType }
func (h *hoard) GetID() evt.EntityID                 { return h.id }
func (h *hoard) SetID(id evt.EntityID)               { h.id = id }
func (h *hoard) EventUpcasters() []evt.EventUpcaster { return nil }
func (h *hoard) Projectors() []evt.EventProjector    { return nil }

type stashed struct {
	Size int `json:"size"`
}

func (*stashed) Type() evt.EventType        { return "hoard:stashed" }
func (*stashed) Version() evt.EventVersion  { return 1 }
func (*stashed) EntityType() evt.EntityType { return hoardType }
func (*stashed) EntityID() evt.EntityID     { return "" }

type stash struct{ Size int }

func (*stash) Type() evt.CommandType      { return "hoard:stash" }
func (*stash) EntityType() evt.EntityType { return hoardType }

func (h *hoard) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
	if s, ok := command.(*stash); ok {
		return evt.CommandResult{Events: []evt.Event{&stashed{Size: s.Size}}}, nil
	}

	return evt.CommandResult{}, evt.NewBadCommandError(command)
}

func (h *hoard) Apply(event evt.Event) error {
	if e, ok := event.(*stashed); ok {
		h.Items = append(h.Items, strings.Repeat("x", e.Size))

		return nil
	}

	return evt.NewBadEventError(event)
}

func (h *hoard) DeserializeEvent(serialized evt.SerializedEvent) (evt.Event, error) {
	var event stashed
	if err := json.Unmarshal(serialized.Payload, &event); err != nil {
		return nil, err
	}

	return &event, nil
}

// testOversizedSnapshotIsSkipped checks that a snapshot whose item would be over the per-item
// commit budget is left out of its commit instead of failing it, on every path that writes one: a
// command, a transaction, and a catch-up load. Before the guard, every commit that reached the
// snapshot interval failed with CommitTooLargeError once the state was too large, bricking the
// stream.
func testOversizedSnapshotIsSkipped(t *testing.T, repo evt.Repository, transactions bool) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()
	id := evt.EntityID(ns + "-hoard")
	store := snapshots.NewStore(repo, 2)

	execute := func(n, size int) {
		t.Helper()
		require.NoError(t, store.Execute(ctx, &hoard{}, id, &stash{Size: size}, tallyMetadata(fmt.Sprintf("%s-%d", ns, n))))
	}
	snapshotAt := func(sequence, eventSequence evt.EventSequence, msg string) {
		t.Helper()
		snapshot, err := repo.GetSnapshot(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, snapshot, msg)
		require.Equal(t, [2]evt.EventSequence{sequence, eventSequence},
			[2]evt.EventSequence{snapshot.Sequence, snapshot.EventSequence}, msg)
	}

	// Two pieces fit: the commit at the interval writes the first snapshot.
	execute(1, hoardPiece)
	execute(2, hoardPiece)
	snapshotAt(1, 2, "a snapshot that fits is written")

	// Four pieces do not: the commit at the next interval still succeeds, without its snapshot.
	execute(3, hoardPiece)
	execute(4, hoardPiece)
	snapshotAt(1, 2, "an oversized snapshot is skipped and the stored one stays")

	// The stream stays writable and loads from the older snapshot plus the later events.
	execute(5, 1)

	loaded := &hoard{}
	cc, err := store.LoadAggregate(ctx, loaded, id)
	require.NoError(t, err)
	require.Len(t, loaded.Items, 5)
	require.Equal(t, evt.EventSequence(5), *cc.CurrentSequence)
	require.Equal(t, evt.EventSequence(1), *cc.CurrentSnapshot)

	if transactions {
		// A transaction append that reaches the interval commits without the snapshot too.
		tx := store.Transaction()
		require.NoError(t, tx.Read(ctx, &hoard{}, id))
		require.NoError(t, tx.Append(id, &stashed{Size: 1}))

		_, err = tx.Commit(ctx, tallyMetadata(ns+"-tx"))
		require.NoError(t, err)
		snapshotAt(1, 2, "a transaction skips an oversized snapshot")
	}

	// A catch-up load does not write it either.
	catchUp := snapshots.NewStore(repo, 2).WithCatchUpSnapshotThreshold(1)
	_, err = catchUp.LoadAggregate(ctx, &hoard{}, id)
	require.NoError(t, err)
	snapshotAt(1, 2, "a catch-up load skips an oversized snapshot")
}

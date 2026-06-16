package mem

import (
	"context"
	"errors"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
	evttest "github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

func Test_DIProviders(t *testing.T) {
	store, err := ProvideStore(nil)
	require.NoError(t, err)
	require.NotNil(t, store)

	repo, err := ProvideRepository(nil)
	require.NoError(t, err)
	require.NotNil(t, repo)
}

func Test_NewStore(t *testing.T) {
	store := NewStore()
	require.NotNil(t, store)
}

func Test_Repository_StreamAllEvents(t *testing.T) {
	repo := &Repository{
		events:    make(map[string][]evt.SerializedEvent),
		snapshots: make(map[string]evt.SerializedSnapshot),
	}

	repo.events["a"] = []evt.SerializedEvent{
		{EntityID: "a", Sequence: 1, Type: "t1"},
	}
	repo.events["b"] = []evt.SerializedEvent{
		{EntityID: "b", Sequence: 1, Type: "t1"},
		{EntityID: "b", Sequence: 2, Type: "t2"},
	}

	ch := repo.StreamAllEvents(context.Background(), evt.StreamFilter{})

	got := make([][]evt.SerializedEvent, 0, 2)
	for r := range ch {
		events, err := r.Unwrap()
		require.NoError(t, err)
		got = append(got, events)
	}

	// Map iteration order is undefined; assert we got both slices by length.
	require.Len(t, got, 2)

	lens := make([]int, 0, len(got))
	for _, s := range got {
		lens = append(lens, len(s))
	}
	require.ElementsMatch(t, []int{1, 2}, lens)
}

func Test_Repository_StreamEntities(t *testing.T) {
	repo := &Repository{
		events:    make(map[string][]evt.SerializedEvent),
		snapshots: make(map[string]evt.SerializedSnapshot),
	}

	// Include a "snapshot" sentinel (sequence 0) to exercise the skip path.
	repo.events["a"] = []evt.SerializedEvent{
		{EntityID: "a", Sequence: 0, Type: "snapshot"},
		{EntityID: "a", Sequence: 1, Type: "t1", Payload: []byte("a1")},
	}
	repo.events["b"] = []evt.SerializedEvent{
		{EntityID: "b", Sequence: 1, Type: "t1", Payload: []byte("b1")},
		{EntityID: "b", Sequence: 2, Type: "t2", Payload: []byte("b2")},
	}

	applyEvent := func(_ context.Context, e evt.SerializedEvent, current evt.Entity) (evt.Entity, error) {
		if e.Type == "boom" {
			return nil, errors.New("boom")
		}
		if current == nil {
			current = evttest.NewEntity(e.EntityID)
		}
		ent, ok := current.(*evttest.Entity)
		if !ok {
			return nil, errors.New("unexpected entity type")
		}
		ent.Value = string(e.Payload)
		return ent, nil
	}

	ch := repo.StreamEntities(context.Background(), evt.StreamFilter{}, applyEvent)

	gotIDs := make([]evt.EntityID, 0, 2)
	for r := range ch {
		ent, err := r.Unwrap()
		require.NoError(t, err)
		require.NotNil(t, ent)
		gotIDs = append(gotIDs, ent.GetID())
	}

	require.ElementsMatch(t, []evt.EntityID{"a", "b"}, gotIDs)
}

func Test_Repository_StreamEntities_ApplyError(t *testing.T) {
	repo := &Repository{
		events:    make(map[string][]evt.SerializedEvent),
		snapshots: make(map[string]evt.SerializedSnapshot),
	}
	repo.events["a"] = []evt.SerializedEvent{
		{EntityID: "a", Sequence: 1, Type: "boom"},
	}

	applyEvent := func(_ context.Context, _ evt.SerializedEvent, _ evt.Entity) (evt.Entity, error) {
		return nil, errors.New("apply failed")
	}

	ch := repo.StreamEntities(context.Background(), evt.StreamFilter{}, applyEvent)

	var sawErr bool
	for r := range ch {
		_, err := r.Unwrap()
		if err != nil {
			sawErr = true
		}
	}

	require.True(t, sawErr)
}

func Test_CommitStream_CollectsErrors(t *testing.T) {
	repo := &Repository{
		events:    make(map[string][]evt.SerializedEvent),
		snapshots: make(map[string]evt.SerializedSnapshot),
	}

	ch := make(chan result.Result[evt.SerializedResult])
	go func() {
		defer close(ch)
		ch <- result.Err[evt.SerializedResult](errors.New("nope"))
	}()

	errs := repo.CommitStream(context.Background(), ch)
	require.Len(t, errs, 1)
	require.Contains(t, errs[0].Error(), "nope")
}

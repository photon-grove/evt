package viewstore

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/photon-grove/evt"
)

type fakeRepo struct {
	views map[string]*evt.SerializedView
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{views: map[string]*evt.SerializedView{}}
}

func key(pk, sk string) string {
	if sk == "" {
		sk = evt.DefaultViewSK
	}
	return pk + "\x00" + sk
}

func (r *fakeRepo) GetView(_ context.Context, pk string) (*evt.SerializedView, error) {
	v, ok := r.views[key(pk, evt.DefaultViewSK)]
	if !ok {
		return nil, nil
	}
	dup := *v
	dup.Payload = append([]byte(nil), v.Payload...)
	return &dup, nil
}

func (r *fakeRepo) PutView(_ context.Context, view *evt.SerializedView) error {
	if view == nil {
		return errors.New("view is required")
	}
	dup := *view
	if dup.SK == "" {
		dup.SK = evt.DefaultViewSK
	}
	dup.Payload = append([]byte(nil), view.Payload...)
	r.views[key(dup.PK, dup.SK)] = &dup
	return nil
}

func (r *fakeRepo) ListViewsByEntityType(_ context.Context, entityType evt.EntityType) ([]*evt.SerializedView, error) {
	out := make([]*evt.SerializedView, 0, len(r.views))
	for _, v := range r.views {
		if v.EntityType != entityType {
			continue
		}
		dup := *v
		dup.Payload = append([]byte(nil), v.Payload...)
		out = append(out, &dup)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PK < out[j].PK })
	return out, nil
}

func (r *fakeRepo) ListViewsByEntityTypePaged(ctx context.Context, entityType evt.EntityType, _ int, _ string) (*evt.PagedResult, error) {
	views, err := r.ListViewsByEntityType(ctx, entityType)
	if err != nil {
		return nil, err
	}
	return &evt.PagedResult{Views: views}, nil
}

func (r *fakeRepo) ListViewsByPK(_ context.Context, pk string) ([]*evt.SerializedView, error) {
	out := make([]*evt.SerializedView, 0, len(r.views))
	for _, v := range r.views {
		if v.PK != pk {
			continue
		}
		dup := *v
		dup.Payload = append([]byte(nil), v.Payload...)
		out = append(out, &dup)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SK < out[j].SK })
	return out, nil
}

func (r *fakeRepo) ListViewsByPKPaged(ctx context.Context, pk string, _ int, _ string) (*evt.PagedResult, error) {
	views, err := r.ListViewsByPK(ctx, pk)

	return &evt.PagedResult{Views: views}, err
}

func (r *fakeRepo) ListViewsByEntityTypeEach(ctx context.Context, entityType evt.EntityType, fn func(*evt.SerializedView) error) error {
	views, err := r.ListViewsByEntityType(ctx, entityType)
	if err != nil {
		return err
	}

	for _, view := range views {
		if err := fn(view); err != nil {
			return err
		}
	}

	return nil
}

func (r *fakeRepo) ListViewsByPKEach(ctx context.Context, pk string, fn func(*evt.SerializedView) error) error {
	views, err := r.ListViewsByPK(ctx, pk)
	if err != nil {
		return err
	}

	for _, view := range views {
		if err := fn(view); err != nil {
			return err
		}
	}

	return nil
}

type testItem struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

const testEntityType evt.EntityType = "viewstore.test.item"

func TestCodecGetPut(t *testing.T) {
	repo := newFakeRepo()
	codec := New[testItem](repo, testEntityType)

	if err := codec.PutAt(context.Background(), "ITEM#1", "1", testItem{ID: "1", Name: "alpha"}); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, found, err := codec.Get(context.Background(), "ITEM#1")
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if got.Name != "alpha" {
		t.Fatalf("unexpected: %+v", got)
	}

	_, found, err = codec.Get(context.Background(), "missing")
	if err != nil {
		t.Fatalf("get missing err: %v", err)
	}
	if found {
		t.Fatalf("expected not found")
	}
}

func TestCodecListByPKSkipInvalid(t *testing.T) {
	repo := newFakeRepo()
	codec := New[testItem](repo, testEntityType)

	if err := codec.Put(context.Background(), "TAG#x", "1", "1", testItem{ID: "1", Name: "a"}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := codec.Put(context.Background(), "TAG#x", "2", "2", testItem{ID: "2", Name: "b"}); err != nil {
		t.Fatalf("put: %v", err)
	}
	// inject a view with invalid payload at the same PK
	if err := repo.PutView(context.Background(), &evt.SerializedView{
		PK: "TAG#x", SK: "3", EntityID: "3", EntityType: testEntityType, Payload: []byte("not-json"),
	}); err != nil {
		t.Fatalf("inject: %v", err)
	}

	var skipped int
	out, err := codec.ListByPK(context.Background(), "TAG#x", func(_ *evt.SerializedView, _ error) { skipped++ })
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 valid, got %d", len(out))
	}
	if skipped != 1 {
		t.Fatalf("expected 1 skipped, got %d", skipped)
	}
}

func TestCodecListAll(t *testing.T) {
	repo := newFakeRepo()
	codec := New[testItem](repo, testEntityType)

	if err := codec.PutAt(context.Background(), "ITEM#1", "1", testItem{ID: "1", Name: "a"}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := codec.PutAt(context.Background(), "ITEM#2", "2", testItem{ID: "2", Name: "b"}); err != nil {
		t.Fatalf("put: %v", err)
	}
	out, err := codec.ListAll(context.Background(), nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2, got %d", len(out))
	}
}

func TestSingleStore(t *testing.T) {
	repo := newFakeRepo()
	store := NewSingle[string, testItem](
		repo,
		testEntityType,
		func(id string) string { return "ITEM#" + id },
		func(v testItem) evt.EntityID { return evt.EntityID(v.ID) },
	)

	if err := store.Put(context.Background(), "1", testItem{ID: "1", Name: "alpha"}); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, found, err := store.Get(context.Background(), "1")
	if err != nil || !found || got.Name != "alpha" {
		t.Fatalf("get: found=%v err=%v got=%+v", found, err, got)
	}

	all, err := store.ListAll(context.Background(), nil)
	if err != nil || len(all) != 1 {
		t.Fatalf("list: got=%d err=%v", len(all), err)
	}
}

type ptrEntity struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	dep  string
}

func TestCodecFactory(t *testing.T) {
	repo := newFakeRepo()
	codec := NewWithFactory[*ptrEntity](repo, testEntityType, func() *ptrEntity {
		return &ptrEntity{dep: "injected"}
	})

	if err := codec.PutAt(context.Background(), "PTR#1", "1", &ptrEntity{ID: "1", Name: "alpha"}); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, found, err := codec.Get(context.Background(), "PTR#1")
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if got.dep != "injected" {
		t.Fatalf("factory not used: %+v", got)
	}
	if got.Name != "alpha" {
		t.Fatalf("decode mismatch: %+v", got)
	}
}

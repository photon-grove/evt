package evt

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type codecTestRepo struct {
	views map[string]*SerializedView
}

func newCodecTestRepo() *codecTestRepo {
	return &codecTestRepo{views: map[string]*SerializedView{}}
}

func (r *codecTestRepo) GetView(_ context.Context, pk string) (*SerializedView, error) {
	view, ok := r.views[codecTestKey(pk, DefaultViewSK)]
	if !ok {
		return nil, nil
	}

	dup := *view
	dup.Payload = append([]byte(nil), view.Payload...)
	return &dup, nil
}

func (r *codecTestRepo) PutView(_ context.Context, view *SerializedView) error {
	if view == nil {
		return errors.New("view is required")
	}

	dup := *view
	if dup.SK == "" {
		dup.SK = DefaultViewSK
	}
	dup.Payload = append([]byte(nil), view.Payload...)

	r.views[codecTestKey(dup.PK, dup.SK)] = &dup
	return nil
}

func (r *codecTestRepo) ListViewsByEntityType(_ context.Context, entityType EntityType) ([]*SerializedView, error) {
	out := make([]*SerializedView, 0, len(r.views))
	for _, view := range r.views {
		if view.EntityType != entityType {
			continue
		}

		dup := *view
		dup.Payload = append([]byte(nil), view.Payload...)
		out = append(out, &dup)
	}
	return out, nil
}

func (r *codecTestRepo) ListViewsByEntityTypePaged(_ context.Context, entityType EntityType, _ int, _ string) (*PagedResult, error) {
	views, err := r.ListViewsByEntityType(context.Background(), entityType)
	if err != nil {
		return nil, err
	}

	return &PagedResult{
		Views:      views,
		NextCursor: "",
	}, nil
}

func (r *codecTestRepo) ListViewsByEntityTypeEach(ctx context.Context, entityType EntityType, fn func(*SerializedView) error) error {
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

func (r *codecTestRepo) ListViewsByPK(_ context.Context, pk string) ([]*SerializedView, error) {
	out := make([]*SerializedView, 0, len(r.views))
	for _, view := range r.views {
		if view.PK != pk {
			continue
		}

		dup := *view
		dup.Payload = append([]byte(nil), view.Payload...)
		out = append(out, &dup)
	}
	return out, nil
}

func (r *codecTestRepo) ListViewsByPKEach(ctx context.Context, pk string, fn func(*SerializedView) error) error {
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

func (r *codecTestRepo) ListViewsByPKPaged(ctx context.Context, pk string, _ int, _ string) (*PagedResult, error) {
	views, err := r.ListViewsByPK(ctx, pk)

	return &PagedResult{Views: views}, err
}

func codecTestKey(pk, sk string) string {
	if sk == "" {
		sk = DefaultViewSK
	}
	return pk + "\x00" + sk
}

type codecTestView struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type codecConstructedView struct {
	ID   string        `json:"id"`
	Hook func() string `json:"-"`
}

func TestGetJSONViewValueType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newCodecTestRepo()

	err := repo.PutView(ctx, &SerializedView{
		PK:         "test#1",
		EntityID:   EntityID("1"),
		EntityType: EntityType("test.view"),
		Payload:    []byte(`{"id":"1","name":"alpha"}`),
	})
	if err != nil {
		t.Fatalf("put view: %v", err)
	}

	got, found, err := GetJSONView[codecTestView](ctx, repo, "test#1", nil)
	if err != nil {
		t.Fatalf("get JSON view: %v", err)
	}
	if !found {
		t.Fatal("expected view to be found")
	}
	if got.ID != "1" {
		t.Fatalf("expected ID 1, got %q", got.ID)
	}
	if got.Name != "alpha" {
		t.Fatalf("expected name alpha, got %q", got.Name)
	}
}

func TestGetJSONViewPointerConstructorPreservesInjectedFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newCodecTestRepo()

	err := repo.PutView(ctx, &SerializedView{
		PK:         "constructed#1",
		EntityID:   EntityID("1"),
		EntityType: EntityType("constructed.view"),
		Payload:    []byte(`{"id":"1"}`),
	})
	if err != nil {
		t.Fatalf("put view: %v", err)
	}

	got, found, err := GetJSONView[*codecConstructedView](ctx, repo, "constructed#1", func() *codecConstructedView {
		return &codecConstructedView{
			Hook: func() string {
				return "injected"
			},
		}
	})
	if err != nil {
		t.Fatalf("get JSON view: %v", err)
	}
	if !found {
		t.Fatal("expected view to be found")
	}
	if got == nil {
		t.Fatal("expected decoded pointer value")
	}
	if got.ID != "1" {
		t.Fatalf("expected ID 1, got %q", got.ID)
	}
	if got.Hook == nil {
		t.Fatal("expected constructor-injected hook to be preserved")
	}
	if got.Hook() != "injected" {
		t.Fatalf("expected injected hook result, got %q", got.Hook())
	}
}

func TestGetJSONViewNotFound(t *testing.T) {
	t.Parallel()

	got, found, err := GetJSONView[codecTestView](context.Background(), newCodecTestRepo(), "missing", nil)
	if err != nil {
		t.Fatalf("get missing JSON view: %v", err)
	}
	if found {
		t.Fatal("expected found=false for missing view")
	}
	if got != (codecTestView{}) {
		t.Fatalf("expected zero value for missing view, got %#v", got)
	}
}

func TestGetJSONViewRequiresRepository(t *testing.T) {
	t.Parallel()

	_, _, err := GetJSONView[codecTestView](context.Background(), nil, "test#1", nil)
	if err == nil {
		t.Fatal("expected repository configuration error")
	}
	if !strings.Contains(err.Error(), "view repository is not configured") {
		t.Fatalf("expected repository configuration error, got %v", err)
	}
}

func TestPutJSONViewCopiesMetadataAndWritesPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newCodecTestRepo()

	template := &SerializedView{
		PK:         "test#1",
		EntityID:   EntityID("1"),
		EntityType: EntityType("test.view"),
		TTL:        123,
	}

	err := PutJSONView(ctx, repo, template, codecTestView{ID: "1", Name: "alpha"})
	if err != nil {
		t.Fatalf("put JSON view: %v", err)
	}

	if len(template.Payload) != 0 {
		t.Fatalf("expected metadata template payload to remain untouched, got %q", string(template.Payload))
	}

	stored, err := repo.GetView(ctx, "test#1")
	if err != nil {
		t.Fatalf("get stored view: %v", err)
	}
	if stored == nil {
		t.Fatal("expected stored view")
	}
	if stored.PK != template.PK {
		t.Fatalf("expected PK %q, got %q", template.PK, stored.PK)
	}
	if stored.EntityID != template.EntityID {
		t.Fatalf("expected entity ID %q, got %q", template.EntityID, stored.EntityID)
	}
	if stored.EntityType != template.EntityType {
		t.Fatalf("expected entity type %q, got %q", template.EntityType, stored.EntityType)
	}
	if stored.TTL != template.TTL {
		t.Fatalf("expected TTL %d, got %d", template.TTL, stored.TTL)
	}
	if string(stored.Payload) != `{"id":"1","name":"alpha"}` {
		t.Fatalf("unexpected payload: %s", string(stored.Payload))
	}
}

func TestPutJSONViewRequiresRepositoryAndView(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	if err := PutJSONView(ctx, nil, &SerializedView{PK: "test#1"}, codecTestView{}); err == nil {
		t.Fatal("expected repository configuration error")
	}

	if err := PutJSONView(ctx, newCodecTestRepo(), nil, codecTestView{}); err == nil {
		t.Fatal("expected missing serialized view error")
	}
}

func TestPutJSONViewWrapsMarshalError(t *testing.T) {
	t.Parallel()

	err := PutJSONView(
		context.Background(),
		newCodecTestRepo(),
		&SerializedView{PK: "bad#1", SK: "VIEW"},
		struct {
			Bad func() `json:"bad"`
		}{},
	)
	if err == nil {
		t.Fatal("expected marshal error")
	}
	if !strings.Contains(err.Error(), `encode JSON view pk="bad#1" sk="VIEW"`) {
		t.Fatalf("expected encoded view metadata in error, got %v", err)
	}
}

func TestListJSONViewsByEntityTypeStrictDecode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newCodecTestRepo()

	if err := repo.PutView(ctx, &SerializedView{
		PK:         "test#1",
		EntityID:   EntityID("1"),
		EntityType: EntityType("test.view"),
		Payload:    []byte(`{"id":"1","name":"alpha"}`),
	}); err != nil {
		t.Fatalf("put first view: %v", err)
	}
	if err := repo.PutView(ctx, &SerializedView{
		PK:         "test#2",
		EntityID:   EntityID("2"),
		EntityType: EntityType("test.view"),
		Payload:    []byte(`{"id":"2","name":"beta"}`),
	}); err != nil {
		t.Fatalf("put second view: %v", err)
	}

	got, err := ListJSONViewsByEntityType[codecTestView](ctx, repo, EntityType("test.view"), nil)
	if err != nil {
		t.Fatalf("list JSON views: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 views, got %d: %#v", len(got), got)
	}

	byID := map[string]codecTestView{}
	for _, view := range got {
		byID[view.ID] = view
	}

	if byID["1"].Name != "alpha" {
		t.Fatalf("expected alpha view, got %#v", byID["1"])
	}
	if byID["2"].Name != "beta" {
		t.Fatalf("expected beta view, got %#v", byID["2"])
	}
}

func TestListValidJSONViewsByEntityTypeSkipsDecodeErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newCodecTestRepo()

	if err := repo.PutView(ctx, &SerializedView{
		PK:         "test#1",
		EntityID:   EntityID("1"),
		EntityType: EntityType("test.view"),
		Payload:    []byte(`{"id":"1","name":"alpha"}`),
	}); err != nil {
		t.Fatalf("put valid view: %v", err)
	}
	if err := repo.PutView(ctx, &SerializedView{
		PK:         "test#bad",
		EntityID:   EntityID("bad"),
		EntityType: EntityType("test.view"),
		Payload:    []byte(`{not-json`),
	}); err != nil {
		t.Fatalf("put invalid view: %v", err)
	}

	var skipped []string
	got, err := ListValidJSONViewsByEntityType[codecTestView](
		ctx,
		repo,
		EntityType("test.view"),
		nil,
		func(view *SerializedView, err error) {
			if view != nil {
				skipped = append(skipped, view.PK)
			}
			if err == nil {
				t.Fatal("expected decode error callback to receive error")
			}
		},
	)
	if err != nil {
		t.Fatalf("list valid JSON views: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 valid view, got %d: %#v", len(got), got)
	}
	if got[0].ID != "1" {
		t.Fatalf("expected valid view ID 1, got %q", got[0].ID)
	}
	if len(skipped) != 1 || skipped[0] != "test#bad" {
		t.Fatalf("expected test#bad to be skipped, got %#v", skipped)
	}
}

func TestListJSONViewsByPKStrictDecodeFails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newCodecTestRepo()

	if err := repo.PutView(ctx, &SerializedView{
		PK:         "collection#1",
		SK:         "001",
		EntityID:   EntityID("1"),
		EntityType: EntityType("test.view"),
		Payload:    []byte(`{"id":"1","name":"alpha"}`),
	}); err != nil {
		t.Fatalf("put valid collection view: %v", err)
	}
	if err := repo.PutView(ctx, &SerializedView{
		PK:         "collection#1",
		SK:         "002",
		EntityID:   EntityID("2"),
		EntityType: EntityType("test.view"),
		Payload:    []byte(`{not-json`),
	}); err != nil {
		t.Fatalf("put invalid collection view: %v", err)
	}

	got, err := ListJSONViewsByPK[codecTestView](ctx, repo, "collection#1", nil)
	if err == nil {
		t.Fatalf("expected strict decode error, got views %#v", got)
	}

	var decodeErr *JSONViewDecodeError
	if !errors.As(err, &decodeErr) {
		t.Fatalf("expected JSONViewDecodeError, got %T: %v", err, err)
	}
	if decodeErr.PK != "collection#1" || decodeErr.SK != "002" {
		t.Fatalf("expected decode metadata collection#1/002, got %q/%q", decodeErr.PK, decodeErr.SK)
	}
}

func TestListValidJSONViewsByPK(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newCodecTestRepo()

	if err := repo.PutView(ctx, &SerializedView{
		PK:         "collection#1",
		SK:         "001",
		EntityID:   EntityID("1"),
		EntityType: EntityType("test.view"),
		Payload:    []byte(`{"id":"1","name":"alpha"}`),
	}); err != nil {
		t.Fatalf("put valid collection view: %v", err)
	}
	if err := repo.PutView(ctx, &SerializedView{
		PK:         "collection#1",
		SK:         "002",
		EntityID:   EntityID("2"),
		EntityType: EntityType("test.view"),
		Payload:    []byte(`{not-json`),
	}); err != nil {
		t.Fatalf("put invalid collection view: %v", err)
	}

	got, err := ListValidJSONViewsByPK[codecTestView](ctx, repo, "collection#1", nil, nil)
	if err != nil {
		t.Fatalf("list valid JSON views by PK: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 valid view, got %d: %#v", len(got), got)
	}
	if got[0].ID != "1" {
		t.Fatalf("expected valid view ID 1, got %q", got[0].ID)
	}
}

func TestDecodeJSONViewWrapsMetadata(t *testing.T) {
	t.Parallel()

	view := &SerializedView{
		PK:      "bad#1",
		SK:      "VIEW",
		Payload: []byte(`{not-json`),
	}

	var target codecTestView
	err := DecodeJSONView(view, &target)
	if err == nil {
		t.Fatal("expected decode error")
	}

	var decodeErr *JSONViewDecodeError
	if !errors.As(err, &decodeErr) {
		t.Fatalf("expected JSONViewDecodeError, got %T: %v", err, err)
	}
	if decodeErr.PK != "bad#1" {
		t.Fatalf("expected PK bad#1, got %q", decodeErr.PK)
	}
	if decodeErr.SK != "VIEW" {
		t.Fatalf("expected SK VIEW, got %q", decodeErr.SK)
	}
	if decodeErr.Unwrap() == nil {
		t.Fatal("expected wrapped JSON error")
	}
	if !strings.Contains(decodeErr.Error(), `pk="bad#1" sk="VIEW"`) {
		t.Fatalf("expected metadata in error string, got %q", decodeErr.Error())
	}
}

func TestDecodeJSONViewRequiresView(t *testing.T) {
	t.Parallel()

	var target codecTestView
	if err := DecodeJSONView(nil, &target); err == nil {
		t.Fatal("expected missing serialized view error")
	}
}

func TestDecodeJSONViewRejectsNonPointerTarget(t *testing.T) {
	t.Parallel()

	view := &SerializedView{Payload: []byte(`{"id":"1","name":"a"}`)}

	// struct value
	var sval codecTestView
	if err := DecodeJSONView(view, sval); err == nil {
		t.Fatal("expected non-pointer target error for struct value")
	}

	// nil typed pointer
	var nilPtr *codecTestView
	if err := DecodeJSONView(view, nilPtr); err == nil {
		t.Fatal("expected non-pointer target error for nil typed pointer")
	}

	// nil slice
	var nilSlice []codecTestView
	if err := DecodeJSONView(view, nilSlice); err == nil {
		t.Fatal("expected non-pointer target error for nil slice")
	}
}

func TestDecodeJSONViewAcceptsNonNilPointer(t *testing.T) {
	t.Parallel()

	view := &SerializedView{Payload: []byte(`{"id":"1","name":"alpha"}`)}

	var target codecTestView
	if err := DecodeJSONView(view, &target); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target.ID != "1" || target.Name != "alpha" {
		t.Fatalf("unexpected decoded value: %+v", target)
	}
}

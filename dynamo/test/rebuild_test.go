package test

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// rebuildProjector records the entities it projected and returns a fixed TransactionGroup.
type rebuildProjector struct {
	mu    sync.Mutex
	calls []evt.EntityID
	group evt.TransactionGroup
}

func (p *rebuildProjector) Project(_ context.Context, entity evt.Entity, _ []evt.Event) (evt.TransactionGroup, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, entity.GetID())

	return p.group, nil
}

func (p *rebuildProjector) projected() []evt.EntityID {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]evt.EntityID, len(p.calls))
	copy(cp, p.calls)

	return cp
}

// rebuildTxnGroup is a minimal TransactionGroup so a projector can return non-nil view work.
type rebuildTxnGroup struct{}

func (g *rebuildTxnGroup) Len() int { return 1 }
func (g *rebuildTxnGroup) Merge(_ evt.TransactionGroup) (evt.TransactionGroup, error) {
	return g, nil
}
func (g *rebuildTxnGroup) StorageType() evt.StorageType         { return "test" }
func (g *rebuildTxnGroup) TransactionType() evt.TransactionType { return "test-put" }
func (g *rebuildTxnGroup) HandleError(err error, _ int) error   { return err }

func rebuildApplyFunc() func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error) {
	return func(_ context.Context, event evt.SerializedEvent, entity evt.Entity) (evt.Entity, error) {
		if entity == nil {
			return &MockEntity{BaseEntity: evt.NewEntity(event.EntityID)}, nil
		}

		return entity, nil
	}
}

// testTypedEntity reports a concrete Type ("TestEntity") so it survives the rebuild's defensive
// per-entity type check when cfg.EntityType is set — unlike MockEntity, which always reports
// "MockEntity".
type testTypedEntity struct{ *MockEntity }

func (e *testTypedEntity) Type() evt.EntityType { return "TestEntity" }

func typedApplyFunc() func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error) {
	return func(_ context.Context, event evt.SerializedEvent, entity evt.Entity) (evt.Entity, error) {
		if entity == nil {
			return &testTypedEntity{MockEntity: &MockEntity{BaseEntity: evt.NewEntity(event.EntityID)}}, nil
		}

		return entity, nil
	}
}

// Test_RebuildProjectionsByQuery_ScanEnumeration drives the wrapper over the default scan-based
// enumeration: it builds the enumerate-then-query stream and feeds it to the rebuild, committing one
// view group per entity.
func (s *RepositorySuite) Test_RebuildProjectionsByQuery_ScanEnumeration() {
	ctx := context.Background()

	s.client.On("Scan", mock.Anything, mock.MatchedBy(func(in *dynamodb.ScanInput) bool {
		return in.ProjectionExpression != nil && *in.ProjectionExpression == "pk"
	}), mock.Anything).Return(&dynamodb.ScanOutput{
		Items: []map[string]types.AttributeValue{pkItem("a"), pkItem("b")},
	}, nil).Once()

	s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK("a")), mock.Anything).
		Return(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{eventItem("a", 1)}}, nil).Once()
	s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK("b")), mock.Anything).
		Return(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{eventItem("b", 1)}}, nil).Once()

	proj := &rebuildProjector{group: &rebuildTxnGroup{}}

	var committed int
	cfg := evt.RebuildConfig{
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: func(context.Context, evt.TransactionGroup) error { committed++; return nil },
	}

	res, err := s.repo.RebuildProjectionsByQuery(ctx, dynamo.StreamByQueryOptions{Workers: 1}, rebuildApplyFunc(), cfg)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, res.Processed)
	require.Empty(s.T(), res.Errors)
	require.Equal(s.T(), 2, committed)
	require.ElementsMatch(s.T(), []evt.EntityID{"a", "b"}, proj.projected())
	s.client.AssertExpectations(s.T())
}

// Test_RebuildProjectionsByQuery_HeadSource drives the wrapper over the opt-in registry enumeration:
// IDs come from the heads source, not a key-only event-log scan. No Scan is registered, so any
// fallback to the scan path would surface as an unexpected mock call.
func (s *RepositorySuite) Test_RebuildProjectionsByQuery_HeadSource() {
	ctx := context.Background()

	heads := &fakeHeadVisitor{heads: []headEntry{
		{id: "a", seq: 1, typ: "TestEntity"},
		{id: "b", seq: 1, typ: "TestEntity"},
	}}

	s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK("a")), mock.Anything).
		Return(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{eventItem("a", 1)}}, nil).Once()
	s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK("b")), mock.Anything).
		Return(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{eventItem("b", 1)}}, nil).Once()

	proj := &rebuildProjector{group: &rebuildTxnGroup{}}
	cfg := evt.RebuildConfig{
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: func(context.Context, evt.TransactionGroup) error { return nil },
	}

	opts := dynamo.StreamByQueryOptions{Workers: 1, HeadSource: heads}
	res, err := s.repo.RebuildProjectionsByQuery(ctx, opts, rebuildApplyFunc(), cfg)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, res.Processed)
	require.ElementsMatch(s.T(), []evt.EntityID{"a", "b"}, proj.projected())
	require.Equal(s.T(), []evt.EntityID{"a", "b"}, heads.streamed, "IDs came from the registry")
	s.client.AssertExpectations(s.T())
}

// Test_RebuildProjectionsByQuery_DefaultsEntityTypeFromConfig verifies that an empty opts.EntityType
// inherits cfg.EntityType so the type is set once in the rebuild config and still scopes enumeration.
func (s *RepositorySuite) Test_RebuildProjectionsByQuery_DefaultsEntityTypeFromConfig() {
	ctx := context.Background()

	heads := &fakeHeadVisitor{heads: []headEntry{
		{id: "a", seq: 1, typ: "TestEntity"},
		{id: "z", seq: 1, typ: "OtherEntity"},
	}}

	s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK("a")), mock.Anything).
		Return(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{eventItem("a", 1)}}, nil).Once()

	proj := &rebuildProjector{group: &rebuildTxnGroup{}}
	cfg := evt.RebuildConfig{
		EntityType:  evt.EntityType("TestEntity"),
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: func(context.Context, evt.TransactionGroup) error { return nil },
	}

	// opts.EntityType left empty: it should default to cfg.EntityType and scope the head source.
	opts := dynamo.StreamByQueryOptions{Workers: 1, HeadSource: heads}
	res, err := s.repo.RebuildProjectionsByQuery(ctx, opts, typedApplyFunc(), cfg)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, res.Processed)
	require.Equal(s.T(), evt.EntityType("TestEntity"), heads.gotType, "cfg.EntityType scoped enumeration")
	require.Equal(s.T(), []evt.EntityID{"a"}, proj.projected())
	s.client.AssertExpectations(s.T())
}

// Test_RebuildProjectionsByQuery_ScanEntityTypeFromConfig verifies the scan path also inherits
// cfg.EntityType: with no HeadSource, the enumeration Scan carries the entityType filter and the
// rebuild's per-entity check passes for matching entities.
func (s *RepositorySuite) Test_RebuildProjectionsByQuery_ScanEntityTypeFromConfig() {
	ctx := context.Background()

	// Enumeration scan must filter on the inherited entity type.
	s.client.On("Scan", mock.Anything, mock.MatchedBy(func(in *dynamodb.ScanInput) bool {
		v, ok := in.ExpressionAttributeValues[":et"].(*types.AttributeValueMemberS)
		return in.FilterExpression != nil && ok && v.Value == "TestEntity"
	}), mock.Anything).Return(&dynamodb.ScanOutput{
		Items: []map[string]types.AttributeValue{pkItem("a")},
	}, nil).Once()

	s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK("a")), mock.Anything).
		Return(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{eventItem("a", 1)}}, nil).Once()

	proj := &rebuildProjector{group: &rebuildTxnGroup{}}
	cfg := evt.RebuildConfig{
		EntityType:  evt.EntityType("TestEntity"),
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: func(context.Context, evt.TransactionGroup) error { return nil },
	}

	// opts.EntityType empty: defaults to cfg.EntityType and scopes the enumeration scan.
	res, err := s.repo.RebuildProjectionsByQuery(ctx, dynamo.StreamByQueryOptions{Workers: 1}, typedApplyFunc(), cfg)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, res.Processed)
	require.Equal(s.T(), []evt.EntityID{"a"}, proj.projected())
	s.client.AssertExpectations(s.T())
}

// Test_RebuildProjectionsByQuery_ConfigErrorFailsFast confirms an invalid config (missing
// CommitGroup, or no projectors) is rejected before any stream is started — no Scan or Query is
// registered, so a launched enumeration would surface as an unexpected mock call.
func (s *RepositorySuite) Test_RebuildProjectionsByQuery_ConfigErrorFailsFast() {
	ctx := context.Background()

	// DryRun false with no CommitGroup must fail before consuming read capacity.
	missingCommit := evt.RebuildConfig{Projectors: []evt.EventProjector{&rebuildProjector{}}}
	res, err := s.repo.RebuildProjectionsByQuery(ctx, dynamo.StreamByQueryOptions{Workers: 1}, rebuildApplyFunc(), missingCommit)
	require.Nil(s.T(), res)
	require.ErrorContains(s.T(), err, "CommitGroup is required")

	// No projectors must also fail before streaming.
	noProjectors := evt.RebuildConfig{CommitGroup: func(context.Context, evt.TransactionGroup) error { return nil }}
	res, err = s.repo.RebuildProjectionsByQuery(ctx, dynamo.StreamByQueryOptions{Workers: 1}, rebuildApplyFunc(), noProjectors)
	require.Nil(s.T(), res)
	require.ErrorContains(s.T(), err, "at least one projector is required")

	s.client.AssertExpectations(s.T())
}

// Test_RebuildProjectionsByQuery_RejectsSeedEntity confirms the query path refuses a snapshot seeder
// rather than silently ignoring it: StreamEntitiesByQuery replays only surviving events, so honoring
// a SeedEntity config on a compacted stream would commit projections built from truncated history.
// The rejection happens before any stream starts, so no Scan/Query is registered.
func (s *RepositorySuite) Test_RebuildProjectionsByQuery_RejectsSeedEntity() {
	ctx := context.Background()

	cfg := evt.RebuildConfig{
		Projectors:  []evt.EventProjector{&rebuildProjector{}},
		CommitGroup: func(context.Context, evt.TransactionGroup) error { return nil },
		SeedEntity: func(context.Context, evt.SerializedSnapshot) (evt.Entity, error) {
			return nil, nil
		},
	}

	res, err := s.repo.RebuildProjectionsByQuery(ctx, dynamo.StreamByQueryOptions{Workers: 1}, rebuildApplyFunc(), cfg)
	require.Nil(s.T(), res)
	require.ErrorContains(s.T(), err, "SeedEntity is not supported")
	s.client.AssertExpectations(s.T())
}

// Test_RebuildProjectionsByQuery_NilApplyEvent rejects a nil replay callback before starting a
// stream, mirroring evt.RebuildProjections.
func (s *RepositorySuite) Test_RebuildProjectionsByQuery_NilApplyEvent() {
	ctx := context.Background()

	cfg := evt.RebuildConfig{
		Projectors:  []evt.EventProjector{&rebuildProjector{}},
		CommitGroup: func(context.Context, evt.TransactionGroup) error { return nil },
	}

	res, err := s.repo.RebuildProjectionsByQuery(ctx, dynamo.StreamByQueryOptions{}, nil, cfg)
	require.Nil(s.T(), res)
	require.ErrorContains(s.T(), err, "applyEvent callback is required")
	// No Scan or Query was registered, so the mock confirms no stream was started.
	s.client.AssertExpectations(s.T())
}

// Test_RebuildProjectionsByQuery_EntityTypeMismatch rejects conflicting enumeration and rebuild
// types before starting a stream, instead of silently enumerating one type while the rebuild skips
// it as the wrong type.
func (s *RepositorySuite) Test_RebuildProjectionsByQuery_EntityTypeMismatch() {
	ctx := context.Background()

	cfg := evt.RebuildConfig{
		EntityType:  evt.EntityType("Order"),
		Projectors:  []evt.EventProjector{&rebuildProjector{}},
		CommitGroup: func(context.Context, evt.TransactionGroup) error { return nil },
	}

	opts := dynamo.StreamByQueryOptions{EntityType: evt.EntityType("User")}
	res, err := s.repo.RebuildProjectionsByQuery(ctx, opts, rebuildApplyFunc(), cfg)
	require.Nil(s.T(), res)
	require.ErrorContains(s.T(), err, "entity type mismatch")
	s.client.AssertExpectations(s.T())
}

package compat

import (
	"context"
	"fmt"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
)

// CommitViewVector pins the DynamoDB requests of a commit that carries commit-wide view operations
// (ADR 0004, C7): the exact actions, tables, conditions, and values, in order. The marker check
// comes first, then the projector operations in declaration then emission order, then each written
// stream's group and each fence check. Both languages build the Request and compare their requests
// with Output.
type CommitViewVector struct {
	Name    string             `json:"name"`
	Request CommitViewRequest  `json:"request"`
	Output  []FeatureWriteItem `json:"output"`

	// Sizes is the commit budget's item estimate for the events, snapshots, head updates, and fence
	// checks (evt.TransactionItemSizesWithTTL), followed by the size of each view operation. Both
	// languages must estimate the same bytes so CommitTooLarge fires at the same boundary.
	Sizes []int `json:"sizes"`
}

// CommitViewRequest is the commit: the streams it writes, the fences it carries, and the view
// operations of its projectors. The store declares stream-heads and holds the seeded streams.
type CommitViewRequest struct {
	// EventsTable and ViewsTable are the tables the requests name.
	EventsTable string `json:"eventsTable"`
	ViewsTable  string `json:"viewsTable"`

	Seeded []SeededStream `json:"seeded,omitempty"`

	// SeededViews are view rows [pk, sk, owner] the store holds, so a "present" delete finds its
	// row.
	SeededViews [][3]string `json:"seededViews,omitempty"`

	Streams    []HeadStreamRequest `json:"streams"`
	Fences     []HeadFenceRequest  `json:"fences,omitempty"`
	Projectors []StepProjector     `json:"projectors"`
}

func commitViewVectors() []CommitViewVector {
	return []CommitViewVector{
		// Every shape of operation, from two projectors, before two written streams (one with a
		// snapshot) and two fences: the storage spec's layout, with views in action group 3.
		{Name: "views-before-the-streams", Request: CommitViewRequest{
			EventsTable: fakeTable, ViewsTable: ViewsTable,
			Seeded: []SeededStream{
				{EntityID: "a", Head: 7, SnapshotSequence: 1, SnapshotEventSequence: 6},
				{EntityID: "m", Head: 3},
			},
			Streams: []HeadStreamRequest{
				{EntityID: "b", EntityType: "account", First: 1, Count: 1},
				{EntityID: "a", EntityType: "account", First: 8, Count: 1, SnapshotSequence: 2},
			},
			Fences:      []HeadFenceRequest{{EntityID: "z", Head: 0}, {EntityID: "m", Head: 3}},
			SeededViews: [][3]string{{"handles", "grace", "a"}},
			Projectors: []StepProjector{
				{Name: "directory", Operations: []StepView{
					{Op: "put", PK: "directory", SK: "acct#a", Owner: "a", EntityType: "account", Payload: `{"status":"open"}`},
					{Op: "put", PK: "handles", SK: "ada", Owner: "a", EntityType: "account", Payload: `{"owner":"a"}`, Condition: "absent"},
					{Op: "delete", PK: "handles", SK: "grace", Owner: "a", Condition: "present"},
				}},
				{Name: "audit", Operations: []StepView{
					{Op: "delete", PK: "audit", SK: "pending", Owner: "b"},
					{Op: "put", PK: "audit", SK: "c1", Owner: "b", EntityType: "", Payload: `{"streams":2}`},
				}},
			},
		}},
		{Name: "single-stream", Request: CommitViewRequest{
			EventsTable: fakeTable, ViewsTable: ViewsTable,
			Streams: []HeadStreamRequest{{EntityID: "acct-1", EntityType: "account", First: 1, Count: 2}},
			Projectors: []StepProjector{{Name: "ledger", Operations: []StepView{
				{Op: "put", PK: "ledger", SK: "acct-1", Owner: "acct-1", EntityType: "account", Payload: `{"events":2}`},
			}}},
		}},
	}
}

// GenerateCommitViewVectors sends every request through the real DynamoDB repository against a
// fake table and records the actions it sent.
func GenerateCommitViewVectors(ctx context.Context) ([]CommitViewVector, error) {
	vectors := commitViewVectors()

	for i := range vectors {
		result := commitViewResult(vectors[i].Request)

		capture := &featureCaptureDynamo{FakeDynamo: NewFakeDynamo()}
		repo := dynamo.NewRepository(capture, fakeTable).WithLazyInitialization(true).WithViewsTable(ViewsTable)

		if _, err := repo.FeatureMarker(nil).DeclareStreamHeads(ctx); err != nil {
			return nil, fmt.Errorf("commit view vector %s: %w", vectors[i].Name, err)
		}

		for _, seed := range vectors[i].Request.Seeded {
			if err := seedStream(ctx, capture.FakeDynamo, seed); err != nil {
				return nil, fmt.Errorf("commit view vector %s: %w", vectors[i].Name, err)
			}
		}

		for _, seeded := range vectors[i].Request.SeededViews {
			capture.SeedView(seeded[0], seeded[1], seeded[2])
		}

		capture.writes = nil

		if err := repo.Commit(ctx, result); err != nil {
			return nil, fmt.Errorf("commit view vector %s: %w", vectors[i].Name, err)
		}

		vectors[i].Output = capture.writes

		sizes, err := evt.TransactionItemSizes(withoutViews(result))
		if err != nil {
			return nil, fmt.Errorf("commit view vector %s: %w", vectors[i].Name, err)
		}

		for _, view := range result.Views {
			sizes = append(sizes, evt.ViewOperationSize(view.Op))
		}

		vectors[i].Sizes = sizes
	}

	return vectors, nil
}

// commitViewResult builds the commit a request describes: the stream-head vectors' events,
// snapshots, and fences, and the view operations of its projectors.
func commitViewResult(request CommitViewRequest) evt.SerializedResult {
	result := headVectorCommit(StreamHeadRequest{Streams: request.Streams, Fences: request.Fences})
	result.Views = viewsOf("", Step{Projectors: request.Projectors})

	return result
}

func withoutViews(result evt.SerializedResult) evt.SerializedResult {
	result.Views = nil

	return result
}

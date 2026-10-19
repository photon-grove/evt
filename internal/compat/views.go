package compat

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/postgres"
)

// Commit-wide view operations in the harness (ADR 0004, C7). Two kinds of scenario use them:
//
//   - a transaction step names its projectors and operations in the scenario file (Step.Projectors),
//     so the layout, the conditions, and the typed failures are pinned as data;
//   - a scenario marked "projector" registers the shared LedgerProjector on its stores, so the same
//     commit view must produce byte-identical rows in both languages.

// ViewsTable is the name of the views table of the fixture-generating fake.
const ViewsTable = "evt-compat-views"

// LedgerPK is the partition the shared ledger projector writes. It names no entity (it is not an
// {entityType}#{entityId} partition), and its sort keys are stream IDs, so the rows of different
// runs in a live table are told apart by run.
const LedgerPK = "ledger"

// StepProjector is one projector of a transaction step: its name and the operations it emits.
type StepProjector struct {
	Name       string     `json:"name"`
	Operations []StepView `json:"operations"`
}

// StepView is one view operation of a transaction step. The placeholder {id} in pk, sk, entityId,
// and payload is the scenario's entity ID, so rows are scoped to the run. PKPad and SKPad extend a
// key with "x" up to that many bytes, so the key-limit cases need not store a 2 KB string.
type StepView struct {
	Op         string `json:"op"`
	PK         string `json:"pk"`
	SK         string `json:"sk"`
	EntityID   string `json:"entityId,omitempty"`
	EntityType string `json:"entityType,omitempty"`
	Payload    string `json:"payload,omitempty"`
	Condition  string `json:"condition,omitempty"`
	PKPad      int    `json:"pkPad,omitempty"`
	SKPad      int    `json:"skPad,omitempty"`

	// Conflict maps the operation's failed condition to an error with this message.
	Conflict string `json:"conflict,omitempty"`
}

// viewsOf builds the step's commit-wide view operations, in declaration then emission order.
func viewsOf(base evt.EntityID, step Step) []evt.ProjectedView {
	var views []evt.ProjectedView

	scope := func(text string) string { return strings.ReplaceAll(text, "{id}", string(base)) }
	pad := func(text string, size int) string {
		if len(text) < size {
			text += strings.Repeat("x", size-len(text))
		}

		return text
	}

	for index, projector := range step.Projectors {
		for operation, item := range projector.Operations {
			op := evt.ViewOperation{
				Kind:       evt.ViewOperationKind(item.Op),
				PK:         pad(scope(item.PK), item.PKPad),
				SK:         pad(scope(item.SK), item.SKPad),
				EntityID:   evt.EntityID(scope(item.EntityID)),
				EntityType: evt.EntityType(item.EntityType),
				Payload:    []byte(scope(item.Payload)),
				Condition:  evt.ViewCondition(item.Condition),
			}
			if item.Conflict != "" {
				op.Conflict = errors.New(item.Conflict)
			}

			views = append(views, evt.ProjectedView{
				Projector: projector.Name, ProjectorIndex: index, Operation: operation, Op: op,
			})
		}
	}

	return views
}

// ledgerRow is the payload of a ledger row: the account's state after the commit and what the
// commit did to it. Declared in this order, it encodes the same bytes as the Rust struct.
type ledgerRow struct {
	BalanceCents int64  `json:"balanceCents"`
	Status       string `json:"status"`
	Tags         int    `json:"tags"`
	Events       int    `json:"events"`
}

// LedgerProjector is the shared commit-wide projector of the account domain (name "ledger"). For
// every stream the commit wrote it puts a ledger row (pk "ledger", sk the stream ID) from the
// stream's state after the commit, or deletes it when the balance is zero. Every byte is a function
// of the commit view, so Rust must write the same rows.
type LedgerProjector struct{}

// Name implements evt.CommitProjector.
func (LedgerProjector) Name() string { return "ledger" }

// ProjectCommit implements evt.CommitProjector.
func (LedgerProjector) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	var operations []evt.ViewOperation

	for _, stream := range commit.Streams {
		var state Account
		if err := json.Unmarshal(stream.State, &state); err != nil {
			return nil, err
		}

		if state.BalanceCents == 0 {
			operations = append(operations, evt.DeleteView(LedgerPK, string(stream.EntityID)))

			continue
		}

		payload, err := json.Marshal(ledgerRow{
			BalanceCents: state.BalanceCents, Status: state.Status, Tags: len(state.Tags), Events: len(stream.Events),
		})
		if err != nil {
			return nil, err
		}

		operations = append(operations, evt.PutView(LedgerPK, string(stream.EntityID), stream.EntityID, stream.EntityType, payload))
	}

	return operations, nil
}

// ViewRow is a stored view row in the neutral form both backends and both languages compare: the
// five attributes, whatever the table that holds them.
type ViewRow struct {
	PK         string
	SK         string
	EntityID   string
	EntityType string
	Payload    string
}

// Row derives the neutral row from a fixture's DynamoDB item.
func (v ViewExpectation) Row() ViewRow {
	text := func(name string) string {
		attribute, ok := v.DynamoItem[name].(map[string]any)
		if !ok {
			return ""
		}

		value, ok := attribute["S"].(string)
		if !ok {
			return ""
		}

		return value
	}

	return ViewRow{PK: text("pk"), SK: text("sk"), EntityID: text("entityID"), EntityType: text("entityType"), Payload: text("payload")}
}

// PostgresViewTable is the application-owned table the live PostgreSQL runs write view rows to. evt
// defines no view schema; the drivers of both languages create this one.
const PostgresViewTable = "evt_compat_views"

// PostgresViewDDL creates PostgresViewTable.
const PostgresViewDDL = `CREATE TABLE IF NOT EXISTS evt_compat_views (
    pk          text NOT NULL,
    sk          text NOT NULL,
    entity_id   text NOT NULL,
    entity_type text NOT NULL,
    payload     text NOT NULL,
    PRIMARY KEY (pk, sk)
)`

// PostgresViewWriter writes a view operation to PostgresViewTable, with the statement semantics the
// repository relies on: a put that requires the row to be absent is a plain INSERT (the unique
// violation is its failed condition), an unconditional put overwrites, and a delete's affected-row
// count decides a "present" condition. The Rust driver issues the same statements.
func PostgresViewWriter(op evt.ViewOperation) (postgres.ViewStatement, error) {
	if op.Kind == evt.ViewDelete {
		return postgres.ViewStatement{
			SQL:  "DELETE FROM evt_compat_views WHERE pk = $1 AND sk = $2",
			Args: []any{op.PK, op.SK},
		}, nil
	}

	args := []any{op.PK, op.SK, string(op.EntityID), string(op.EntityType), string(op.Payload)}
	insert := "INSERT INTO evt_compat_views (pk, sk, entity_id, entity_type, payload) VALUES ($1, $2, $3, $4, $5)"

	if op.Condition == evt.ViewAbsent {
		return postgres.ViewStatement{SQL: insert, Args: args}, nil
	}

	return postgres.ViewStatement{
		SQL: insert + " ON CONFLICT (pk, sk) DO UPDATE SET entity_id = EXCLUDED.entity_id, " +
			"entity_type = EXCLUDED.entity_type, payload = EXCLUDED.payload",
		Args: args,
	}, nil
}

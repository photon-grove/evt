package evt

import (
	"context"
)

// EventProjector generates transactional view operations that should be committed alongside events.
type EventProjector interface {
	// Project produces a transaction group for the given entity state. Returning nil
	// indicates no operations are required.
	Project(context.Context, Entity, []Event) (TransactionGroup, error)
}

// // ViewTransactionBuilder converts serialized views into transaction groups for the backing store.
// type ViewTransactionBuilder func(views ...*SerializedView) (TransactionGroup, error)

// // EntityViewProjector writes a simple by-id snapshot of the entity to the views table.
// type EntityViewProjector struct {
// 	build ViewTransactionBuilder
// }

// // NewEntityViewProjector creates a projector that upserts the entity snapshot keyed by ID.
// func NewEntityViewProjector(builder ViewTransactionBuilder) *EntityViewProjector {
// 	return &EntityViewProjector{build: builder}
// }

// // Project generates a single view put transaction keyed by entity ID.
// func (p *EntityViewProjector) Project(_ context.Context, entity Entity, _ []Event) (TransactionGroup, error) {
// 	if p == nil || p.build == nil || entity == nil {
// 		return nil, nil
// 	}
// 	entityID := entity.GetID()
// 	if entityID == "" {
// 		return nil, nil
// 	}

// 	payload, err := json.Marshal(entity)
// 	if err != nil {
// 		return nil, err
// 	}

// 	view := &SerializedView{
// 		PK:         string(entityID),
// 		EntityID:   entityID,
// 		EntityType: entity.Type(),
// 		Payload:    payload,
// 	}

// 	return p.build(view)
// }

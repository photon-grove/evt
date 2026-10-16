package snapshots

import (
	"context"
	"fmt"

	"github.com/photon-grove/evt"
)

// Transaction prepares one atomic multi-stream commit (ADR 0004, C1/C2). A caller loads a typed
// read set with Read, appends events to any subset with Append, adds caller-stamped fences with
// Fence, and commits once with Commit. Streams that were read but never written contribute
// engine-filled read fences, so the commit fails when another writer moved them past the head
// the reader saw.
//
// The single-aggregate shortcut stays: Store.Execute remains a load, handle, and commit of one
// entity, implemented over the same repository commit path.
//
// A Transaction commits at most once: Commit consumes it whether it succeeds or fails, and a
// consumed Transaction refuses every later call. Errors from the staging calls split in two.
// Input validation errors leave the Transaction unchanged and usable: a nil entity or a duplicate
// stream passed to Read, and an Append to a stream that was not read. Errors that can leave the
// staged state inconsistent consume it: a failed load in Read, and an event of the wrong entity
// type or one the entity cannot apply in Append. Build a new Transaction after those.
type Transaction struct {
	store   *Store
	streams map[evt.EntityID]*transactionStream
	order   []evt.EntityID
	fences  []evt.ReadFence
	used    bool
}

// transactionStream is one read aggregate and the events appended to it since the read.
type transactionStream struct {
	entity  evt.Entity
	context evt.Context
	pending []evt.Event
}

// Transaction begins a new multi-stream transaction on the store.
func (store *Store) Transaction() *Transaction {
	return &Transaction{
		store:   store,
		streams: make(map[evt.EntityID]*transactionStream),
	}
}

// Read loads an aggregate into the transaction's read set. The entity instance is filled from
// the snapshot and events like Store.LoadEntity, so appended events keep its snapshot payload
// consistent. A stream that receives no events before Commit is fenced at the head it was read
// at; a stream that receives events is written.
func (tx *Transaction) Read(ctx context.Context, entity evt.Entity, entityID evt.EntityID) error {
	if tx.used {
		return fmt.Errorf("snapshots: transaction is already consumed")
	}
	if entity == nil {
		return fmt.Errorf("snapshots: transaction read of %s has a nil entity", entityID)
	}
	if _, exists := tx.streams[entityID]; exists {
		return fmt.Errorf("snapshots: duplicate read of %s", entityID)
	}

	context, err := tx.store.LoadEntity(ctx, entity, entityID)
	if err != nil {
		tx.used = true

		return err
	}

	// Keep the entity's internal ID aligned with the store key, as Execute does, so projectors
	// write views to the right partition.
	if setter, ok := entity.(interface{ SetID(evt.EntityID) }); ok {
		setter.SetID(entityID)
	}

	tx.streams[entityID] = &transactionStream{entity: entity, context: context}
	tx.order = append(tx.order, entityID)

	return nil
}

// Append applies domain events to an already-read stream, preparing its post-commit state. The
// events are committed only when Commit succeeds.
func (tx *Transaction) Append(entityID evt.EntityID, events ...evt.Event) error {
	if tx.used {
		return fmt.Errorf("snapshots: transaction is already consumed")
	}

	stream, ok := tx.streams[entityID]
	if !ok {
		return fmt.Errorf("snapshots: stream %s was not read by this transaction", entityID)
	}

	for _, event := range events {
		if event.EntityType() != stream.entity.Type() {
			tx.used = true

			return fmt.Errorf("snapshots: event type %s does not belong to stream %s", event.EntityType(), entityID)
		}
		if err := stream.entity.Apply(event); err != nil {
			tx.used = true

			return err
		}
	}

	stream.pending = append(stream.pending, events...)

	return nil
}

// Fence adds a caller-stamped read fence for a stream the engine did not load. The fence fails
// the commit when another writer moved the stream past the given head. A fence for a stream
// this transaction read is dropped, whether Fence is called before or after Read: the engine's
// own fence provenance supersedes it.
func (tx *Transaction) Fence(fence evt.ReadFence) error {
	if tx.used {
		return fmt.Errorf("snapshots: transaction is already consumed")
	}
	if _, exists := tx.streams[fence.EntityID]; exists {
		return nil
	}

	fence.CallerStamped = true
	tx.fences = append(tx.fences, fence)

	return nil
}

// Commit stamps the commit ID once, prepares every written stream's events, projector
// operations, and snapshot, fences every unwritten read, and persists exactly one atomic
// repository commit. On failure nothing is written.
func (tx *Transaction) Commit(ctx context.Context, metadata evt.Metadata) ([]evt.SerializedEvent, error) {
	if tx.used {
		return nil, fmt.Errorf("snapshots: transaction is already consumed")
	}
	tx.used = true

	metadata = evt.CommitMetadata(metadata)

	// A caller-stamped fence for a stream this transaction read is superseded by the engine's own
	// fence (or by the write), whichever order Fence and Read were called in.
	result := evt.SerializedResult{}
	for _, fence := range tx.fences {
		if _, read := tx.streams[fence.EntityID]; !read {
			result.Fences = append(result.Fences, fence)
		}
	}

	for _, entityID := range tx.order {
		stream := tx.streams[entityID]

		if metadata.CommandID != nil && stream.context.HasCommandID(*metadata.CommandID) {
			return nil, evt.NewDuplicateCommandError(*metadata.CommandID)
		}

		if len(stream.pending) == 0 {
			// An unwritten read is fenced at the head it saw: the larger of its highest event
			// sequence and its snapshot's event sequence.
			result.Fences = append(result.Fences, evt.ReadFence{
				EntityID:   entityID,
				EntityType: stream.entity.Type(),
				Head:       streamHead(stream.context),
			})

			continue
		}

		serialized, err := evt.SerializeEventsWithContext(stream.pending, &stream.context, metadata)
		if err != nil {
			return nil, err
		}

		projectorTransaction, err := buildProjectorTransaction(ctx, stream.entity, stream.pending)
		if err != nil {
			return nil, err
		}
		result.Transaction = evt.MergeTransactions(result.Transaction, projectorTransaction)
		result.Events = append(result.Events, serialized...)

		if evt.CalculateAdditionalEvents(
			*stream.context.CurrentSequence, len(stream.pending), tx.store.effectiveSnapshotSize(stream.entity.Type()),
		) > 0 {
			payload, err := tx.store.updateSnapshotWithEvents(serialized, &stream.context)
			if err != nil {
				return nil, err
			}

			result.Snapshots = append(result.Snapshots, evt.SerializedSnapshot{
				EntityType:    stream.entity.Type(),
				EntityID:      entityID,
				Sequence:      *stream.context.CurrentSnapshot,
				EventSequence: serialized[len(serialized)-1].Sequence,
				Payload:       payload,
			})
		}
	}

	normalized, err := evt.NormalizeCommit(result)
	if err != nil {
		return nil, err
	}

	if len(normalized.Events) == 0 && len(normalized.Snapshots) == 0 && len(normalized.Fences) == 0 {
		return nil, nil
	}

	if err := tx.store.repo.Commit(ctx, normalized); err != nil {
		return nil, err
	}

	return normalized.Events, nil
}

// streamHead returns the head a fenced read observed: the loaded context's current event
// sequence. LoadEntity seeds it from the snapshot's event sequence and advances it over later
// events, so it is already the larger of the stream's highest event sequence and its snapshot's
// event sequence — including a compacted stream, whose events a snapshot covers.
func streamHead(context evt.Context) evt.EventSequence {
	if context.CurrentSequence == nil {
		return 0
	}

	return *context.CurrentSequence
}

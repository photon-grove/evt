package evt

import (
	"context"
	"errors"
	"fmt"

	"github.com/photon-grove/evt/result" //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
)

// StorageType identifies the backend storage system a Repository uses.
type StorageType string

// StreamFilter is a backend-neutral filter for table-wide streams and rebuilds. Its zero value
// matches every entity. The core contract supports entity-type filtering only; backends may expose
// richer filters through extensions.
type StreamFilter struct {
	// EntityType, when non-empty, restricts the stream to entities of that type.
	EntityType EntityType
}

// Matches reports whether an entity of the given type passes the filter. A zero filter (empty
// EntityType) matches every entity. Backends without server-side filtering can use this to filter
// client-side and still honor the contract.
func (f StreamFilter) Matches(entityType EntityType) bool {
	return f.EntityType == "" || f.EntityType == entityType
}

// Store handles executing Commands, committing Events yielded by those Commands,
// and loading Entity instances from a Repository.
type Store interface {
	// LoadEntity loads the given Entity instance with the given id, retrieving Snapshots
	// and Events from the Repository.
	LoadEntity(
		ctx context.Context,
		entity Entity,
		entityID EntityID,
	) (Context, error)

	// Commit new Events to the Entity within the given context, with optional Metadata.
	Commit(
		ctx context.Context,
		result CommandResult,
		context Context,
		metadata Metadata,
	) ([]SerializedEvent, error)

	// Execute takes an empty Entity instance, loads it from the Repository, handles the given
	// Command, commits the resulting Events using the given Metadata, and applies those Events to the
	// current Entity instance.
	Execute(
		ctx context.Context,
		entity Entity,
		entityID EntityID,
		command Command,
		metadata Metadata,
	) error
}

// Repository manages events for entities consistently and persists snapshots to improve load time.
type Repository interface {
	// Commit new Events for an Entity instance, or for several streams at once.
	//
	// Since v0.9 (ADR 0004, C1/C2/C7) a SerializedResult may also carry Snapshots (per-stream
	// snapshot puts), Fences (read-only streams pinned to an observed head), and Views (the
	// commit-wide projectors' view operations). An implementation that cannot honor Snapshots,
	// Fences, or Views atomically with the Events MUST return an error rather than ignore them: silently dropping a fence commits a decision made on a stale read
	// with no isolation guarantee, and silently dropping a snapshot loses a snapshot the engine
	// believes is durable. An implementation that accepts them must write every stream, snapshot,
	// and projector operation in one transaction (all or nothing), refuse an oversized commit
	// with *CommitTooLargeError before any write (see PreflightCommit), and report a moved fence
	// as *FenceConflictError.
	//
	// An implementation that honors all of this declares it by implementing
	// TransactionalRepository; transaction APIs refuse one that does not.
	Commit(ctx context.Context, result SerializedResult) error

	// Commit a stream of Events using whatever batch size works best for the Repository.
	//
	// Deprecated: Cut in ADR-0006 (K1): CommitStream bypasses command outcomes, the journal, and
	// projections. Commit batched commands through the normal commit path instead (see
	// docs/adr/0006-parity-policy-and-feature-cuts.md). It will be removed in the next minor release.
	CommitStream(ctx context.Context, channel <-chan result.Result[SerializedResult]) []error

	// Commit new Events for an Entity instance while also saving the given Snapshot.
	CommitWithSnapshot(
		ctx context.Context,
		result SerializedResult,
		entityType EntityType,
		entityID EntityID,
		payload []byte,
		currentSnapshot EventSequence,
	) error

	// Get all Events for an Entity instance.
	GetEvents(
		ctx context.Context,
		entityID EntityID,
	) ([]SerializedEvent, error)

	// Get the latest Events beyond the given sequence number for an Entity, used for
	// finding Events after a particular Snapshot.
	GetLatestEvents(
		ctx context.Context,
		entityID EntityID,
		lastSequence EventSequence,
	) ([]SerializedEvent, error)

	// GetRecentEvents returns at most limit Events newest-first (descending sequence) — a bounded
	// "recent activity" read that must not replay a long stream. A limit below 1 is a programmer
	// error returned before any request.
	GetRecentEvents(
		ctx context.Context,
		entityID EntityID,
		limit int,
	) ([]SerializedEvent, error)

	// Get the latest Snapshot for an Entity instance.
	GetSnapshot(
		ctx context.Context,
		entityID EntityID,
	) (*SerializedSnapshot, error)

	// StreamAllEvents returns a channel of all Events in the Repository, optionally narrowed by the
	// backend-neutral StreamFilter. A zero filter streams every event.
	StreamAllEvents(
		ctx context.Context,
		filter StreamFilter,
	) <-chan result.Result[[]SerializedEvent]

	// StreamEntities returns a channel of all Entities in the Repository, optionally narrowed by the
	// backend-neutral StreamFilter, with the given function applied to each Entity to fold the
	// application-specific Events into each instance. A zero filter streams every entity.
	StreamEntities(
		ctx context.Context,
		filter StreamFilter,
		applyEvent func(context.Context, SerializedEvent, Entity) (Entity, error),
	) <-chan result.Result[Entity]
}

// TransactionalRepository is the explicit capability a Repository declares when it commits a
// multi-stream SerializedResult atomically: Events for several streams, per-stream Snapshots, and
// read Fences, all in one transaction (ADR 0004, C1/C2). The Repository method set did not change
// in v0.9, so a repository written earlier still compiles but may ignore Snapshots and Fences or
// write streams one by one. Code that sends such a result, such as snapshots.Store.Transaction,
// refuses a repository that does not implement this interface with *TransactionsUnsupportedError
// before dispatching anything. The in-memory, DynamoDB, and PostgreSQL repositories implement it.
//
// Implement it only when Commit honors the full contract documented on Repository.Commit.
type TransactionalRepository interface {
	Repository

	// AtomicTransactions is a marker: implementing it asserts the capability above.
	AtomicTransactions()
}

// ViewWritingRepository is the explicit capability a Repository declares when its Commit writes
// the commit-wide view operations of SerializedResult.Views atomically with the events (ADR 0004,
// C7), or refuses them with *ViewsNotConfiguredError when it has nowhere to write them. A
// repository written before v0.9 does not implement it and would silently drop the operations, so
// snapshots.Store refuses to run a commit-wide projector over one. The in-memory, DynamoDB, and
// PostgreSQL repositories implement it.
//
// Implement it only when Commit honors that contract. A decorator type hides the capability unless
// it declares it too.
type ViewWritingRepository interface {
	Repository

	// WritesViews is a marker: implementing it asserts the capability above.
	WritesViews()
}

// ViewsUnsupportedError refuses a commit-wide projector over a repository that has not declared
// evt.ViewWritingRepository: it would commit the events and silently drop the view operations.
// Nothing was written.
type ViewsUnsupportedError struct {
	// Repository is the refusing repository's Go type.
	Repository string
}

func (e *ViewsUnsupportedError) Error() string {
	return fmt.Sprintf(
		"repository %s does not implement evt.ViewWritingRepository; refusing a commit-wide projector",
		e.Repository,
	)
}

// HistoryDeclarer is the capability of a Repository that keeps the durable feature manifest and can
// add entity types to its historyDependent set: those types' commits are never compacted away,
// because a rebuild of a history-dependent projector replays them. The in-memory, DynamoDB, and
// PostgreSQL repositories implement it.
type HistoryDeclarer interface {
	// DeclareHistoryDependent durably adds the entity types to the manifest's historyDependent
	// set. A type already declared is left alone, and declaring nothing new writes nothing.
	DeclareHistoryDependent(ctx context.Context, types ...EntityType) error
}

// HistoryDeclarationUnsupportedError refuses a history-dependent projector over a repository that
// cannot declare history dependence durably. Running it anyway would let CompactBelow delete the
// commits its rebuild replays, so the commit fails before any write.
type HistoryDeclarationUnsupportedError struct {
	// Repository is the refusing repository's Go type.
	Repository string
}

func (e *HistoryDeclarationUnsupportedError) Error() string {
	return fmt.Sprintf(
		"repository %s does not implement evt.HistoryDeclarer; refusing a history-dependent projector",
		e.Repository,
	)
}

// RequireTransactions returns *TransactionsUnsupportedError unless repo implements
// TransactionalRepository.
func RequireTransactions(repo Repository) error {
	if _, ok := repo.(TransactionalRepository); ok {
		return nil
	}

	return &TransactionsUnsupportedError{Repository: fmt.Sprintf("%T", repo)}
}

// ErrTransactionsUnsupported matches a TransactionsUnsupportedError with errors.Is.
var ErrTransactionsUnsupported = errors.New("repository does not support atomic multi-stream transactions")

// TransactionsUnsupportedError refuses a multi-stream transaction, snapshot batch, or read fence
// sent to a Repository that has not declared TransactionalRepository. Nothing was written.
type TransactionsUnsupportedError struct {
	// Repository is the refused repository's Go type.
	Repository string
}

func (e *TransactionsUnsupportedError) Error() string {
	return fmt.Sprintf(
		"repository %s does not implement evt.TransactionalRepository; refusing a commit that needs atomic snapshots or fences",
		e.Repository,
	)
}

// Is makes errors.Is(err, ErrTransactionsUnsupported) match.
func (e *TransactionsUnsupportedError) Is(target error) bool {
	return target == ErrTransactionsUnsupported
}

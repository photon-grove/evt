package evt

import (
	"errors"
	"fmt"
	"strings"
)

// HeadFindingKind classifies how a stream's head item disagrees with its log (storage spec, Stream
// Heads, Event-writing paths). The head item must equal the derived head: the larger of the
// stream's highest event sequence and its snapshot's event sequence.
type HeadFindingKind string

const (
	// HeadMissing is a stream with events or a snapshot (derived head of at least 1) and no head
	// item. It is what a store written by v0.8 looks like.
	HeadMissing HeadFindingKind = "missing"

	// HeadLower is a head item below the derived head: a writer appended without maintaining it.
	HeadLower HeadFindingKind = "lower"

	// HeadHigher is a head item above the derived head: events were deleted, or the item was
	// raised by hand. Only an explicit lowering repair changes it.
	HeadHigher HeadFindingKind = "higher"

	// HeadTTLLow is a DynamoDB head item whose ttl is below an item of its stream (or that has a
	// ttl while a stream item never expires), so the head item could be deleted while the stream
	// remains. The head ttl must be at least that of every other item.
	HeadTTLLow HeadFindingKind = "ttl-low"

	// HeadOrphan is a head item with no event or snapshot left to count. A DynamoDB head item that
	// has a ttl is benign (a retained stream's head item outlives the rest until its ttl), so it
	// is a warning there; one with no ttl never expires and would stop the stream's ID from being
	// created again, so it is a finding, as is any orphan row on PostgreSQL.
	HeadOrphan HeadFindingKind = "orphan"
)

// HeadFinding is one stream whose head item disagrees with its log.
type HeadFinding struct {
	EntityID EntityID
	Kind     HeadFindingKind

	// Stored is the head item's value; zero when the stream has no head item.
	Stored EventSequence

	// Derived is the head the log derives (zero for an orphan).
	Derived EventSequence

	// StoredTTL and DerivedTTL are the DynamoDB head item's ttl and the ttl it must have; zero means
	// no ttl. They are set only for HeadTTLLow.
	StoredTTL  int64
	DerivedTTL int64
}

// String describes the finding on one line.
func (f HeadFinding) String() string {
	switch f.Kind {
	case HeadMissing:
		return fmt.Sprintf("%s: missing head item, log derives head %d", f.EntityID, f.Derived)
	case HeadLower:
		return fmt.Sprintf("%s: head %d is lower than the derived head %d", f.EntityID, f.Stored, f.Derived)
	case HeadHigher:
		return fmt.Sprintf("%s: head %d is higher than the derived head %d", f.EntityID, f.Stored, f.Derived)
	case HeadTTLLow:
		return fmt.Sprintf("%s: head item ttl %s, but its stream needs %s", f.EntityID,
			describeHeadTTL(f.StoredTTL), describeHeadTTL(f.DerivedTTL))
	case HeadOrphan:
		return fmt.Sprintf("%s: head item %d has no events or snapshot", f.EntityID, f.Stored)
	default:
		return fmt.Sprintf("%s: %s (head %d, derived %d)", f.EntityID, f.Kind, f.Stored, f.Derived)
	}
}

func describeHeadTTL(ttl int64) string {
	if ttl == 0 {
		return "none (never expires)"
	}

	return fmt.Sprintf("%d", ttl)
}

// ErrHeadsNotVerified matches a HeadsNotVerifiedError with errors.Is.
var ErrHeadsNotVerified = errors.New("stream heads do not match the log")

// HeadsNotVerifiedError reports that a verification found streams whose head item disagrees with
// the log, so the migration did not declare stream-heads (or, after the declaration, found drift).
type HeadsNotVerifiedError struct {
	// Phase names the verification that failed ("verify", "before declaring", "after declaring", or
	// "transaction" for PostgreSQL).
	Phase string

	// Findings are the failing streams. A report may list only the first of them.
	Findings []HeadFinding

	// Total is the number of failing streams, which can exceed len(Findings).
	Total int
}

// Error returns the error message string.
func (e *HeadsNotVerifiedError) Error() string {
	var b strings.Builder

	fmt.Fprintf(&b, "stream heads %s: %d stream(s) do not match the log", e.Phase, e.Total)

	for i, finding := range e.Findings {
		if i == 3 {
			fmt.Fprintf(&b, "; ...")

			break
		}

		fmt.Fprintf(&b, "; %s", finding)
	}

	return b.String()
}

// Is makes errors.Is(err, ErrHeadsNotVerified) match.
func (e *HeadsNotVerifiedError) Is(target error) bool { return target == ErrHeadsNotVerified }

// ErrHeadsAlreadyDeclared is returned by a migration of a store that already declares stream-heads:
// it is migrated, so there is nothing to backfill or declare. Use verification, or a repair if a
// stray older writer left stale heads.
var ErrHeadsAlreadyDeclared = errors.New("store already declares stream-heads")

// ErrWritersNotConfirmed is returned by a mutating migration step run without the operator's
// confirmation that every older writer is stopped. The tool cannot prove it; the enforced access
// deny of the migration procedure is the evidence.
var ErrWritersNotConfirmed = errors.New(
	"refusing to write head items without confirmation that every writer is stopped")

// ErrReservedStreams matches a ReservedStreamsError with errors.Is.
var ErrReservedStreams = errors.New("store holds streams in reserved namespaces")

// ReservedStreamsError reports streams (not engine rows) whose entity IDs fall in a reserved
// namespace (SYSTEM# or command-outcome#). Entity IDs were never restricted before v0.9, but
// head-aware code hides reserved partitions from enumeration, heads, and rebuild scans, so such a
// stream must be renamed (migrated) before the v0.9 features are enabled. The migration refuses to
// backfill or declare over one instead of silently leaving it without a head item.
type ReservedStreamsError struct {
	// IDs are the offending entity IDs, sorted. A report may list only the first of them.
	IDs []EntityID

	// Total is the number of such streams, which can exceed len(IDs).
	Total int
}

// Error returns the error message string.
func (e *ReservedStreamsError) Error() string {
	shown := e.IDs
	suffix := ""

	if len(shown) > 5 {
		shown = shown[:5]
		suffix = ", ..."
	}

	names := make([]string, 0, len(shown))
	for _, id := range shown {
		names = append(names, string(id))
	}

	return fmt.Sprintf(
		"%d stream(s) are in reserved namespaces and must be renamed before enabling stream heads (%s%s)",
		e.Total, strings.Join(names, ", "), suffix)
}

// Is makes errors.Is(err, ErrReservedStreams) match.
func (e *ReservedStreamsError) Is(target error) bool { return target == ErrReservedStreams }

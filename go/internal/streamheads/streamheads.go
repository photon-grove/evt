// Package streamheads holds the DynamoDB wire constants of the per-stream head item (ADR 0007;
// storage spec, Stream Heads) and the sizes of the actions that carry them. The commit path, the
// byte budget, and the compatibility harness share one definition, so the expressions a commit
// sends and the bytes it budgets cannot drift apart.
package streamheads

import "strconv"

// SortKey is the sort key of a stream's head item. It sits below the snapshot at 0 and the events
// from 1, so no event, snapshot, latest(n), or compaction query (each bounds sk at 0 or more)
// returns it.
const SortKey = -1

// SortKeyText is SortKey as a DynamoDB number attribute value.
const SortKeyText = "-1"

// Expressions of the head update (the first action of a written stream's group).
const (
	// UpdateExpression raises the head. The ttl-bearing form below is for entity types with
	// retention.
	UpdateExpression = "SET head = :last"

	// RetainedUpdateExpression also raises the head item's ttl (never-lower rule).
	RetainedUpdateExpression = "SET head = :last, #ttl = :t"

	// CreateCondition guards a stream's first commit (first sequence 1): no head item exists.
	CreateCondition = "attribute_not_exists(head)"

	// AppendCondition guards an append: the head is still the sequence before the commit's first.
	AppendCondition = "head = :prev"

	// RetainedAppendCondition is AppendCondition for entity types with retention: the stored ttl
	// is never lowered.
	RetainedAppendCondition = "head = :prev AND (attribute_not_exists(#ttl) OR #ttl <= :t)"

	// TTLPlaceholder and TTLName map the reserved word ttl.
	TTLPlaceholder = "#ttl"
	TTLName        = "ttl"
)

// Conditions of a read fence (one ConditionCheck on the head item).
const (
	// FenceCondition passes while no commit has written the stream since the reader saw head :head.
	FenceCondition = "head = :head"

	// FenceAbsentCondition is the fence of a stream the reader saw with no events (head 0).
	FenceAbsentCondition = "attribute_not_exists(head)"
)

// Conditions of the head guard that an out-of-band snapshot write adds to its transaction (H3).
const (
	// GuardCondition requires the stream to have reached the snapshot's event sequence :new.
	GuardCondition = "head >= :new"

	// GuardEmptyCondition is the guard for a snapshot of event sequence 0: the head item may be
	// absent.
	GuardEmptyCondition = "attribute_not_exists(head) OR head >= :new"

	// GuardRetainedCondition is GuardCondition on an Update that also refreshes the head ttl.
	GuardRetainedCondition = "head >= :new AND (attribute_not_exists(#ttl) OR #ttl <= :t)"

	// GuardRetainedUpdateExpression refreshes the head item's ttl, never lowering it.
	GuardRetainedUpdateExpression = "SET #ttl = :t"
)

// KeySize is the size of a head item's key: both attribute names plus the entity ID and the sort
// key text.
func KeySize(entityID string) int {
	return len("pk") + len("sk") + len(entityID) + len(SortKeyText)
}

func digits(n int64) int { return len(strconv.FormatInt(n, 10)) }

// UpdateSize is the budgeted size of a stream's head update: the key attributes, both expression
// strings, the name placeholder and name, and the attribute values (placeholder plus number text),
// the rule every update action follows. ttl is the head ttl for an entity type with retention and
// 0 otherwise; first and last are the commit's first and last sequence for the stream.
func UpdateSize(entityID string, first, last, ttl int64) int {
	size := KeySize(entityID) + len(":last") + digits(last)

	update, condition := UpdateExpression, CreateCondition
	if first > 1 {
		condition = AppendCondition
		size += len(":prev") + digits(first-1)
	}

	if ttl != 0 {
		update = RetainedUpdateExpression
		if first > 1 {
			condition = RetainedAppendCondition
		}

		size += len(TTLPlaceholder) + len(TTLName) + len(":t") + digits(ttl)
	}

	return size + len(update) + len(condition)
}

// FenceSize is the budgeted size of a fenced stream's head check.
func FenceSize(entityID string, head int64) int {
	if head < 1 {
		return KeySize(entityID) + len(FenceAbsentCondition)
	}

	return KeySize(entityID) + len(FenceCondition) + len(":head") + digits(head)
}

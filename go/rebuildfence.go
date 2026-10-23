package evt

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// The rebuild fence (storage spec, Rebuild Fence; ADR-0004 C10) is two engine rows in the
// reserved SYSTEM# namespace: a lock row naming the rebuild that holds the fence, and a generation
// counter that is odd while a rebuild is in progress or a force-release left it unsettled. Every
// commit is refused while the generation is unsettled (on DynamoDB by asserting the lock row is
// absent, which a force-release keeps in place); readers of derived state fence their reads
// against the generation.
const (
	// RebuildLockID is the partition key of the rebuild-lock row (sort key 0).
	RebuildLockID = SystemNamespacePrefix + "REBUILD"

	// RebuildGenerationID is the partition key of the rebuild generation row (sort key 0).
	RebuildGenerationID = SystemNamespacePrefix + "REBUILD#GENERATION"

	// MaxRebuildOwnerBytes is the longest rebuild owner token, in UTF-8 bytes.
	MaxRebuildOwnerBytes = 512

	// MaxRebuildViewBatch is the most view operations one guarded rebuild write carries: the
	// 100th action of the DynamoDB transaction is the lock-row guard.
	MaxRebuildViewBatch = 99

	// MaxRebuildBeginAttempts bounds how often Begin reads the state and decides again after the
	// transaction it built lost a race.
	MaxRebuildBeginAttempts = 8
)

// RebuildGeneration is the rebuild generation a reader observes: the monotonic counter's value (0
// while the item does not exist) and whether the last release was a force-release.
//
// The value is odd if and only if a rebuild is in progress, with one exception: a force-release
// releases the lock but leaves the value odd and sets ForceReleased, which means "derived state
// untrustworthy", not "rebuild in progress". Readers and commits treat every odd value alike: a
// reader gets *RebuildUnavailableError and a commit *RebuildInProgressError until a rebuild ends.
type RebuildGeneration struct {
	Value         int64
	ForceReleased bool
}

// Settled reports whether derived state can be trusted: the generation is even.
func (g RebuildGeneration) Settled() bool {
	return g.Value%2 == 0
}

// RebuildLock is the rebuild-lock row: the owner token of the holder, the generation Begin
// produced when it took the lock (the fencing token of the lease), and the second the lease lapses,
// in whole Unix seconds. The lease has expired when LeaseExpiresAt is before now.
type RebuildLock struct {
	Owner          string
	Generation     int64
	LeaseExpiresAt time.Time
}

// Expired reports whether the lease lapsed before now. A lease is still held at the second it
// expires.
func (l RebuildLock) Expired(now time.Time) bool {
	return l.LeaseExpiresAt.Unix() < now.Unix()
}

// RebuildState is the generation and the lock row, read in that order. The two reads are not one
// snapshot; a caller that must decide on the generation uses RebuildReader.Generation.
type RebuildState struct {
	Generation RebuildGeneration

	// Lock is the lock row, nil when no lock is held (none exists, or only the item a
	// force-release leaves on DynamoDB).
	Lock *RebuildLock
}

// InProgress reports whether a rebuild holds the fence: the lock row exists, even when its lease
// has expired.
func (s RebuildState) InProgress() bool {
	return s.Lock != nil
}

// RebuildLease is what Begin returns: the owner, the odd generation the transaction produced, and
// the second the lease lapses. The generation is the lease's fencing token: Renew, End, and
// WriteViews present the whole lease, and a lock held by anyone else, or by the same owner token
// under a later generation (another incarnation of the worker that took over), is not this lease.
type RebuildLease struct {
	Owner          string
	Generation     int64
	LeaseExpiresAt time.Time
}

// RebuildReader reads the generation, the one primitive a reader of derived state needs.
type RebuildReader interface {
	// Generation reads the generation item with a strongly consistent read, whatever the
	// repository's read-consistency setting.
	Generation(ctx context.Context) (RebuildGeneration, error)
}

// RebuildFence is the lifecycle of the durable rebuild fence over one event log. The in-memory,
// DynamoDB, and PostgreSQL repositories provide it (RebuildFenceProvider).
type RebuildFence interface {
	RebuildReader

	// State reads the generation, then the lock row, each with a consistent read.
	State(ctx context.Context) (RebuildState, error)

	// Begin acquires the lock from a quiesced or force-released state, or takes over an expired
	// lease. An unexpired lease is refused with *RebuildLockHeldError. The returned lease carries
	// the odd generation the transaction produced. Lease is rounded up to whole seconds and must
	// be at least one second.
	Begin(ctx context.Context, owner string, lease time.Duration) (RebuildLease, error)

	// Renew extends the lease to the given duration from now and returns it with the new expiry. It
	// fails with *RebuildLeaseLostError unless the lock row still names the lease's owner and
	// generation (a takeover, even by the same owner token, changes the generation); it does not
	// require the lease to be unexpired.
	Renew(ctx context.Context, lease RebuildLease, extend time.Duration) (RebuildLease, error)

	// End releases the lock and restores the settled generation, clearing the force-release flag.
	// It fails with *RebuildLeaseLostError unless the caller still holds the lease (owner and
	// generation).
	End(ctx context.Context, lease RebuildLease) (RebuildGeneration, error)

	// ForceRelease is the operator's escape hatch for a lost or wedged worker: it releases the
	// lock of the named owner, so that worker's lease is lost at once and a new rebuild can begin
	// without waiting for the lease to lapse, but leaves the generation odd and sets the
	// force-release flag. The derived state may be half-written (a failed rebuild may already have
	// wiped rows or journal counters), so readers stay fenced and every commit is still refused
	// with *RebuildInProgressError until a later rebuild begins and ends. It fails with
	// *RebuildLeaseLostError when no lock is held or the lock names another owner.
	ForceRelease(ctx context.Context, owner string) (RebuildGeneration, error)

	// WriteViews writes one batch of view operations (at most MaxRebuildViewBatch) atomically with
	// a guard that the caller still holds the lease (owner and generation) and that it has not
	// expired. A failed guard writes nothing and is *RebuildLeaseLostError: the worker lost its
	// lock, to a takeover or a force-release, and must stop.
	WriteViews(ctx context.Context, lease RebuildLease, views []ProjectedView) error
}

// RebuildFenceProvider is implemented by repositories that keep the rebuild fence: the in-memory,
// DynamoDB, and PostgreSQL repositories.
type RebuildFenceProvider interface {
	// RebuildFence returns the fence over this repository's event log.
	RebuildFence() RebuildFence
}

// ErrRebuildInProgress matches, with errors.Is, a commit refused because a rebuild holds the lock.
var ErrRebuildInProgress = errors.New("a projection rebuild is in progress")

// RebuildInProgressError is returned when a commit's rebuild-lock check fails: the generation is
// unsettled, because a rebuild holds the lock (including one whose lease expired) or a
// force-release left the rebuild unfinished. It is transient — the commit can succeed once a
// rebuild ends and settles the generation — but it is never a conflict: fence-aware retry runs
// the command again after a backoff and returns this error when the attempts run out, never a
// stale-fence rejection, and it records no outcome. Nothing was written.
type RebuildInProgressError struct{}

// Error returns the error message string.
func (*RebuildInProgressError) Error() string { return ErrRebuildInProgress.Error() }

// Is makes errors.Is(err, ErrRebuildInProgress) match.
func (*RebuildInProgressError) Is(target error) bool { return target == ErrRebuildInProgress }

// IsRebuildInProgressErr returns true when err is a RebuildInProgressError.
func IsRebuildInProgressErr(err error) bool {
	var inProgress *RebuildInProgressError
	return errors.As(err, &inProgress)
}

// RebuildLockHeldError is returned by Begin when another rebuild holds an unexpired lease.
type RebuildLockHeldError struct {
	// Owner is the holder's owner token and LeaseExpiresAt the second its lease lapses.
	Owner          string
	LeaseExpiresAt time.Time
}

// Error returns the error message string.
func (e *RebuildLockHeldError) Error() string {
	return fmt.Sprintf(
		"the rebuild lock is held by %q until %s",
		e.Owner, e.LeaseExpiresAt.UTC().Format(time.RFC3339),
	)
}

// IsRebuildLockHeldErr returns true when err is a RebuildLockHeldError.
func IsRebuildLockHeldErr(err error) bool {
	var held *RebuildLockHeldError
	return errors.As(err, &held)
}

// RebuildLeaseLostError reports that the caller no longer holds the lock it named: the lock row
// is gone, names another owner, or (for a guarded write) its lease lapsed. The worker must stop.
type RebuildLeaseLostError struct {
	Owner string
}

// Error returns the error message string.
func (e *RebuildLeaseLostError) Error() string {
	return fmt.Sprintf("rebuild owner %q does not hold the lock", e.Owner)
}

// IsRebuildLeaseLostErr returns true when err is a RebuildLeaseLostError.
func IsRebuildLeaseLostErr(err error) bool {
	var lost *RebuildLeaseLostError
	return errors.As(err, &lost)
}

// RebuildConflictError is returned by Begin when it lost its race MaxRebuildBeginAttempts times.
// It is transient.
type RebuildConflictError struct {
	Attempts int
}

// Error returns the error message string.
func (e *RebuildConflictError) Error() string {
	return fmt.Sprintf("the rebuild lock kept changing; gave up after %d attempts", e.Attempts)
}

// IsRebuildConflictErr returns true when err is a RebuildConflictError.
func IsRebuildConflictErr(err error) bool {
	var conflict *RebuildConflictError
	return errors.As(err, &conflict)
}

// RebuildStateInvalidError reports that the lock row and the generation disagree in a way the
// protocol cannot produce, because every transition writes both in one transaction. It is
// reported rather than repaired.
type RebuildStateInvalidError struct {
	Detail string
}

// Error returns the error message string.
func (e *RebuildStateInvalidError) Error() string {
	return "the rebuild lock and generation disagree: " + e.Detail
}

// IsRebuildStateInvalidErr returns true when err is a RebuildStateInvalidError.
func IsRebuildStateInvalidErr(err error) bool {
	var invalid *RebuildStateInvalidError
	return errors.As(err, &invalid)
}

// RebuildUnavailableError is returned to a reader that found an odd generation: a rebuild is in
// progress, or a force-release left derived state untrustworthy. It is transient.
type RebuildUnavailableError struct {
	Generation RebuildGeneration
}

// Error returns the error message string.
func (e *RebuildUnavailableError) Error() string {
	if e.Generation.ForceReleased {
		return fmt.Sprintf(
			"derived state is unavailable: generation %d was left by a force-released rebuild",
			e.Generation.Value,
		)
	}

	return fmt.Sprintf("derived state is unavailable: a rebuild is in progress (generation %d)", e.Generation.Value)
}

// IsRebuildUnavailableErr returns true when err is a RebuildUnavailableError.
func IsRebuildUnavailableErr(err error) bool {
	var unavailable *RebuildUnavailableError
	return errors.As(err, &unavailable)
}

// RebuildEpochChangedError is returned to a reader whose generation changed between its before and
// after reads: a rebuild started, finished, or was taken over while the read ran. It is transient.
type RebuildEpochChangedError struct {
	Observed int64
	Current  int64
}

// Error returns the error message string.
func (e *RebuildEpochChangedError) Error() string {
	return fmt.Sprintf("the rebuild generation changed from %d to %d during the read", e.Observed, e.Current)
}

// IsRebuildEpochChangedErr returns true when err is a RebuildEpochChangedError.
func IsRebuildEpochChangedErr(err error) bool {
	var changed *RebuildEpochChangedError
	return errors.As(err, &changed)
}

// IsRebuildTransient reports whether err is one of the rebuild-fence failures a caller retries
// later: a refused commit, a lost Begin race, an unavailable generation, or a changed epoch.
// policy.Classify maps them to policy.ClassTransient.
func IsRebuildTransient(err error) bool {
	return IsRebuildInProgressErr(err) ||
		IsRebuildConflictErr(err) ||
		IsRebuildUnavailableErr(err) ||
		IsRebuildEpochChangedErr(err)
}

// CheckRebuildOwner refuses an owner token that is empty, longer than MaxRebuildOwnerBytes, not
// valid UTF-8, or containing U+0000, before any request.
func CheckRebuildOwner(owner string) error {
	switch {
	case owner == "":
		return &InvalidArgumentError{Argument: "owner", Message: "a rebuild owner must not be empty"}
	case len(owner) > MaxRebuildOwnerBytes:
		return &InvalidArgumentError{
			Argument: "owner",
			Message:  fmt.Sprintf("a rebuild owner is at most %d bytes, got %d", MaxRebuildOwnerBytes, len(owner)),
		}
	case !utf8.ValidString(owner):
		return &InvalidArgumentError{Argument: "owner", Message: "a rebuild owner must be valid UTF-8"}
	case strings.ContainsRune(owner, 0):
		// PostgreSQL text cannot hold U+0000, so one backend would refuse what another stores.
		return &InvalidArgumentError{Argument: "owner", Message: "a rebuild owner must not contain U+0000"}
	}

	return nil
}

// RebuildLeaseSeconds converts a lease duration to whole seconds, rounding up. A lease below one
// second is refused with an *InvalidArgumentError.
func RebuildLeaseSeconds(lease time.Duration) (int64, error) {
	if lease < time.Second {
		return 0, &InvalidArgumentError{Argument: "lease", Message: "a rebuild lease is at least one second"}
	}

	seconds := int64(lease / time.Second)
	if lease%time.Second != 0 {
		seconds++
	}

	return seconds, nil
}

// RebuildPlanKind is what Begin does for an observed state.
type RebuildPlanKind int

const (
	// RebuildAcquire takes a fresh lock from a settled generation (absent or even).
	RebuildAcquire RebuildPlanKind = iota + 1

	// RebuildAcquireForceReleased takes a fresh lock from the odd generation a force-release left.
	RebuildAcquireForceReleased

	// RebuildTakeover replaces the owner of an expired lease.
	RebuildTakeover
)

// RebuildNoMatch is the value an acquire binds for a branch of its generation condition that does
// not apply to the observed state: no generation equals it.
const RebuildNoMatch int64 = -1

// RebuildPlan is Begin's decision, shared by every backend so they bind identical values.
type RebuildPlan struct {
	Kind RebuildPlanKind

	// Increment is what the generation grows by: 1 to acquire from a settled generation, 2 to
	// acquire from a force-released one or to take over.
	Increment int64

	// SeenEven and SeenOdd are the acquire's observed generation, RebuildNoMatch for the branch
	// that does not apply. SeenOdd is also the takeover's observed odd generation.
	SeenEven int64
	SeenOdd  int64

	// ObservedOwner is the owner of the lock row a takeover replaces.
	ObservedOwner string
}

// PlanRebuildBegin decides what Begin does for an observed state at now: acquire, acquire from a
// force-released generation, take over an expired lease, refuse an unexpired one with
// *RebuildLockHeldError, or report a state the protocol cannot produce with
// *RebuildStateInvalidError.
func PlanRebuildBegin(state RebuildState, now time.Time) (RebuildPlan, error) {
	generation := state.Generation

	if state.Lock == nil {
		switch {
		case generation.Settled():
			return RebuildPlan{
				Kind: RebuildAcquire, Increment: 1,
				SeenEven: generation.Value, SeenOdd: RebuildNoMatch,
			}, nil
		case generation.ForceReleased:
			return RebuildPlan{
				Kind: RebuildAcquireForceReleased, Increment: 2,
				SeenEven: RebuildNoMatch, SeenOdd: generation.Value,
			}, nil
		default:
			return RebuildPlan{}, &RebuildStateInvalidError{
				Detail: fmt.Sprintf("generation %d is odd with no lock row and no force-release flag", generation.Value),
			}
		}
	}

	if generation.Settled() {
		return RebuildPlan{}, &RebuildStateInvalidError{
			Detail: fmt.Sprintf("a lock row exists for %q but generation %d is even", state.Lock.Owner, generation.Value),
		}
	}

	if !state.Lock.Expired(now) {
		return RebuildPlan{}, &RebuildLockHeldError{Owner: state.Lock.Owner, LeaseExpiresAt: state.Lock.LeaseExpiresAt}
	}

	return RebuildPlan{
		Kind: RebuildTakeover, Increment: 2,
		SeenEven: RebuildNoMatch, SeenOdd: generation.Value,
		ObservedOwner: state.Lock.Owner,
	}, nil
}

// RebuildEpoch is a settled generation a read was made under (storage spec, Rebuild epochs).
type RebuildEpoch struct {
	Generation int64
}

// CheckRebuildViewBatch validates one guarded rebuild write before any request: it carries at least
// one view operation, the operations pass CheckViews, and the batch plus its guard fits the commit
// budget (MaxRebuildViewBatch operations and the byte limits of a commit). A batch over a limit is
// *CommitTooLargeError, and an empty one an *InvalidArgumentError.
func CheckRebuildViewBatch(views []ProjectedView) error {
	if len(views) == 0 {
		return &InvalidArgumentError{Argument: "views", Message: "a rebuild write carries at least one view operation"}
	}

	if err := CheckViews(views); err != nil {
		return err
	}

	// The guard is the last action; like the control checks of a commit it is sized by its keys.
	sizes := make([]int, 0, len(views)+1)
	for _, view := range views {
		sizes = append(sizes, ViewOperationSize(view.Op))
	}

	sizes = append(sizes, len("pk")+len("sk")+len(RebuildLockID)+1)

	return CommitBudget(sizes)
}

// ObserveRebuildEpoch reads the generation. An odd one — a rebuild in progress, or a force-release
// that left derived state untrustworthy — is *RebuildUnavailableError; an even one is the epoch
// the read that follows is made under.
func ObserveRebuildEpoch(ctx context.Context, reader RebuildReader) (RebuildEpoch, error) {
	generation, err := reader.Generation(ctx)
	if err != nil {
		return RebuildEpoch{}, err
	}

	if !generation.Settled() {
		return RebuildEpoch{}, &RebuildUnavailableError{Generation: generation}
	}

	return RebuildEpoch{Generation: generation.Value}, nil
}

// Verify reads the generation again. An odd one is *RebuildUnavailableError and a settled one that
// differs from the epoch is *RebuildEpochChangedError; a settled generation equal to the epoch
// means no rebuild started, finished, or was taken over while the read ran.
func (e RebuildEpoch) Verify(ctx context.Context, reader RebuildReader) error {
	generation, err := reader.Generation(ctx)
	if err != nil {
		return err
	}

	if !generation.Settled() {
		return &RebuildUnavailableError{Generation: generation}
	}

	if generation.Value != e.Generation {
		return &RebuildEpochChangedError{Observed: e.Generation, Current: generation.Value}
	}

	return nil
}

// ReadAtRebuildEpoch runs read under the rebuild epoch check: it observes the generation, runs
// read, and verifies the generation again. The read's result counts only when it returns nil. A
// failed observe skips the read, and a failed read is returned as it is without verifying.
//
// A command handler that read a derived view returns the error as its own: the engine returns it
// at once and records no outcome, so a decision made on a half-rebuilt view is never recorded.
func ReadAtRebuildEpoch(
	ctx context.Context,
	reader RebuildReader,
	read func(ctx context.Context) error,
) error {
	epoch, err := ObserveRebuildEpoch(ctx, reader)
	if err != nil {
		return err
	}

	if err = read(ctx); err != nil {
		return err
	}

	return epoch.Verify(ctx, reader)
}

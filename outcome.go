package evt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Limits the storage spec pins for command outcomes (Command Outcomes). Both are checked before
// any write, in every language and on every backend.
const (
	// MaxCommandIDBytes bounds a command ID, in UTF-8 bytes. The outcome partition key is the
	// 16-byte "command-outcome#" prefix plus the ID.
	MaxCommandIDBytes = 512

	// MaxOutcomeBytes bounds the stored response bytes and, separately, the encoded rejection.
	MaxOutcomeBytes = 65536
)

// OutcomeStatus is the final status of a recorded command.
type OutcomeStatus string

const (
	// OutcomeAccepted records a command the engine accepted. It may have written events, or none
	// (an accepted no-op).
	OutcomeAccepted OutcomeStatus = "accepted"

	// OutcomeRejected records a domain rejection. It is terminal for the command's digest.
	OutcomeRejected OutcomeStatus = "rejected"
)

// RejectionCode is a free-form, machine-readable reason for a rejection. The engine provides the
// codes below; applications add their own.
type RejectionCode string

// Engine-provided rejection codes (storage spec, Rejections).
const (
	// RejectionNotFound is the code of a NotFoundError.
	RejectionNotFound RejectionCode = "not-found"

	// RejectionBadCommand is the code of a BadCommandError.
	RejectionBadCommand RejectionCode = "bad-command"

	// RejectionStaleFence is the code the engine records when the state a command was decided on
	// went stale and the command cannot be decided again: a caller-stamped read fence failed, or
	// fence-aware retry ran out of attempts (ADR 0004, C5).
	RejectionStaleFence RejectionCode = "stale-fence"
)

// CallerFenceStaleRejection is the stale-fence rejection the engine records when a caller-stamped
// read fence went stale before the commit: the engine cannot know the caller's decision still
// holds, so it does not retry.
func CallerFenceStaleRejection() Rejection {
	return Rejection{Code: RejectionStaleFence, Message: "a caller-stamped fence went stale"}
}

// RetriesExhaustedRejection is the stale-fence rejection the engine records when fence-aware
// retry ran out of attempts: every attempt was decided on state another writer then moved. The
// attempt count is the policy's, kept in the details as an integer so both languages store the
// same bytes.
func RetriesExhaustedRejection(attempts int) Rejection {
	return Rejection{
		Code:    RejectionStaleFence,
		Message: "the state this command was decided on kept changing; retries were exhausted",
		Details: map[string]any{"attempts": attempts},
	}
}

// Rejection is the standard, persisted domain rejection (ADR 0004, C4). Its JSON encoding is the
// stored form: keys in the order code, message, details, with details null when absent.
type Rejection struct {
	Code    RejectionCode  `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details"`
}

// Rejectioner is implemented by domain errors that map onto a Rejection. An error returned by
// Entity.Handle that implements it (anywhere in its Unwrap chain) is persisted as a rejected
// outcome; any other error is an internal failure that writes no outcome.
type Rejectioner interface {
	error

	// Rejection returns the standard rejection for the error.
	Rejection() Rejection
}

// RejectionOf maps a handler error to a Rejection. It reports false for an internal failure
// (a bad event, an encoding or storage error, or any error that does not implement Rejectioner),
// which the engine returns as an error and records no outcome for.
func RejectionOf(err error) (Rejection, bool) {
	var rejectioner Rejectioner
	if errors.As(err, &rejectioner) {
		return rejectioner.Rejection(), true
	}

	return Rejection{}, false
}

// Rejection maps a NotFoundError onto the engine's not-found rejection.
func (e *NotFoundError) Rejection() Rejection {
	return Rejection{Code: RejectionNotFound, Message: e.msg}
}

// Rejection maps a BadCommandError onto the engine's bad-command rejection.
func (e *BadCommandError) Rejection() Rejection {
	return Rejection{Code: RejectionBadCommand, Message: e.Error()}
}

// ErrRejected matches a RejectionError with errors.Is.
var ErrRejected = errors.New("command rejected")

// RejectionError is what the error-returning Store.Execute returns on a store with outcomes
// enabled when the command was rejected — on first execution and on every replay. ExecuteWithOutcome
// returns the rejection as a CommandOutcome instead.
type RejectionError struct {
	Rejection Rejection

	// Err is the handler's original domain error. It is nil when the rejection was replayed from a
	// stored outcome, because only the rejection is persisted.
	Err error
}

func (e *RejectionError) Error() string {
	return fmt.Sprintf("command rejected (%s): %s", e.Rejection.Code, e.Rejection.Message)
}

// Unwrap returns the handler's original error, when there is one, so errors such as
// NotFoundError stay matchable on first execution.
func (e *RejectionError) Unwrap() error { return e.Err }

// Is makes errors.Is(err, ErrRejected) match.
func (e *RejectionError) Is(target error) bool { return target == ErrRejected }

// EncodeRejection returns the stored JSON form of a rejection.
func EncodeRejection(rejection Rejection) ([]byte, error) {
	return json.Marshal(rejection)
}

// DecodeRejection parses the stored JSON form of a rejection. Numbers inside details keep their
// stored text, so a replayed rejection re-encodes verbatim.
func DecodeRejection(data []byte) (Rejection, error) {
	var rejection Rejection

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	if err := decoder.Decode(&rejection); err != nil {
		return Rejection{}, fmt.Errorf("decode rejection: %w", err)
	}

	// The whole document must be the rejection: a second value or trailing garbage is a corrupt
	// row, as it is for Rust's parser and PostgreSQL's jsonb.
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Rejection{}, errors.New("decode rejection: unexpected data after the rejection")
	}

	return rejection, nil
}

// CanonicalRejection returns the rejection as it reads back from storage: encoded and decoded, so
// the numbers in Details are json.Number, nested values are fresh copies, and a nil or empty
// Details stays as it round-trips. A first execution returns this form so it equals a replay.
func CanonicalRejection(rejection Rejection) (Rejection, error) {
	encoded, err := EncodeRejection(rejection)
	if err != nil {
		return Rejection{}, fmt.Errorf("encode rejection: %w", err)
	}

	return DecodeRejection(encoded)
}

// Outcome is the durable record of one command: the row written once, in its final state, on the
// reserved command-outcome#<id> stream inside the commit transaction.
type Outcome struct {
	CommandID CommandID
	Status    OutcomeStatus

	// Digest is the identity digest the command was recorded under.
	Digest string

	// Response is the stored response bytes, nil when the command returned none.
	Response []byte

	// EventIDs are the events the commit wrote, in ascending order by stream ID (bytewise), then
	// sequence. Empty for a rejection and for an accepted no-op.
	EventIDs []EventID

	// Rejection is set when Status is OutcomeRejected.
	Rejection *Rejection
}

// Accepted reports whether the command was accepted.
func (o Outcome) Accepted() bool { return o.Status == OutcomeAccepted }

// Rejected reports whether the command was rejected.
func (o Outcome) Rejected() bool { return o.Status == OutcomeRejected }

// NoOp reports whether the command was accepted without writing any event.
func (o Outcome) NoOp() bool { return o.Accepted() && len(o.EventIDs) == 0 }

// CommandOutcome is what executing a command on a store with outcomes enabled returns: the
// recorded Outcome, plus what this call did.
type CommandOutcome struct {
	Outcome

	// Events are the events this call committed, in the order the commit persists them. Empty
	// when the outcome was replayed from a stored row, and for a rejection or a no-op.
	Events []SerializedEvent

	// Replayed reports that the outcome was read back from a stored row — a duplicate command
	// replay, a lost race resolved by the winner's row, or ambiguous-write recovery — rather than
	// produced by this call.
	Replayed bool
}

// EncodeEventIDs returns the stored JSON array of event IDs.
func EncodeEventIDs(ids []EventID) (string, error) {
	data, err := json.Marshal(ids)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// DecodeEventIDs parses the stored JSON array of event IDs.
func DecodeEventIDs(data string) ([]EventID, error) {
	var ids []EventID
	if err := json.Unmarshal([]byte(data), &ids); err != nil {
		return nil, fmt.Errorf("decode event IDs: %w", err)
	}

	return ids, nil
}

// CheckCommandID returns *CommandIDTooLongError for an ID over MaxCommandIDBytes and an
// *InvalidArgumentError for an empty one. Both happen before any write.
func CheckCommandID(id CommandID) error {
	if id == "" {
		return &InvalidArgumentError{Argument: "commandId", Message: "a command ID is required when command outcomes are enabled"}
	}

	if !utf8.ValidString(string(id)) {
		return &InvalidArgumentError{Argument: "commandId", Message: "a command ID must be valid UTF-8"}
	}

	// PostgreSQL text cannot hold U+0000, so it is refused on every backend alike.
	if strings.ContainsRune(string(id), 0) {
		return &InvalidArgumentError{Argument: "commandId", Message: "a command ID must not contain U+0000"}
	}

	if len(id) > MaxCommandIDBytes {
		return &CommandIDTooLongError{Length: len(id)}
	}

	return nil
}

// jsonHasNUL reports whether encoded JSON holds U+0000 in any string or object key, at any depth.
// It inspects the encoding rather than the Go values, so it is exact for every marshalable type
// (typed maps, slices, arrays, pointers, structs, and types with their own MarshalJSON).
func jsonHasNUL(encoded []byte) (bool, error) {
	var decoded any

	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return false, err
	}

	return valueHasNUL(decoded), nil
}

// valueHasNUL walks a generically decoded JSON value.
func valueHasNUL(value any) bool {
	switch v := value.(type) {
	case string:
		return strings.ContainsRune(v, 0)
	case map[string]any:
		for key, nested := range v {
			if strings.ContainsRune(key, 0) || valueHasNUL(nested) {
				return true
			}
		}
	case []any:
		for _, nested := range v {
			if valueHasNUL(nested) {
				return true
			}
		}
	}

	return false
}

// checkText refuses a caller-supplied string that cannot be stored identically on every backend:
// it must be valid UTF-8 and must not contain U+0000, which PostgreSQL text and jsonb cannot hold.
func checkText(argument, value string) error {
	if !utf8.ValidString(value) {
		return &InvalidArgumentError{Argument: argument, Message: "must be valid UTF-8"}
	}

	if strings.ContainsRune(value, 0) {
		return &InvalidArgumentError{Argument: argument, Message: "must not contain U+0000"}
	}

	return nil
}

// CheckMetadata validates every caller-supplied string of the command metadata that the engine
// persists on the command's events: the command, causation, and principal IDs, the trace carrier,
// the origin, the address, the region, the timestamp, the extension keys and values, and the commit
// ID. Each must be valid UTF-8 and free of U+0000, which PostgreSQL cannot store, so the same
// command has the same result on every backend. It runs before the outcome pre-check and the
// handler.
func CheckMetadata(metadata Metadata) error {
	optional := func(argument string, value *string) error {
		if value == nil {
			return nil
		}

		return checkText(argument, *value)
	}

	var commandID, causationID, principal *string

	if metadata.CommandID != nil {
		commandID = (*string)(metadata.CommandID)
	}

	if metadata.CausationID != nil {
		causationID = (*string)(metadata.CausationID)
	}

	if metadata.PrincipalID != nil {
		principal = (*string)(metadata.PrincipalID)
	}

	for _, field := range []struct {
		name  string
		value *string
	}{
		{"commandId", commandID},
		{"causationId", causationID},
		{"principalId", principal},
		{"address", metadata.Address},
	} {
		if err := optional(field.name, field.value); err != nil {
			return err
		}
	}

	for _, field := range []struct{ name, value string }{
		{"region", metadata.Region},
		{"timestamp", metadata.Timestamp},
		{"commitId", metadata.CommitID},
	} {
		if err := checkText(field.name, field.value); err != nil {
			return err
		}
	}

	if metadata.Origin != nil {
		if err := checkText("origin.source", string(metadata.Origin.Source)); err != nil {
			return err
		}

		if err := checkText("origin.endpoint", string(metadata.Origin.Endpoint)); err != nil {
			return err
		}
	}

	if metadata.Trace != nil {
		for key, value := range *metadata.Trace {
			if err := checkText("trace", key); err != nil {
				return err
			}

			if err := checkText("trace", value); err != nil {
				return err
			}
		}
	}

	for key, value := range metadata.Extensions {
		if err := checkText("extensions", key); err != nil {
			return err
		}

		if err := checkText("extensions", value); err != nil {
			return err
		}
	}

	return nil
}

// CheckDigest validates an identity digest — the output of the default or a custom Digester —
// before anything is read, run, or written: it must be non-empty valid UTF-8, because it is stored
// verbatim as a string, and must not contain U+0000, which PostgreSQL text cannot hold (refused on
// every backend alike). A digester that returns anything else is a programmer error.
func CheckDigest(digest string) error {
	if digest == "" {
		return &InvalidArgumentError{Argument: "digest", Message: "the digester returned an empty digest"}
	}

	if !utf8.ValidString(digest) {
		return &InvalidArgumentError{Argument: "digest", Message: "the digester returned a digest that is not valid UTF-8"}
	}

	if strings.ContainsRune(digest, 0) {
		return &InvalidArgumentError{Argument: "digest", Message: "the digester returned a digest containing U+0000"}
	}

	return nil
}

// ErrCorruptOutcome matches a CorruptOutcomeError with errors.Is.
var ErrCorruptOutcome = errors.New("corrupt command outcome")

// CorruptOutcomeError reports a stored outcome row that is not a valid outcome: an unknown status,
// a missing digest, a status that disagrees with the rejection or the event IDs, a malformed event
// ID or response. A corrupt row is never replayed as a result.
type CorruptOutcomeError struct {
	CommandID CommandID
	Reason    string
}

func (e *CorruptOutcomeError) Error() string {
	return fmt.Sprintf("stored outcome for command %q is corrupt: %s", e.CommandID, e.Reason)
}

// Is makes errors.Is(err, ErrCorruptOutcome) match.
func (e *CorruptOutcomeError) Is(target error) bool { return target == ErrCorruptOutcome }

// ValidateStoredOutcome checks an outcome decoded from storage: a known status, a digest, a
// rejection with a code exactly when the status is rejected, no event IDs on a rejection, and
// well-formed event IDs ({entityId}:{sequence}, a positive canonical sequence). It returns
// *CorruptOutcomeError otherwise. Every backend runs it on the rows it reads.
func ValidateStoredOutcome(outcome Outcome) error {
	corrupt := func(reason string) error {
		return &CorruptOutcomeError{CommandID: outcome.CommandID, Reason: reason}
	}

	if outcome.Digest == "" {
		return corrupt("the digest is missing")
	}

	switch outcome.Status {
	case OutcomeAccepted:
		if outcome.Rejection != nil {
			return corrupt("an accepted outcome carries a rejection")
		}
	case OutcomeRejected:
		if outcome.Rejection == nil || outcome.Rejection.Code == "" {
			return corrupt("a rejected outcome has no rejection code")
		}

		if len(outcome.EventIDs) > 0 {
			return corrupt("a rejected outcome lists events")
		}
	default:
		return corrupt(fmt.Sprintf("unknown status %q", outcome.Status))
	}

	for _, id := range outcome.EventIDs {
		index := strings.LastIndex(string(id), ":")
		if index <= 0 {
			return corrupt(fmt.Sprintf("event ID %q is not {entityId}:{sequence}", id))
		}

		sequence, err := strconv.ParseInt(string(id)[index+1:], 10, 64)
		if err != nil || sequence < 1 || strconv.FormatInt(sequence, 10) != string(id)[index+1:] {
			return corrupt(fmt.Sprintf("event ID %q has no positive canonical sequence", id))
		}
	}

	return nil
}

// CheckOutcome validates an outcome row before any write, in the order the spec pins: the command
// ID first, then the response bound, then the rejection bound.
func CheckOutcome(outcome *Outcome) error {
	if err := CheckCommandID(outcome.CommandID); err != nil {
		return err
	}

	if err := CheckDigest(outcome.Digest); err != nil {
		return err
	}

	switch outcome.Status {
	case OutcomeAccepted:
		if outcome.Rejection != nil {
			return &InvalidArgumentError{Argument: "rejection", Message: "an accepted outcome carries no rejection"}
		}
	case OutcomeRejected:
		if outcome.Rejection == nil || outcome.Rejection.Code == "" {
			return &InvalidArgumentError{Argument: "rejection", Message: "a rejected outcome needs a rejection with a code"}
		}
	default:
		return &InvalidArgumentError{Argument: "status", Message: fmt.Sprintf("unknown outcome status %q", outcome.Status)}
	}

	if len(outcome.Response) > MaxOutcomeBytes {
		return &OutcomeTooLargeError{What: "response", Size: len(outcome.Response)}
	}

	if outcome.Rejection != nil {
		encoded, err := EncodeRejection(*outcome.Rejection)
		if err != nil {
			return fmt.Errorf("encode rejection: %w", err)
		}

		// The code and message are stored as plain text as well as inside the JSON, where invalid
		// bytes would silently become U+FFFD: require valid UTF-8 so both copies agree.
		if err := checkText("rejection.code", string(outcome.Rejection.Code)); err != nil {
			return err
		}

		if err := checkText("rejection.message", outcome.Rejection.Message); err != nil {
			return err
		}

		// U+0000 cannot be stored by PostgreSQL (text, jsonb), so a rejection that carries it in its
		// code, message, or details (whatever Go types the details hold) is refused on every
		// backend alike, before any write. The encoding is checked, so the test is exact.
		if hasNUL, err := jsonHasNUL(encoded); err != nil {
			return fmt.Errorf("encode rejection: %w", err)
		} else if hasNUL {
			return &InvalidArgumentError{Argument: "rejection", Message: "a rejection must not contain U+0000 in its code, message, or details"}
		}

		if len(encoded) > MaxOutcomeBytes {
			return &OutcomeTooLargeError{What: "rejection", Size: len(encoded)}
		}
	}

	return nil
}

// CommandIDTooLongError refuses a command ID over MaxCommandIDBytes before any write.
type CommandIDTooLongError struct {
	// Length is the ID's length in UTF-8 bytes.
	Length int
}

func (e *CommandIDTooLongError) Error() string {
	return fmt.Sprintf("command ID is %d bytes; the limit is %d", e.Length, MaxCommandIDBytes)
}

// OutcomeTooLargeError refuses a response or rejection over MaxOutcomeBytes before any write.
type OutcomeTooLargeError struct {
	// What names the oversized value: "response" or "rejection".
	What string

	// Size is the value's size in bytes.
	Size int
}

func (e *OutcomeTooLargeError) Error() string {
	return fmt.Sprintf("outcome %s is %d bytes; the limit is %d", e.What, e.Size, MaxOutcomeBytes)
}

// ErrIdempotencyKeyReuse matches an IdempotencyKeyReuseError with errors.Is.
var ErrIdempotencyKeyReuse = errors.New("idempotency key reuse")

// IdempotencyKeyReuseError reports a command ID that already carries an outcome recorded under a
// different identity digest. It is returned as an error and never stored: the existing row belongs
// to the first command that claimed the ID.
type IdempotencyKeyReuseError struct {
	CommandID CommandID

	// Prior is the stored outcome the ID belongs to (the prior_outcome readback).
	Prior Outcome
}

func (e *IdempotencyKeyReuseError) Error() string {
	return fmt.Sprintf("command ID %q was already used by a different command", e.CommandID)
}

// Is makes errors.Is(err, ErrIdempotencyKeyReuse) match.
func (e *IdempotencyKeyReuseError) Is(target error) bool { return target == ErrIdempotencyKeyReuse }

// ErrOutcomeConflict matches an OutcomeConflictError with errors.Is.
var ErrOutcomeConflict = errors.New("command outcome already recorded")

// OutcomeConflictError reports that a commit's outcome put lost its attribute_not_exists
// condition: another writer already recorded an outcome for the command ID. It is never a
// generic optimistic-concurrency conflict — re-preparing the command cannot resolve it. The
// engine resolves it by reading the row (the winner's outcome or IdempotencyKeyReuse).
type OutcomeConflictError struct {
	CommandID CommandID
}

func (e *OutcomeConflictError) Error() string {
	return fmt.Sprintf("an outcome is already recorded for command %q", e.CommandID)
}

// Is makes errors.Is(err, ErrOutcomeConflict) match.
func (e *OutcomeConflictError) Is(target error) bool { return target == ErrOutcomeConflict }

// ErrAmbiguousWrite matches an AmbiguousWriteError with errors.Is.
var ErrAmbiguousWrite = errors.New("ambiguous write")

// AmbiguousWriteError reports a commit whose result the caller cannot know (a timeout, a lost
// connection, a cancelled context, a server fault): the transaction may or may not have applied,
// and may still land later. A backend returns it from Commit for an indeterminate failure of a
// commit that carries an outcome. The store resolves it by polling the outcome row for a bounded
// window (ambiguous-write recovery) on a context detached from the caller's cancellation; it
// returns the error itself only when the row never appeared (or could not be read). An absent row
// is never proof that the commit did not apply, so the error never says it did not.
//
// The store returns it classified transient: running the same command ID again is safe, because
// every attempt reads the outcome row first and every write is conditional, and it finds the
// outcome if the first commit lands.
type AmbiguousWriteError struct {
	CommandID CommandID

	// Cause is the indeterminate failure.
	Cause error
}

func (e *AmbiguousWriteError) Error() string {
	return fmt.Sprintf("command %q may or may not have applied: %v", e.CommandID, e.Cause)
}

// Unwrap returns the indeterminate failure.
func (e *AmbiguousWriteError) Unwrap() error { return e.Cause }

// Is makes errors.Is(err, ErrAmbiguousWrite) match.
func (e *AmbiguousWriteError) Is(target error) bool { return target == ErrAmbiguousWrite }

// ErrOutcomesUnsupported matches an OutcomesUnsupportedError with errors.Is.
var ErrOutcomesUnsupported = errors.New("repository does not support command outcomes")

// OutcomesUnsupportedError refuses a store with outcomes enabled over a repository that has not
// declared OutcomeRepository. Nothing is read or written.
type OutcomesUnsupportedError struct {
	// Repository is the refused repository's Go type.
	Repository string
}

func (e *OutcomesUnsupportedError) Error() string {
	return fmt.Sprintf("repository %s does not implement evt.OutcomeRepository; command outcomes are unavailable", e.Repository)
}

// Is makes errors.Is(err, ErrOutcomesUnsupported) match.
func (e *OutcomesUnsupportedError) Is(target error) bool { return target == ErrOutcomesUnsupported }

// OutcomeRepository is the explicit capability a Repository declares when it records command
// outcomes (ADR 0004, C3): Commit writes SerializedResult.Outcome as the outcome row inside the
// commit transaction, in its final state, conditioned on the row not existing, and GetOutcome
// reads it back. The in-memory, DynamoDB, and PostgreSQL repositories implement it.
//
// Commit must report a lost outcome condition as *OutcomeConflictError and count the outcome item
// in the commit budget. It must report a failed event put, floor or snapshot condition, fence, or
// continuity check — another writer moved a stream — as an optimistic-concurrency conflict
// (errors.Is(err, ErrOptimisticConflict), or a ConflictError), because fence-aware retry
// re-prepares the command on exactly those and persists a stale-fence rejection when it runs out
// of attempts. It MUST report a commit whose result is genuinely unknown — a timeout or
// cancelled context, a dispatch or connection failure, a truncated response, a 5xx or server
// fault, a connection dropped during COMMIT — as *AmbiguousWriteError, because only that error
// makes the store re-read the outcome row. A definitive failure (validation, a missing table,
// authorization, a constraint error other than the outcome-row conflict) MUST be returned as is:
// reporting it as ambiguous would turn a request that can never succeed into a retryable
// "did not apply". GetOutcome must validate the rows it decodes (ValidateStoredOutcome).
type OutcomeRepository interface {
	TransactionalRepository

	// GetOutcome reads the outcome row for a command ID with a strongly consistent read, or nil
	// when none exists. The read must be strongly consistent regardless of the repository's
	// read-consistency setting: an eventually consistent miss would re-execute a committed command.
	GetOutcome(ctx context.Context, id CommandID) (*Outcome, error)
}

// RequireOutcomes returns *OutcomesUnsupportedError unless repo implements OutcomeRepository.
func RequireOutcomes(repo Repository) (OutcomeRepository, error) {
	if outcomes, ok := repo.(OutcomeRepository); ok {
		return outcomes, nil
	}

	return nil, &OutcomesUnsupportedError{Repository: fmt.Sprintf("%T", repo)}
}

// OutcomeKey returns the partition key of a command's outcome row.
func OutcomeKey(id CommandID) string {
	return CommandOutcomeNamespacePrefix + string(id)
}

// digestTag pins the canonical identity encoding. A change to the encoding bumps the tag.
const digestTag = "evt-cmd-digest-v1"

// DigestInput is the identity of a command: what the caller asked for, never what the executed
// commit wrote.
type DigestInput struct {
	// CommandType is the command's type string.
	CommandType CommandType

	// Targets are the stream IDs the request itself names, known before any state is read. They
	// are deduplicated and sorted by byte order when digested. The streams a command ends up
	// reading, fencing, or writing are not part of its identity: a command may derive them from
	// the state it reads (a rename releases whichever handle the entity currently holds), so they
	// can differ between attempts of the same command.
	Targets []EntityID

	// Payload is the canonical JSON of the command payload, or nil when it has none.
	Payload []byte

	// Principal is the command metadata's principalId, empty when unset.
	Principal string
}

// Digester computes a command's identity digest before the command executes. A custom digester
// stores its own opaque string; recovery compares digests for equality only.
type Digester func(DigestInput) (string, error)

// CanonicalDigestBytes returns the canonical bytes the default digest hashes:
//
//	"evt-cmd-digest-v1" || 0x00
//	|| lp(command type)
//	|| uint32-le(target count) || lp(target stream ID) ...
//	|| lp(canonical payload JSON)
//	|| lp(principal)
func CanonicalDigestBytes(input DigestInput) []byte {
	targets := slices.Clone(input.Targets)
	slices.Sort(targets)
	targets = slices.Compact(targets)

	var buffer bytes.Buffer

	buffer.WriteString(digestTag)
	buffer.WriteByte(0)
	writeLengthPrefixed(&buffer, []byte(input.CommandType))

	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], uint32(len(targets))) //nolint:gosec // a target count cannot approach 2^32
	buffer.Write(count[:])

	for _, target := range targets {
		writeLengthPrefixed(&buffer, []byte(target))
	}

	writeLengthPrefixed(&buffer, input.Payload)
	writeLengthPrefixed(&buffer, []byte(input.Principal))

	return buffer.Bytes()
}

func writeLengthPrefixed(buffer *bytes.Buffer, value []byte) {
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], uint32(len(value))) //nolint:gosec // identity fields cannot approach 2^32 bytes
	buffer.Write(length[:])
	buffer.Write(value)
}

// DefaultDigest is the default Digester: SHA-256 over CanonicalDigestBytes, as lowercase hex.
func DefaultDigest(input DigestInput) (string, error) {
	sum := sha256.Sum256(CanonicalDigestBytes(input))

	return hex.EncodeToString(sum[:]), nil
}

// CommandIdentity describes a command to a store with outcomes enabled when there is no single
// Command value to derive it from, such as a multi-stream transaction.
//
// It is the caller's intent — the principal (from the metadata), the command type, the streams
// the request names, and the payload — and nothing derived from state. The streams a command
// reads and writes are decided per attempt, after the identity is fixed, and may change between
// attempts; they are not part of the identity, so a re-prepared command, a client retry that
// arrives after the command landed, and an ambiguous-write recovery all digest to the same value.
//
// That makes the identity a contract: everything that distinguishes the request must be in
// Targets or Payload. Two requests with the same command ID, type, targets, payload, and principal
// are the same command, so a transfer whose destination is chosen only inside the decision step
// would replay the first destination's outcome for a second destination. Put it in the payload.
type CommandIdentity struct {
	// Type is the command's type string.
	Type CommandType

	// Targets are the stream IDs the request itself names (an account, a profile), known before
	// any state is read. Streams the command derives from state it reads, such as the handle an
	// entity currently holds, are not targets. A command with no named stream, such as an
	// outcome-only command, has none.
	Targets []EntityID

	// Payload is JSON-encoded canonically into the digest. Nil means the command has no payload.
	Payload any
}

// DigestInput builds the digest input for the identity and the command metadata's principal.
func (identity CommandIdentity) DigestInput(metadata Metadata) (DigestInput, error) {
	input := DigestInput{CommandType: identity.Type, Targets: identity.Targets}

	// The identity strings are hashed as bytes, but a command's type, targets, and principal that
	// are not valid UTF-8 cannot be stored or compared identically on every backend.
	if err := checkText("commandType", string(identity.Type)); err != nil {
		return DigestInput{}, err
	}

	for _, target := range identity.Targets {
		if err := checkText("targets", string(target)); err != nil {
			return DigestInput{}, err
		}

		// A command may not name a reserved target stream: no outcome is read or recorded for a
		// command aimed at engine-managed rows, even one that later writes nothing.
		if err := CheckReservedEntityID(target); err != nil {
			return DigestInput{}, err
		}
	}

	if identity.Payload != nil {
		payload, err := json.Marshal(identity.Payload)
		if err != nil {
			return DigestInput{}, fmt.Errorf("encode command payload: %w", err)
		}

		input.Payload = payload
	}

	// Every persisted metadata string, the principal included, is validated before the pre-check
	// and the handler: the event metadata is JSON, and PostgreSQL's jsonb cannot hold U+0000.
	if err := CheckMetadata(metadata); err != nil {
		return DigestInput{}, err
	}

	if metadata.PrincipalID != nil {
		input.Principal = string(*metadata.PrincipalID)
	}

	return input, nil
}

// OutcomeAttribute is one attribute of the DynamoDB outcome item. Number marks the numeric
// attributes (sk); every other attribute is a string.
type OutcomeAttribute struct {
	Name   string
	Number bool
	Value  string
}

// OutcomeAttributes returns the attributes of the DynamoDB outcome item in the spec's order: pk,
// sk, status, digest, then response, events, rejection, and code when present. The response is
// standard padded base64; events is the JSON array of event IDs; rejection is the rejection's
// JSON. It is the single source of the item's shape, shared by the DynamoDB writer and the
// commit budget.
func OutcomeAttributes(outcome Outcome) ([]OutcomeAttribute, error) {
	attributes := []OutcomeAttribute{
		{Name: "pk", Value: OutcomeKey(outcome.CommandID)},
		{Name: "sk", Number: true, Value: "1"},
		{Name: "status", Value: string(outcome.Status)},
		{Name: "digest", Value: outcome.Digest},
	}

	if len(outcome.Response) > 0 {
		attributes = append(attributes, OutcomeAttribute{Name: "response", Value: base64.StdEncoding.EncodeToString(outcome.Response)})
	}

	if len(outcome.EventIDs) > 0 {
		events, err := EncodeEventIDs(outcome.EventIDs)
		if err != nil {
			return nil, err
		}

		attributes = append(attributes, OutcomeAttribute{Name: "events", Value: events})
	}

	if outcome.Rejection != nil {
		rejection, err := EncodeRejection(*outcome.Rejection)
		if err != nil {
			return nil, fmt.Errorf("encode rejection: %w", err)
		}

		attributes = append(attributes,
			OutcomeAttribute{Name: "rejection", Value: string(rejection)},
			OutcomeAttribute{Name: "code", Value: string(outcome.Rejection.Code)},
		)
	}

	return attributes, nil
}

// OutcomeItemSize is the DynamoDB-encoded size of the outcome item the commit writes: the sum over
// its attributes of the name's UTF-8 length plus the value's length. events supplies the written
// event IDs, which fill the events attribute exactly as NormalizeCommit derives them.
func OutcomeItemSize(outcome Outcome, events []SerializedEvent) (int, error) {
	outcome.EventIDs = outcome.EventIDs[:0:0]
	for _, event := range events {
		outcome.EventIDs = append(outcome.EventIDs, event.ID)
	}

	attributes, err := OutcomeAttributes(outcome)
	if err != nil {
		return 0, err
	}

	size := 0
	for _, attribute := range attributes {
		size += len(attribute.Name) + len(attribute.Value)
	}

	return size, nil
}

// OutcomeFromAttributes rebuilds an Outcome from the string attributes of a stored outcome item
// (status, digest, response, events, rejection). Missing attributes decode to their empty values.
// The command ID comes from the key, not the item.
// A row that is not a valid outcome (see ValidateStoredOutcome) is a *CorruptOutcomeError.
func OutcomeFromAttributes(id CommandID, status, digest, response, events, rejection string) (Outcome, error) {
	outcome := Outcome{CommandID: id, Status: OutcomeStatus(status), Digest: digest}
	corrupt := func(err error) error { return &CorruptOutcomeError{CommandID: id, Reason: err.Error()} }

	if response != "" {
		decoded, err := base64.StdEncoding.DecodeString(response)
		if err != nil {
			return Outcome{}, corrupt(fmt.Errorf("decode outcome response: %w", err))
		}

		outcome.Response = decoded
	}

	if events != "" {
		ids, err := DecodeEventIDs(events)
		if err != nil {
			return Outcome{}, corrupt(err)
		}

		outcome.EventIDs = ids
	}

	if rejection != "" {
		decoded, err := DecodeRejection([]byte(rejection))
		if err != nil {
			return Outcome{}, corrupt(err)
		}

		outcome.Rejection = &decoded
	}

	if err := ValidateStoredOutcome(outcome); err != nil {
		return Outcome{}, err
	}

	return outcome, nil
}

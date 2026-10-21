package dynamo

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/internal/streamheads"
)

// The v0.8 to stream-heads migration (storage spec, Stream Heads, Migration; ADR 0007). A table
// written by v0.8 has streams with no head items. The migration is stop-the-world: with every
// older writer stopped (and that enforced, which the tool cannot do), HeadsMigration backfills a
// head item per stream from the log, verifies every stream, and only then declares the
// stream-heads feature. It is the only code that writes head items outside a commit.

// HeadsMigrationPhase names a step of Migrate, reported to HeadsMigrationConfig.Progress when the
// step starts.
type HeadsMigrationPhase string

// The steps of Migrate, in order.
const (
	PhaseBackfill            HeadsMigrationPhase = "backfill"
	PhaseVerify              HeadsMigrationPhase = "verify"
	PhaseVerifyBeforeDeclare HeadsMigrationPhase = "verify-before-declare"
	PhaseDeclare             HeadsMigrationPhase = "declare"
	PhaseVerifyAfterDeclare  HeadsMigrationPhase = "verify-after-declare"
)

// defaultHeadsConcurrency bounds the concurrent head writes of a backfill or repair.
const defaultHeadsConcurrency = 8

// HeadsMigrationConfig tunes a HeadsMigration. The zero value is usable.
type HeadsMigrationConfig struct {
	// PageSize is the Scan page size in items; zero lets DynamoDB fill a 1 MB page. Streams are
	// accumulated across pages, so any size gives the same result.
	PageSize int32

	// Concurrency bounds concurrent head writes; zero means 8.
	Concurrency int

	// Now is the clock that decides which items have expired (ttl in the past); nil means
	// time.Now. Each scan reads it once.
	Now func() time.Time

	// Progress, when set, is called as each phase of Migrate starts.
	Progress func(HeadsMigrationPhase)
}

// HeadsMigration migrates one event-log table to stream heads. Construct it with
// Repository.HeadsMigration.
type HeadsMigration struct {
	repo *Repository
	cfg  HeadsMigrationConfig
}

// HeadsMigration returns the migration of this repository's events table.
func (repo *Repository) HeadsMigration(cfg HeadsMigrationConfig) *HeadsMigration {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = defaultHeadsConcurrency
	}

	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	return &HeadsMigration{repo: repo, cfg: cfg}
}

// HeadsVerifyReport is the result of comparing every stream's head item with its log.
type HeadsVerifyReport struct {
	// Streams is the number of non-reserved partitions scanned, expired ones included.
	Streams int

	// Findings are the streams that fail verification: a missing, lower, or higher head, or a head
	// item whose ttl is below its stream's.
	Findings []evt.HeadFinding

	// ReservedStreams are streams (not engine rows) in a reserved namespace. Head-aware code hides
	// them, so they must be renamed first: Migrate refuses to continue while any exists.
	ReservedStreams []evt.EntityID

	// Warnings do not fail verification: a head item with a ttl and nothing left to count, which a
	// retained stream leaves behind until its ttl passes.
	Warnings []evt.HeadFinding
}

// OK reports whether every stream passed.
func (r HeadsVerifyReport) OK() bool { return len(r.Findings) == 0 }

// HeadsBackfillReport counts what a backfill did, partition by partition.
type HeadsBackfillReport struct {
	// DryRun is true when nothing was written; Written then counts the head items that would be.
	DryRun bool

	// Streams is the number of non-reserved partitions scanned.
	Streams int

	// Empty partitions had nothing left that has not expired, and need no head item.
	Empty int

	// ZeroHead partitions hold only a snapshot at event sequence 0 (head items start at 1).
	ZeroHead int

	// Present partitions already had a head item, which is never overwritten here.
	Present int

	// Written head items were created.
	Written int

	// Raced head items appeared between the scan and the write; verification judges them.
	Raced int

	// MixedTTL counts written head items with no ttl because a stream item never expires while
	// another has a ttl: a head item with the larger ttl would be deleted under the stream.
	MixedTTL int
}

// HeadsRepairReport counts what a repair did.
type HeadsRepairReport struct {
	DryRun bool

	// Repaired streams had their head item created, raised, lowered, or its ttl fixed.
	Repaired []evt.HeadFinding

	// Raced head items changed between the scan and the write and were left for the next run.
	Raced int

	// Remaining are the findings a verification still reports after a repair in which some write
	// raced. It is empty when no write raced, and when the re-verification came back clean.
	Remaining []evt.HeadFinding

	// Left are findings a repair does not touch: a higher head without AllowLower, and a head item
	// with no derived head to lower to.
	Left []evt.HeadFinding
}

// HeadsMigrationReport is the result of Migrate.
type HeadsMigrationReport struct {
	Backfill HeadsBackfillReport

	// Verify is the last verification that ran: the one after the declaration when the migration
	// got that far.
	Verify HeadsVerifyReport

	// Declared reports that this run declared stream-heads.
	Declared bool

	// Manifest is the manifest after the declaration; its zero value when none was made.
	Manifest evt.FeatureManifest
}

// HeadsMigrateOptions are the operator's choices for Migrate.
type HeadsMigrateOptions struct {
	// ConfirmWritersStopped states that the operator stopped and fenced every older writer
	// (migration step 1). It is required unless DryRun is set.
	ConfirmWritersStopped bool

	// DryRun scans and counts without writing a head item or the marker.
	DryRun bool
}

// HeadsRepairOptions are the operator's choices for Repair.
type HeadsRepairOptions struct {
	// ConfirmWritersStopped is required unless DryRun is set.
	ConfirmWritersStopped bool

	// AllowLower also lowers a head item above the derived head. Lowering breaks head
	// monotonicity (H1), so every writer must be stopped: an in-flight fence reads the head.
	AllowLower bool

	DryRun bool
}

// modeledFeatures is the feature set the migration understands: a store that declares anything
// else (the Rust-first journal extensions keep rows it would misread) is refused.
func modeledFeatures() evt.SupportedFeatures { return evt.SupportedFeatures(nil).WithStreamHeads() }

// CheckPreconditions refuses a table whose marker declares a feature the migration does not model
// (journal in particular), with *evt.UnsupportedFeatureError. It returns the manifest, nil when
// the table declares nothing. Every operation below runs it first.
func (m *HeadsMigration) CheckPreconditions(ctx context.Context) (*evt.FeatureManifest, error) {
	manifest, err := m.repo.FeatureMarker(nil).Manifest(ctx)
	if err != nil {
		return nil, err
	}

	if manifest != nil {
		if err := manifest.Validate(modeledFeatures()); err != nil {
			return nil, err
		}
	}

	return manifest, nil
}

// Verify rescans the table and reports every stream whose head item is missing, lower, or higher
// than the derived head, or whose ttl is below its stream's. It is read-only.
func (m *HeadsMigration) Verify(ctx context.Context) (HeadsVerifyReport, error) {
	if _, err := m.CheckPreconditions(ctx); err != nil {
		return HeadsVerifyReport{}, err
	}

	return m.verify(ctx)
}

func (m *HeadsMigration) verify(ctx context.Context) (HeadsVerifyReport, error) {
	scanned, err := m.scan(ctx)
	if err != nil {
		return HeadsVerifyReport{}, err
	}

	tallies := scanned.tallies
	report := HeadsVerifyReport{Streams: len(tallies), ReservedStreams: scanned.reserved}

	for _, id := range sortedStreamIDs(tallies) {
		finding, warning := tallies[id].classify(id)
		if finding != nil {
			report.Findings = append(report.Findings, *finding)
		}

		if warning != nil {
			report.Warnings = append(report.Warnings, *warning)
		}
	}

	return report, nil
}

// Migrate runs the migration: backfill, verify, verify again immediately before declaring,
// declare stream-heads, and verify once more. Any step that finds a stream whose head does not
// match its log stops the run with *evt.HeadsNotVerifiedError, so a store with a headless stream
// is never declared. It is idempotent and resumable: a rerun skips the head items already written.
// A table that already declares stream-heads returns evt.ErrHeadsAlreadyDeclared.
//
// The post-declaration verification can fail after the declaration was made (a stray older writer
// ran); the report then has Declared set and the error names the drift to repair.
func (m *HeadsMigration) Migrate(ctx context.Context, opts HeadsMigrateOptions) (HeadsMigrationReport, error) {
	var report HeadsMigrationReport

	if !opts.DryRun && !opts.ConfirmWritersStopped {
		return report, evt.ErrWritersNotConfirmed
	}

	manifest, err := m.CheckPreconditions(ctx)
	if err != nil {
		return report, err
	}

	if manifest != nil && manifest.DeclaresStreamHeads() {
		return report, evt.ErrHeadsAlreadyDeclared
	}

	m.progress(PhaseBackfill)

	report.Backfill, err = m.backfill(ctx, opts.DryRun)
	if err != nil {
		return report, err
	}

	if opts.DryRun {
		return report, nil
	}

	for _, phase := range []HeadsMigrationPhase{PhaseVerify, PhaseVerifyBeforeDeclare} {
		m.progress(phase)

		report.Verify, err = m.verify(ctx)
		if err != nil {
			return report, err
		}

		if len(report.Verify.ReservedStreams) > 0 {
			return report, reservedStreams(report.Verify.ReservedStreams)
		}

		if !report.Verify.OK() {
			return report, notVerified(string(phase), report.Verify)
		}
	}

	m.progress(PhaseDeclare)

	// A feature declared since the first check (a writer of another language) is a reason to stop
	// as much as one declared before it.
	if _, err = m.CheckPreconditions(ctx); err != nil {
		return report, err
	}

	report.Manifest, err = m.repo.FeatureMarker(nil).DeclareStreamHeads(ctx)
	if err != nil {
		return report, err
	}

	report.Declared = true

	m.progress(PhaseVerifyAfterDeclare)

	report.Verify, err = m.verify(ctx)
	if err != nil {
		return report, err
	}

	if len(report.Verify.ReservedStreams) > 0 {
		return report, reservedStreams(report.Verify.ReservedStreams)
	}

	if !report.Verify.OK() {
		return report, notVerified(string(PhaseVerifyAfterDeclare), report.Verify)
	}

	return report, nil
}

func reservedStreams(ids []evt.EntityID) error {
	return &evt.ReservedStreamsError{IDs: ids[:min(len(ids), 100)], Total: len(ids)}
}

func (m *HeadsMigration) progress(phase HeadsMigrationPhase) {
	if m.cfg.Progress != nil {
		m.cfg.Progress(phase)
	}
}

func notVerified(phase string, report HeadsVerifyReport) error {
	shown := report.Findings[:min(len(report.Findings), 10)]

	return &evt.HeadsNotVerifiedError{Phase: phase, Findings: shown, Total: len(report.Findings)}
}

// backfill writes a head item for every stream that has none: the derived head, and for a stream
// whose items all carry a ttl, the largest of them. It scans the whole table first and writes
// after, because a partition split by a page boundary is complete only when the scan ends.
func (m *HeadsMigration) backfill(ctx context.Context, dryRun bool) (HeadsBackfillReport, error) {
	scanned, err := m.scan(ctx)
	if err != nil {
		return HeadsBackfillReport{}, err
	}

	// A stream in a reserved namespace is invisible to head-aware code and must be renamed, not
	// left without a head item under a declaration. Nothing has been written yet.
	if len(scanned.reserved) > 0 {
		return HeadsBackfillReport{}, reservedStreams(scanned.reserved)
	}

	tallies := scanned.tallies
	report := HeadsBackfillReport{DryRun: dryRun, Streams: len(tallies)}

	var writes []headWrite

	for _, id := range sortedStreamIDs(tallies) {
		tally := tallies[id]

		switch {
		case tally.live == 0:
			report.Empty++
		case tally.derived() == 0:
			report.ZeroHead++
		case tally.headPresent:
			report.Present++
		default:
			writes = append(writes, tally.createHead(id))

			if tally.mixedTTL() {
				report.MixedTTL++
			}
		}
	}

	raced, err := m.write(ctx, writes, dryRun)
	if err != nil {
		return report, err
	}

	report.Raced = raced
	report.Written = len(writes) - raced

	return report, nil
}

// Repair raises a head item below the derived head, creates a missing one, and fixes a ttl below
// its stream's. With AllowLower it also lowers a head item above the derived head, writing
// SET head = :derived conditioned on head = :observed. Run it with every writer stopped. It
// does not declare the feature and does not touch a head item it cannot explain (one with no
// events or snapshot left and no ttl, or a higher one without AllowLower): those are reported in
// Left.
func (m *HeadsMigration) Repair(ctx context.Context, opts HeadsRepairOptions) (HeadsRepairReport, error) {
	report := HeadsRepairReport{DryRun: opts.DryRun}

	if !opts.DryRun && !opts.ConfirmWritersStopped {
		return report, evt.ErrWritersNotConfirmed
	}

	if _, err := m.CheckPreconditions(ctx); err != nil {
		return report, err
	}

	scanned, err := m.scan(ctx)
	if err != nil {
		return report, err
	}

	tallies := scanned.tallies

	var (
		writes   []headWrite
		repaired []evt.HeadFinding
	)

	for _, id := range sortedStreamIDs(tallies) {
		tally := tallies[id]

		finding, _ := tally.classify(id)
		if finding == nil {
			continue
		}

		if finding.Kind == evt.HeadOrphan || (finding.Kind == evt.HeadHigher && (!opts.AllowLower || tally.derived() == 0)) {
			report.Left = append(report.Left, *finding)

			continue
		}

		writes = append(writes, tally.repairHead(id))
		repaired = append(repaired, *finding)
	}

	raced, err := m.write(ctx, writes, opts.DryRun)
	if err != nil {
		return report, err
	}

	report.Raced = raced
	report.Repaired = repaired

	// A write that lost its condition left its stream unrepaired. Look again, so the caller can tell
	// a stream someone else fixed in the meantime from one that still disagrees with its log.
	if raced > 0 {
		verified, verr := m.verify(ctx)
		if verr != nil {
			return report, verr
		}

		report.Remaining = verified.Findings
	}

	return report, nil
}

// headWrite is one conditional write of a head item.
type headWrite struct {
	id    evt.EntityID
	input dynamodb.UpdateItemInput
}

// write performs the head writes with bounded concurrency and returns how many lost their
// condition: another writer changed the head item since the scan, which verification then judges.
// Any other failure stops the run; the writes are idempotent, so a rerun resumes.
func (m *HeadsMigration) write(ctx context.Context, writes []headWrite, dryRun bool) (int, error) {
	if dryRun || len(writes) == 0 {
		return 0, nil
	}

	var (
		mu     sync.Mutex
		raced  int
		failed error
		wg     sync.WaitGroup
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	queue := make(chan headWrite)

	for range min(m.cfg.Concurrency, len(writes)) {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for write := range queue {
				write.input.TableName = &m.repo.EventsTable

				_, err := m.repo.client.UpdateItem(ctx, &write.input)

				var lost *types.ConditionalCheckFailedException

				mu.Lock()

				switch {
				case err == nil:
				case errors.As(err, &lost):
					raced++
				case failed == nil:
					failed = fmt.Errorf("dynamo: writing the head item of %s: %w", write.id, err)

					cancel()
				}

				mu.Unlock()
			}
		}()
	}

feed:
	for _, write := range writes {
		select {
		case queue <- write:
		case <-ctx.Done():
			break feed
		}
	}

	close(queue)
	wg.Wait()

	if failed != nil {
		return raced, failed
	}

	return raced, ctx.Err()
}

// streamTally is what one scan learns about a partition. Items whose ttl has passed but which are
// not yet deleted are counted nowhere but expired.
type streamTally struct {
	// live counts non-head items (events and the snapshot) that have not expired.
	live int

	// events is the highest live event sort key; snapshot is the live snapshot's event sequence.
	events   evt.EventSequence
	snapshot evt.EventSequence

	// maxTTL is the largest ttl of the live items that have one; noTTL is set when a live item has
	// none, so it never expires.
	maxTTL int64
	noTTL  bool

	headPresent bool
	head        evt.EventSequence
	headTTL     int64
}

// derived is the stream's derived head (invariant H4): the larger of the highest event sort key
// and the snapshot's event sequence.
func (t *streamTally) derived() evt.EventSequence { return max(t.events, t.snapshot) }

// wantTTL is the ttl a head item needs: the largest ttl across the live items when all of them
// have one, and none otherwise. A head item that expires first would leave a stream without one.
func (t *streamTally) wantTTL() int64 {
	if t.noTTL {
		return 0
	}

	return t.maxTTL
}

// mixedTTL reports a stream with items that expire and items that never do.
func (t *streamTally) mixedTTL() bool { return t.noTTL && t.maxTTL != 0 }

// add folds one scanned item into the tally. now is the epoch second that decides expiry.
func (t *streamTally) add(sk int, item map[string]types.AttributeValue, now int64) {
	ttl, hasTTL := int64Attr(item, "ttl")

	if sk == -1 {
		// An item at sk = -1 with no head attribute is not a head item (a commit's create
		// condition, attribute_not_exists(head), treats it as absent too), so it is left to be
		// filled in rather than judged.
		if head, ok := int64Attr(item, "head"); ok {
			t.headPresent = true
			t.head = evt.EventSequence(head)
			t.headTTL = ttl
		}

		return
	}

	if sk < 0 || (hasTTL && ttl < now) {
		return
	}

	t.live++

	if hasTTL {
		t.maxTTL = max(t.maxTTL, ttl)
	} else {
		t.noTTL = true
	}

	if sk > 0 {
		t.events = max(t.events, evt.EventSequence(sk))

		return
	}

	if eventSeq, ok := int64Attr(item, "eventSeq"); ok {
		t.snapshot = max(t.snapshot, evt.EventSequence(eventSeq))
	}
}

// classify compares the head item with the log. A finding fails verification; a warning does not.
func (t *streamTally) classify(id evt.EntityID) (finding, warning *evt.HeadFinding) {
	if t.live == 0 {
		if !t.headPresent {
			return nil, nil
		}

		orphan := &evt.HeadFinding{EntityID: id, Kind: evt.HeadOrphan, Stored: t.head, StoredTTL: t.headTTL}

		// A head item with a ttl goes when the ttl does. One without never will, and a create
		// of the stream's ID would fail against it.
		if t.headTTL == 0 {
			return orphan, nil
		}

		return nil, orphan
	}

	derived := t.derived()

	switch {
	case t.headPresent && derived == 0:
		return &evt.HeadFinding{EntityID: id, Kind: evt.HeadHigher, Stored: t.head}, nil
	case derived == 0:
		return nil, nil
	case !t.headPresent:
		return &evt.HeadFinding{EntityID: id, Kind: evt.HeadMissing, Derived: derived}, nil
	case t.head < derived:
		return &evt.HeadFinding{EntityID: id, Kind: evt.HeadLower, Stored: t.head, Derived: derived}, nil
	case t.head > derived:
		return &evt.HeadFinding{EntityID: id, Kind: evt.HeadHigher, Stored: t.head, Derived: derived}, nil
	case t.ttlLow():
		return &evt.HeadFinding{
			EntityID: id, Kind: evt.HeadTTLLow, Stored: t.head, Derived: derived,
			StoredTTL: t.headTTL, DerivedTTL: t.wantTTL(),
		}, nil
	}

	return nil, nil
}

// ttlLow reports a head item that could be deleted while its stream remains: it has a ttl and a
// live item outlives it, or does not expire at all. A head item with no ttl never expires and is
// always safe.
func (t *streamTally) ttlLow() bool {
	return t.headTTL != 0 && (t.noTTL || t.headTTL < t.maxTTL)
}

// createHead is the backfill write of a stream that has no head item.
func (t *streamTally) createHead(id evt.EntityID) headWrite {
	return t.headUpdate(id, t.derived(), t.wantTTL(), false, streamheads.CreateCondition, nil)
}

// repairHead is the repair write for a stream with a finding: it sets the head to the derived
// head, and raises (or removes) a ttl that is too low.
func (t *streamTally) repairHead(id evt.EntityID) headWrite {
	if !t.headPresent {
		return t.createHead(id)
	}

	derived := t.derived()
	if derived == 0 {
		derived = t.head
	}

	var (
		ttl       int64
		removeTTL bool
	)

	switch {
	case !t.ttlLow():
	case t.wantTTL() == 0:
		removeTTL = true
	default:
		ttl = t.wantTTL()
	}

	write := t.headUpdate(id, derived, ttl, removeTTL, "head = :observed", &t.head)

	// A write that changes the ttl is also conditioned on the ttl it saw. Otherwise a head-aware
	// writer that refreshed the ttl without moving the head (an out-of-band snapshot write does)
	// between the scan and this write could have a larger ttl overwritten by a smaller one, which
	// breaks the never-lower rule.
	// (Only a head item that has a ttl is ever changed: ttlLow requires one.)
	if ttl != 0 || removeTTL {
		condition := *write.input.ConditionExpression + " AND " + streamheads.TTLPlaceholder + " = :observedTtl"
		write.input.ExpressionAttributeValues[":observedTtl"] = numberValue(t.headTTL)
		write.input.ConditionExpression = &condition
		write.input.ExpressionAttributeNames = map[string]string{streamheads.TTLPlaceholder: streamheads.TTLName}
	}

	return write
}

// headUpdate builds the UpdateItem of a head item. ttl is set when non-zero and removed when
// removeTTL is; observed, when set, is bound to :observed for a condition that uses it.
func (t *streamTally) headUpdate(
	id evt.EntityID,
	head evt.EventSequence,
	ttl int64,
	removeTTL bool,
	condition string,
	observed *evt.EventSequence,
) headWrite {
	expression := streamheads.UpdateExpression
	names := map[string]string(nil)
	values := map[string]types.AttributeValue{":last": numberValue(int64(head))}

	if ttl != 0 {
		expression = streamheads.RetainedUpdateExpression
		names = map[string]string{streamheads.TTLPlaceholder: streamheads.TTLName}
		values[":t"] = numberValue(ttl)
	}

	if removeTTL {
		expression += " REMOVE " + streamheads.TTLPlaceholder
		names = map[string]string{streamheads.TTLPlaceholder: streamheads.TTLName}
	}

	if observed != nil {
		values[":observed"] = numberValue(int64(*observed))
	}

	return headWrite{id: id, input: dynamodb.UpdateItemInput{
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(id)},
			"sk": &types.AttributeValueMemberN{Value: streamheads.SortKeyText},
		},
		UpdateExpression:          aws.String(expression),
		ConditionExpression:       aws.String(condition),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	}}
}

// tableScan is what one pass over the table learns: a tally per non-reserved partition, and the
// streams that sit in a reserved namespace without being engine rows.
type tableScan struct {
	tallies  map[evt.EntityID]*streamTally
	reserved []evt.EntityID
}

// scan reads the whole table with strongly consistent reads and folds it into one tally per
// non-reserved partition. Items of a partition may be split across pages (or interleaved with
// other partitions), so nothing is decided until the scan ends. Reserved namespaces hold engine
// rows; a row there that is not one is a pre-v0.9 stream the migration cannot leave behind, so the
// scan collects those too (the same rule as evt-doctor's reserved-namespace check).
func (m *HeadsMigration) scan(ctx context.Context) (tableScan, error) {
	// version, owner, leaseExpiresAt, value, and status are DynamoDB reserved words, and they are the
	// attributes that tell an engine row from a user stream, with forceReleased, which marks the
	// lock item a force-release leaves in place of the lock.
	input := dynamodb.ScanInput{
		TableName:            &m.repo.EventsTable,
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("pk, sk, eventSeq, head, #ttl, #ver, #own, #lease, #val, #status, forceReleased"),
		ExpressionAttributeNames: map[string]string{
			streamheads.TTLPlaceholder: streamheads.TTLName,
			"#ver":                     "version",
			"#own":                     "owner",
			"#lease":                   "leaseExpiresAt",
			"#val":                     "value",
			"#status":                  "status",
		},
	}

	if m.cfg.PageSize > 0 {
		input.Limit = aws.Int32(m.cfg.PageSize)
	}

	now := m.cfg.Now().Unix()
	scanned := tableScan{tallies: make(map[evt.EntityID]*streamTally)}
	reserved := make(map[evt.EntityID]struct{})

	pages := dynamodb.NewScanPaginator(m.repo.client, &input)
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return tableScan{}, fmt.Errorf("dynamo: scanning %s for stream heads: %w", m.repo.EventsTable, err)
		}

		for _, item := range page.Items {
			pk, ok := item["pk"].(*types.AttributeValueMemberS)
			if !ok {
				continue
			}

			id := evt.EntityID(pk.Value)

			if evt.IsReservedEntityID(id) {
				if ttl, hasTTL := int64Attr(item, "ttl"); !isEngineRow(id, item) && (!hasTTL || ttl >= now) {
					reserved[id] = struct{}{}
				}

				continue
			}

			sk, ok := numAttr(item, "sk")
			if !ok {
				continue
			}

			tally := scanned.tallies[id]
			if tally == nil {
				tally = &streamTally{}
				scanned.tallies[id] = tally
			}

			tally.add(sk, item, now)
		}
	}

	scanned.reserved = make([]evt.EntityID, 0, len(reserved))
	for id := range reserved {
		scanned.reserved = append(scanned.reserved, id)
	}

	slices.Sort(scanned.reserved)

	return scanned, nil
}

// isEngineRow reports whether a reserved-namespace row was written by the engine: the feature
// marker, the rebuild lock and generation, and command outcomes have stable shapes a user stream
// cannot accidentally mimic at the same key.
func isEngineRow(id evt.EntityID, item map[string]types.AttributeValue) bool {
	sk, ok := int64Attr(item, "sk")
	if !ok {
		return false
	}

	has := func(name string) bool { _, present := item[name]; return present }

	switch {
	case string(id) == evt.SystemNamespacePrefix+"FEATURES":
		return sk == 0 && has("version")
	case string(id) == evt.SystemNamespacePrefix+"REBUILD":
		// A held lock, or the item a force-release leaves in its place.
		return sk == 0 && (has("owner") && has("leaseExpiresAt") || has("forceReleased"))
	case string(id) == evt.SystemNamespacePrefix+"REBUILD#GENERATION":
		return sk == 0 && has("value")
	case strings.HasPrefix(string(id), evt.CommandOutcomeNamespacePrefix):
		return sk == 1 && has("status")
	}

	return false
}

func sortedStreamIDs(tallies map[evt.EntityID]*streamTally) []evt.EntityID {
	ids := make([]evt.EntityID, 0, len(tallies))
	for id := range tallies {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	return ids
}

// int64Attr reads a DynamoDB Number attribute as an integer, false when absent or not an integer.
func int64Attr(item map[string]types.AttributeValue, name string) (int64, bool) {
	attr, ok := item[name].(*types.AttributeValueMemberN)
	if !ok {
		return 0, false
	}

	n, err := strconv.ParseInt(attr.Value, 10, 64)
	if err != nil {
		return 0, false
	}

	return n, true
}

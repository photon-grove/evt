package dynamo

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
)

const tallyNow = 1_000

func n(value int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)}
}

func tallyItem(id string, sk int, attrs map[string]int64) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: id},
		"sk": n(int64(sk)),
	}

	for name, value := range attrs {
		item[name] = n(value)
	}

	return item
}

func tallyOf(t *testing.T, items ...map[string]types.AttributeValue) *streamTally {
	t.Helper()

	tally := &streamTally{}

	for _, item := range items {
		sk, ok := numAttr(item, "sk")
		require.True(t, ok)

		tally.add(sk, item, tallyNow)
	}

	return tally
}

func TestStreamTallyDerivesTheHeadAndItsTTL(t *testing.T) {
	event := func(sk int, ttl int64) map[string]types.AttributeValue {
		attrs := map[string]int64{}
		if ttl != 0 {
			attrs["ttl"] = ttl
		}

		return tallyItem("s", sk, attrs)
	}

	snapshot := func(eventSeq, ttl int64) map[string]types.AttributeValue {
		attrs := map[string]int64{"eventSeq": eventSeq}
		if ttl != 0 {
			attrs["ttl"] = ttl
		}

		return tallyItem("s", 0, attrs)
	}

	head := func(value, ttl int64) map[string]types.AttributeValue {
		attrs := map[string]int64{"head": value}
		if ttl != 0 {
			attrs["ttl"] = ttl
		}

		return tallyItem("s", -1, attrs)
	}

	tests := map[string]struct {
		items       []map[string]types.AttributeValue
		derived     evt.EventSequence
		wantTTL     int64
		live        int
		kind        evt.HeadFindingKind // "" means no finding
		warningKind evt.HeadFindingKind
	}{
		"events only": {
			items: []map[string]types.AttributeValue{event(1, 0), event(2, 0), event(3, 0)}, derived: 3, live: 3,
			kind: evt.HeadMissing,
		},
		"a snapshot ahead of the events": {
			items: []map[string]types.AttributeValue{snapshot(9, 0), event(7, 0), event(8, 0)}, derived: 9, live: 3,
			kind: evt.HeadMissing,
		},
		"events ahead of the snapshot": {
			items: []map[string]types.AttributeValue{snapshot(4, 0), event(5, 0), event(6, 0)}, derived: 6, live: 3,
			kind: evt.HeadMissing,
		},
		"a snapshot of nothing needs no head": {
			items: []map[string]types.AttributeValue{snapshot(0, 0)}, derived: 0, live: 1,
		},
		"a snapshot of nothing with a head is too high": {
			items: []map[string]types.AttributeValue{snapshot(0, 0), head(2, 0)}, derived: 0, live: 1,
			kind: evt.HeadHigher,
		},
		"the head ttl is the maximum": {
			items: []map[string]types.AttributeValue{event(1, 1100), event(2, 1300), snapshot(1, 1200)}, derived: 2,
			wantTTL: 1300, live: 3, kind: evt.HeadMissing,
		},
		"expired items are ignored": {
			items: []map[string]types.AttributeValue{event(1, 900), event(2, 999), event(3, 1500)}, derived: 3,
			wantTTL: 1500, live: 1, kind: evt.HeadMissing,
		},
		"an item expiring exactly now is not expired": {
			items: []map[string]types.AttributeValue{event(1, tallyNow)}, derived: 1, wantTTL: tallyNow, live: 1,
			kind: evt.HeadMissing,
		},
		"nothing left": {
			items: []map[string]types.AttributeValue{event(1, 10), event(2, 20)}, derived: 0, live: 0,
		},
		"an expired stream with a head that will expire is an orphan warning": {
			items: []map[string]types.AttributeValue{event(1, 10), head(1, 2000)}, derived: 0, live: 0,
			warningKind: evt.HeadOrphan,
		},
		"an orphan head that never expires is a finding": {
			items: []map[string]types.AttributeValue{head(1, 0)}, derived: 0, live: 0,
			kind: evt.HeadOrphan,
		},
		"an item that never expires keeps the head from expiring": {
			items: []map[string]types.AttributeValue{event(1, 1100), event(2, 0)}, derived: 2, wantTTL: 0, live: 2,
			kind: evt.HeadMissing,
		},
		"an item at sk -1 without a head attribute is not a head item": {
			items:   []map[string]types.AttributeValue{event(1, 0), tallyItem("s", -1, map[string]int64{"ttl": 5000})},
			derived: 1, live: 1, kind: evt.HeadMissing,
		},
		"a matching head": {
			items: []map[string]types.AttributeValue{event(1, 0), event(2, 0), head(2, 0)}, derived: 2, live: 2,
		},
		"a lower head": {
			items: []map[string]types.AttributeValue{event(1, 0), event(2, 0), head(1, 0)}, derived: 2, live: 2,
			kind: evt.HeadLower,
		},
		"a higher head": {
			items: []map[string]types.AttributeValue{event(1, 0), event(2, 0), head(3, 0)}, derived: 2, live: 2,
			kind: evt.HeadHigher,
		},
		"a head ttl below an item's": {
			items:   []map[string]types.AttributeValue{event(1, 1100), event(2, 1300), head(2, 1200)},
			derived: 2, wantTTL: 1300, live: 2, kind: evt.HeadTTLLow,
		},
		"a head ttl equal to the maximum": {
			items:   []map[string]types.AttributeValue{event(1, 1100), event(2, 1300), head(2, 1300)},
			derived: 2, wantTTL: 1300, live: 2,
		},
		"a head with no ttl over expiring items never expires": {
			items:   []map[string]types.AttributeValue{event(1, 1100), head(1, 0)},
			derived: 1, wantTTL: 1100, live: 1,
		},
		"a head ttl over items that never expire": {
			items:   []map[string]types.AttributeValue{event(1, 0), head(1, 5000)},
			derived: 1, live: 1, kind: evt.HeadTTLLow,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			tally := tallyOf(t, test.items...)

			require.Equal(t, test.derived, tally.derived())
			require.Equal(t, test.wantTTL, tally.wantTTL())
			require.Equal(t, test.live, tally.live)

			finding, warning := tally.classify("s")

			if test.kind == "" {
				require.Nil(t, finding)
			} else {
				require.NotNil(t, finding)
				require.Equal(t, test.kind, finding.Kind, finding.String())
			}

			if test.warningKind == "" {
				require.Nil(t, warning)
			} else {
				require.NotNil(t, warning)
				require.Equal(t, test.warningKind, warning.Kind)
			}
		})
	}
}

// pagedScan serves a fixed list of items as Scan pages of a fixed size, strongly consistent reads
// asserted, and records every UpdateItem.
type pagedScan struct {
	Client

	items    []map[string]types.AttributeValue
	pageSize int

	// updateErr, when set, decides the result of each UpdateItem.
	updateErr func(*dynamodb.UpdateItemInput) error

	mu         sync.Mutex
	updates    []*dynamodb.UpdateItemInput
	puts       int
	unreadable []string // reads that were not strongly consistent
}

func (p *pagedScan) Scan(
	_ context.Context,
	params *dynamodb.ScanInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.ScanOutput, error) {
	p.mu.Lock()
	if params.ConsistentRead == nil || !*params.ConsistentRead {
		p.unreadable = append(p.unreadable, "Scan")
	}
	p.mu.Unlock()

	start := 0

	if params.ExclusiveStartKey != nil {
		pk := params.ExclusiveStartKey["pk"].(*types.AttributeValueMemberS).Value //nolint:errcheck // the test controls the key

		var err error

		start, err = strconv.Atoi(pk)
		if err != nil {
			return nil, err
		}
	}

	end := min(start+p.pageSize, len(p.items))
	out := &dynamodb.ScanOutput{Items: p.items[start:end]}

	if end < len(p.items) {
		out.LastEvaluatedKey = map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: strconv.Itoa(end)}}
	}

	return out, nil
}

func (p *pagedScan) GetItem(
	_ context.Context, params *dynamodb.GetItemInput, _ ...func(*dynamodb.Options),
) (*dynamodb.GetItemOutput, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if params.ConsistentRead == nil || !*params.ConsistentRead {
		p.unreadable = append(p.unreadable, "GetItem")
	}

	return &dynamodb.GetItemOutput{}, nil
}

func (p *pagedScan) PutItem(
	context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options),
) (*dynamodb.PutItemOutput, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.puts++

	return &dynamodb.PutItemOutput{}, nil
}

func (p *pagedScan) UpdateItem(
	_ context.Context,
	params *dynamodb.UpdateItemInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.UpdateItemOutput, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.updates = append(p.updates, params)

	if p.updateErr != nil {
		if err := p.updateErr(params); err != nil {
			return nil, err
		}
	}

	return &dynamodb.UpdateItemOutput{}, nil
}

// The rebuild fence's rows are engine rows, the item a force-release leaves in place of the lock
// included, so a store left force-released can still be migrated.
func TestTheRebuildFenceRowsAreEngineRows(t *testing.T) {
	forceReleased := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: evt.RebuildLockID}, "sk": n(0),
		"forceReleased": &types.AttributeValueMemberBOOL{Value: true},
	}

	client := &pagedScan{pageSize: 2, items: []map[string]types.AttributeValue{
		tallyItem("a", 1, nil),
		forceReleased,
		tallyItem(evt.RebuildGenerationID, 0, map[string]int64{"value": 3}),
	}}

	migration := NewRepository(client, "table").WithLazyInitialization(true).HeadsMigration(HeadsMigrationConfig{
		PageSize: 2, Now: func() time.Time { return time.Unix(tallyNow, 0) },
	})

	scanned, err := migration.scan(context.Background())
	require.NoError(t, err)
	require.Empty(t, scanned.reserved)
	require.Len(t, scanned.tallies, 1)

	require.True(t, isEngineRow(evt.RebuildLockID, tallyItem(evt.RebuildLockID, 0, map[string]int64{"owner": 1, "leaseExpiresAt": 1})))
	require.False(t, isEngineRow(evt.RebuildLockID, tallyItem(evt.RebuildLockID, 0, map[string]int64{"owner": 1})))
	require.False(t, isEngineRow(evt.RebuildLockID, tallyItem(evt.RebuildLockID, 1, nil)))
}

func TestScanAccumulatesAPartitionSplitAcrossPages(t *testing.T) {
	// Two partitions interleaved, so every page boundary splits one.
	client := &pagedScan{pageSize: 2, items: []map[string]types.AttributeValue{
		tallyItem("a", 1, nil), tallyItem("b", 0, map[string]int64{"eventSeq": 7}),
		tallyItem("a", 2, nil), tallyItem("b", 8, nil),
		tallyItem("a", 3, nil), tallyItem("SYSTEM#FEATURES", 0, map[string]int64{"version": 1}),
		tallyItem("b", 9, nil),
	}}

	migration := NewRepository(client, "table").WithLazyInitialization(true).HeadsMigration(HeadsMigrationConfig{
		PageSize: 2, Now: func() time.Time { return time.Unix(tallyNow, 0) },
	})

	scanned, err := migration.scan(context.Background())
	require.NoError(t, err)

	tallies := scanned.tallies
	require.Len(t, tallies, 2, "the reserved partition is skipped")
	require.Empty(t, scanned.reserved, "the feature marker is an engine row")
	require.Equal(t, evt.EventSequence(3), tallies["a"].derived())
	require.Equal(t, evt.EventSequence(9), tallies["b"].derived())
	require.Equal(t, 3, tallies["a"].live)
	require.Equal(t, 3, tallies["b"].live)
}

func TestBackfillWritesConditionalHeadItems(t *testing.T) {
	client := &pagedScan{pageSize: 3, items: []map[string]types.AttributeValue{
		tallyItem("plain", 1, nil), tallyItem("plain", 2, nil),
		tallyItem("retained", 1, map[string]int64{"ttl": 5000}), tallyItem("retained", 2, map[string]int64{"ttl": 6000}),
		tallyItem("present", 1, nil), tallyItem("present", -1, map[string]int64{"head": 1}),
		tallyItem("zero", 0, map[string]int64{"eventSeq": 0}),
	}}

	migration := NewRepository(client, "table").WithLazyInitialization(true).HeadsMigration(HeadsMigrationConfig{
		Concurrency: 1, Now: func() time.Time { return time.Unix(tallyNow, 0) },
	})

	dry, err := migration.backfill(context.Background(), true)
	require.NoError(t, err)
	require.Equal(t, HeadsBackfillReport{DryRun: true, Streams: 4, Written: 2, Present: 1, ZeroHead: 1}, dry)
	require.Empty(t, client.updates, "a dry run writes nothing")

	report, err := migration.backfill(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, HeadsBackfillReport{Streams: 4, Written: 2, Present: 1, ZeroHead: 1}, report)
	require.Len(t, client.updates, 2)

	plain, retained := client.updates[0], client.updates[1]

	require.Equal(t, "table", *plain.TableName)
	require.Equal(t, "plain", plain.Key["pk"].(*types.AttributeValueMemberS).Value) //nolint:errcheck // asserted by the equality
	require.Equal(t, "-1", plain.Key["sk"].(*types.AttributeValueMemberN).Value)    //nolint:errcheck // asserted by the equality
	require.Equal(t, "SET head = :last", *plain.UpdateExpression)
	require.Equal(t, "attribute_not_exists(head)", *plain.ConditionExpression)
	require.Equal(t, map[string]types.AttributeValue{":last": n(2)}, plain.ExpressionAttributeValues)
	require.Empty(t, plain.ExpressionAttributeNames)

	require.Equal(t, "SET head = :last, #ttl = :t", *retained.UpdateExpression)
	require.Equal(t, "attribute_not_exists(head)", *retained.ConditionExpression)
	require.Equal(t, map[string]types.AttributeValue{":last": n(2), ":t": n(6000)}, retained.ExpressionAttributeValues)
	require.Equal(t, map[string]string{"#ttl": "ttl"}, retained.ExpressionAttributeNames)
}

func TestRepairWritesAreConditionedOnTheObservedHead(t *testing.T) {
	client := &pagedScan{pageSize: 100, items: []map[string]types.AttributeValue{
		tallyItem("lower", 1, nil), tallyItem("lower", 2, nil), tallyItem("lower", -1, map[string]int64{"head": 1}),
		tallyItem("higher", 1, nil), tallyItem("higher", -1, map[string]int64{"head": 4}),
		tallyItem("slow", 1, map[string]int64{"ttl": 7000}), tallyItem("slow", -1, map[string]int64{"head": 1, "ttl": 2000}),
		tallyItem("immortal", 1, nil), tallyItem("immortal", -1, map[string]int64{"head": 1, "ttl": 2000}),
	}}

	migration := NewRepository(client, "table").WithLazyInitialization(true).HeadsMigration(HeadsMigrationConfig{
		Concurrency: 1, Now: func() time.Time { return time.Unix(tallyNow, 0) },
	})

	report, err := migration.Repair(context.Background(), HeadsRepairOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	require.Len(t, report.Repaired, 3)
	require.Len(t, report.Left, 1)
	require.Equal(t, evt.EntityID("higher"), report.Left[0].EntityID)
	require.Len(t, client.updates, 3)

	byKey := map[string]*dynamodb.UpdateItemInput{}
	for _, update := range client.updates {
		byKey[update.Key["pk"].(*types.AttributeValueMemberS).Value] = update //nolint:errcheck // asserted below
	}

	lower := byKey["lower"]
	require.Equal(t, "SET head = :last", *lower.UpdateExpression)
	require.Equal(t, "head = :observed", *lower.ConditionExpression)
	require.Equal(t, map[string]types.AttributeValue{":last": n(2), ":observed": n(1)}, lower.ExpressionAttributeValues)

	// A write that changes the ttl is conditioned on the ttl it saw, so a refresh by a head-aware
	// writer in between is never overwritten by a smaller value.
	require.Equal(t, "head = :observed", *byKey["lower"].ConditionExpression)

	slow := byKey["slow"]
	require.Equal(t, "SET head = :last, #ttl = :t", *slow.UpdateExpression)
	require.Equal(t, "head = :observed AND #ttl = :observedTtl", *slow.ConditionExpression)
	require.Equal(t, map[string]types.AttributeValue{
		":last": n(1), ":t": n(7000), ":observed": n(1), ":observedTtl": n(2000),
	}, slow.ExpressionAttributeValues)
	require.Equal(t, map[string]string{"#ttl": "ttl"}, slow.ExpressionAttributeNames)

	immortal := byKey["immortal"]
	require.Equal(t, "SET head = :last REMOVE #ttl", *immortal.UpdateExpression)
	require.Equal(t, "head = :observed AND #ttl = :observedTtl", *immortal.ConditionExpression)
	require.Equal(t, n(2000), immortal.ExpressionAttributeValues[":observedTtl"])
	require.Equal(t, map[string]string{"#ttl": "ttl"}, immortal.ExpressionAttributeNames)

	client.updates = nil

	report, err = migration.Repair(context.Background(), HeadsRepairOptions{ConfirmWritersStopped: true, AllowLower: true})
	require.NoError(t, err)
	require.Len(t, report.Repaired, 4)
	require.Empty(t, report.Left)

	for _, update := range client.updates {
		if update.Key["pk"].(*types.AttributeValueMemberS).Value == "higher" { //nolint:errcheck // asserted by the comparison
			require.Equal(t, "SET head = :last", *update.UpdateExpression)
			require.Equal(t, "head = :observed", *update.ConditionExpression)
			require.Equal(t, map[string]types.AttributeValue{":last": n(1), ":observed": n(4)}, update.ExpressionAttributeValues)
		}
	}
}

func TestMutatingStepsNeedTheOperatorsConfirmation(t *testing.T) {
	client := &pagedScan{pageSize: 10, items: []map[string]types.AttributeValue{tallyItem("a", 1, nil)}}
	migration := NewRepository(client, "table").WithLazyInitialization(true).HeadsMigration(HeadsMigrationConfig{})

	_, err := migration.Migrate(context.Background(), HeadsMigrateOptions{})
	require.ErrorIs(t, err, evt.ErrWritersNotConfirmed)

	_, err = migration.Repair(context.Background(), HeadsRepairOptions{AllowLower: true})
	require.ErrorIs(t, err, evt.ErrWritersNotConfirmed)

	require.Empty(t, client.updates)
}

// A head write that loses its condition (another writer created or changed the head item since the
// scan) is not an error: it is counted, and the verification that follows judges the result.
func TestALostConditionIsRacedNotFatal(t *testing.T) {
	client := &pagedScan{pageSize: 100, items: []map[string]types.AttributeValue{
		tallyItem("a", 1, nil), tallyItem("b", 1, nil), tallyItem("c", 1, nil),
	}}
	client.updateErr = func(input *dynamodb.UpdateItemInput) error {
		if input.Key["pk"].(*types.AttributeValueMemberS).Value == "b" { //nolint:errcheck // the test controls the key
			return &types.ConditionalCheckFailedException{}
		}

		return nil
	}

	migration := NewRepository(client, "table").WithLazyInitialization(true).HeadsMigration(HeadsMigrationConfig{Concurrency: 2})

	report, err := migration.backfill(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, 2, report.Written)
	require.Equal(t, 1, report.Raced)
	require.Len(t, client.updates, 3)
}

// Any other write failure stops the run: nothing is declared, and the error reaches the caller. The
// writes are idempotent, so a rerun resumes.
func TestAFailedWriteStopsTheMigrationBeforeDeclaring(t *testing.T) {
	boom := errors.New("write failed")

	items := make([]map[string]types.AttributeValue, 0, 40)
	for i := range 40 {
		items = append(items, tallyItem("stream-"+strconv.Itoa(1000+i), 1, nil))
	}

	client := &pagedScan{pageSize: 100, items: items}
	client.updateErr = func(*dynamodb.UpdateItemInput) error { return boom }

	migration := NewRepository(client, "table").WithLazyInitialization(true).HeadsMigration(HeadsMigrationConfig{Concurrency: 4})

	report, err := migration.Migrate(context.Background(), HeadsMigrateOptions{ConfirmWritersStopped: true})
	require.ErrorIs(t, err, boom)
	require.False(t, report.Declared)
	require.Zero(t, client.puts, "the marker is never declared after a failed backfill")
	require.Less(t, len(client.updates), 40, "the first failure stops the remaining writes")

	_, err = migration.Repair(context.Background(), HeadsRepairOptions{ConfirmWritersStopped: true})
	require.ErrorIs(t, err, boom)
}

// Strongly consistent reads are a spec requirement (the emulator ignores them, so only this test
// notices a scan that stops asking for them).
func TestEveryReadIsStronglyConsistent(t *testing.T) {
	client := &pagedScan{pageSize: 2, items: []map[string]types.AttributeValue{
		tallyItem("a", 1, nil), tallyItem("a", 2, nil), tallyItem("b", 1, nil),
	}}

	migration := NewRepository(client, "table").WithLazyInitialization(true).HeadsMigration(HeadsMigrationConfig{
		Concurrency: 1, Now: func() time.Time { return time.Unix(tallyNow, 0) },
	})

	ctx := context.Background()

	_, err := migration.Verify(ctx)
	require.NoError(t, err)

	_, err = migration.Repair(ctx, HeadsRepairOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)

	_, err = migration.Migrate(ctx, HeadsMigrateOptions{ConfirmWritersStopped: true})
	require.Error(t, err, "the fake never persists the heads it is asked to write")

	require.Empty(t, client.unreadable)
}

// A repair write that lost its condition leaves its stream as it was. The repair looks again: a
// stream someone else fixed meanwhile is not reported, one that still disagrees is.
func TestARacedRepairReVerifies(t *testing.T) {
	items := []map[string]types.AttributeValue{
		tallyItem("fixed-elsewhere", 1, nil), tallyItem("fixed-elsewhere", 2, nil),
		tallyItem("fixed-elsewhere", -1, map[string]int64{"head": 1}),
		tallyItem("still-wrong", 1, nil), tallyItem("still-wrong", 2, nil),
		tallyItem("still-wrong", -1, map[string]int64{"head": 1}),
	}

	client := &pagedScan{pageSize: 100, items: items}
	client.updateErr = func(input *dynamodb.UpdateItemInput) error {
		if input.Key["pk"].(*types.AttributeValueMemberS).Value == "fixed-elsewhere" { //nolint:errcheck // the test controls the key
			// The other writer raised the head; the next scan sees it.
			client.items[2] = tallyItem("fixed-elsewhere", -1, map[string]int64{"head": 2})
		}

		return &types.ConditionalCheckFailedException{}
	}

	migration := NewRepository(client, "table").WithLazyInitialization(true).HeadsMigration(HeadsMigrationConfig{Concurrency: 1})

	report, err := migration.Repair(context.Background(), HeadsRepairOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	require.Equal(t, 2, report.Raced)
	require.Len(t, report.Remaining, 1)
	require.Equal(t, evt.EntityID("still-wrong"), report.Remaining[0].EntityID)

	// Without a race there is nothing to look at again.
	client.updateErr = nil

	report, err = migration.Repair(context.Background(), HeadsRepairOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	require.Zero(t, report.Raced)
	require.Empty(t, report.Remaining)
}

// A stream in a reserved namespace is invisible to head-aware code, so the migration stops before
// writing anything; engine rows are not streams.
func TestReservedStreamsStopTheMigrationBeforeAnyWrite(t *testing.T) {
	client := &pagedScan{pageSize: 100, items: []map[string]types.AttributeValue{
		tallyItem("a", 1, nil),
		tallyItem("SYSTEM#FEATURES", 0, map[string]int64{"version": 3}),
		tallyItem("command-outcome#c1", 1, map[string]int64{"status": 1}),
		tallyItem("SYSTEM#account", 1, nil),
		tallyItem("SYSTEM#account", 2, nil),
		tallyItem("command-outcome#legacy", 1, nil),
		tallyItem("SYSTEM#expired", 1, map[string]int64{"ttl": 5}),
	}}

	migration := NewRepository(client, "table").WithLazyInitialization(true).HeadsMigration(HeadsMigrationConfig{
		Now: func() time.Time { return time.Unix(tallyNow, 0) },
	})

	for _, dryRun := range []bool{true, false} {
		_, err := migration.Migrate(context.Background(), HeadsMigrateOptions{ConfirmWritersStopped: true, DryRun: dryRun})

		var reserved *evt.ReservedStreamsError
		require.ErrorAs(t, err, &reserved)
		require.ErrorIs(t, err, evt.ErrReservedStreams)
		require.Equal(t, []evt.EntityID{"SYSTEM#account", "command-outcome#legacy"}, reserved.IDs)
		require.Equal(t, 2, reserved.Total)
	}

	require.Empty(t, client.updates)
	require.Zero(t, client.puts)

	report, err := migration.Verify(context.Background())
	require.NoError(t, err)
	require.Equal(t, []evt.EntityID{"SYSTEM#account", "command-outcome#legacy"}, report.ReservedStreams)
}

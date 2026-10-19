//go:build awsmeasure

// Package awsmeasure measures what DynamoDB bills for the transaction actions the per-stream head
// item (ADR 0007) changes, using ReturnConsumedCapacity on a scratch table. It exists because the
// ADR's WCU numbers assume a ConditionCheck bills by the size of the item it reads, and only real
// DynamoDB can confirm that: the local emulator does not bill.
//
// It is behind the awsmeasure build tag and an explicit confirmation, so no ordinary test run
// reaches AWS. A human runs it with credentials that may create and delete a table:
//
//	EVT_AWSMEASURE_CONFIRM=yes AWS_REGION=us-west-2 \
//	  go test -tags=awsmeasure ./dynamo/awsmeasure -run TestMeasureConsumedCapacity -v -count=1
//
// It creates an on-demand table named evt-awsmeasure-<unix time>, writes a handful of items,
// prints one row per measured request, and deletes the table (also when a request fails). Cost is
// a few dozen request units. The IAM actions it needs are dynamodb:CreateTable, DescribeTable,
// PutItem, UpdateItem, GetItem, and DeleteTable on that table, plus the item-level actions that
// TransactWriteItems requires. AWS_ENDPOINT_URL points it at an emulator instead, which prints
// zeros and proves only that the script runs.
package awsmeasure

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt/internal/streamheads"
)

// callTimeout bounds every request.
const callTimeout = 30 * time.Second

type measurement struct {
	name        string
	description string
	write       float64
	read        float64
	reported    bool
}

func num(n int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(n, 10)}
}

func str(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }

func key(pk string, sk int64) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"pk": str(pk), "sk": num(sk)}
}

// snapshotItem is an inline snapshot (sk = 0) padded so the whole item is about size bytes, the
// way DynamoDB counts it (attribute names plus values).
func snapshotItem(pk string, size int) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		"pk": str(pk), "sk": num(0), "seq": num(3), "eventSeq": num(7), "entityType": str("scratch"),
	}

	used := 0
	for name, value := range item {
		used += len(name)

		switch v := value.(type) {
		case *types.AttributeValueMemberS:
			used += len(v.Value)
		case *types.AttributeValueMemberN:
			used += len(v.Value)
		}
	}

	used += len("payload")
	item["payload"] = str(strings.Repeat("x", max(0, size-used)))

	return item
}

func eventItem(pk string, sequence int64) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": str(pk), "sk": num(sequence), "type": str("scratch:event"), "version": num(1),
		"entityType": str("scratch"), "payload": str(`{"amount":10}`),
		"metadata": str(`{"region":"us-west-2","timestamp":"2026-10-12T00:00:00Z"}`),
	}
}

func TestMeasureConsumedCapacity(t *testing.T) {
	if os.Getenv("EVT_AWSMEASURE_CONFIRM") != "yes" {
		t.Skip("set EVT_AWSMEASURE_CONFIRM=yes to create a scratch table and measure (see the package comment)")
	}

	root := context.Background()

	call := func() (context.Context, context.CancelFunc) { return context.WithTimeout(root, callTimeout) }

	cfg, err := config.LoadDefaultConfig(root,
		config.WithHTTPClient(awshttp.NewBuildableClient().WithTimeout(callTimeout)),
		config.WithRegion(envOr("AWS_REGION", "us-west-2")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg)
	table := fmt.Sprintf("evt-awsmeasure-%d", time.Now().Unix())

	createTable(t, client, table)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*callTimeout)
		defer cancel()

		if _, derr := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(table)}); derr != nil {
			t.Errorf("DELETE THE SCRATCH TABLE %s BY HAND: %v", table, derr)
		}
	})

	// Items: snapshots of three sizes, a head item, and a stream of events to continue.
	seed := []map[string]types.AttributeValue{
		snapshotItem("snap-1k", 1024), snapshotItem("snap-4k", 4*1024), snapshotItem("snap-12k", 12*1024),
		{"pk": str("headed"), "sk": num(-1), "head": num(7)},
		eventItem("headed", 7),
	}

	for _, item := range seed {
		ctx, cancel := call()
		_, perr := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(table), Item: item})
		cancel()
		require.NoError(t, perr)
	}

	transact := func(name, description string, items ...types.TransactWriteItem) measurement {
		ctx, cancel := call()
		defer cancel()

		out, terr := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems:          items,
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		})
		require.NoError(t, terr, name)

		m := measurement{name: name, description: description}

		for _, consumed := range out.ConsumedCapacity {
			m.reported = true
			m.write += aws.ToFloat64(consumed.WriteCapacityUnits)
			m.read += aws.ToFloat64(consumed.ReadCapacityUnits)

			if consumed.WriteCapacityUnits == nil && consumed.ReadCapacityUnits == nil {
				m.write += aws.ToFloat64(consumed.CapacityUnits)
			}
		}

		return m
	}

	check := func(pk string, sk int64, condition string, values map[string]types.AttributeValue) types.TransactWriteItem {
		return types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
			TableName: aws.String(table), Key: key(pk, sk), ConditionExpression: aws.String(condition),
			ExpressionAttributeValues: values,
		}}
	}

	headUpdate := func(prev, last int64) types.TransactWriteItem {
		return types.TransactWriteItem{Update: &types.Update{
			TableName: aws.String(table), Key: key("headed", -1),
			UpdateExpression:          aws.String(streamheads.UpdateExpression),
			ConditionExpression:       aws.String(streamheads.AppendCondition),
			ExpressionAttributeValues: map[string]types.AttributeValue{":prev": num(prev), ":last": num(last)},
		}}
	}

	put := func(item map[string]types.AttributeValue) types.TransactWriteItem {
		return types.TransactWriteItem{Put: &types.Put{TableName: aws.String(table), Item: item}}
	}

	floor := map[string]types.AttributeValue{":first": num(8)}

	results := append(make([]measurement, 0, 12),
		transact("check, 1 KB snapshot", "old floor check on a 1 KB snapshot item",
			check("snap-1k", 0, "eventSeq < :first", floor)),
		transact("check, 4 KB snapshot", "old floor check on a 4 KB snapshot item",
			check("snap-4k", 0, "eventSeq < :first", floor)),
		transact("check, 12 KB snapshot", "old floor check / fence companion on a 12 KB snapshot item",
			check("snap-12k", 0, "eventSeq < :first", floor)),
		transact("check, missing item", "a ConditionCheck on an item that does not exist",
			check("absent", 0, "attribute_not_exists(pk)", nil)),
		transact("check, head item", "the new fence: head = :head on the head item",
			check("headed", -1, streamheads.FenceCondition, map[string]types.AttributeValue{":head": num(7)})),
		transact("check, head item missing", "the new fence on a stream with no head item",
			check("absent", -1, streamheads.FenceAbsentCondition, nil)),
		transact("update, head item", "the head update of an append (head 7 to 8)", headUpdate(7, 8)),
		transact("put, event", "one event put, for scale", put(eventItem("scale", 1))),
		transact("append, new layout", "head update + one event put", headUpdate(8, 9), put(eventItem("headed", 9))),
		transact("append, old layout", "continuation check (head event) + one event put + floor check (12 KB snapshot)",
			check("headed", 7, "attribute_exists(pk)", nil), put(eventItem("headed", 10)),
			check("snap-12k", 0, "eventSeq < :first", floor)),
	)

	// Strongly consistent reads: what the pre-commit continuity reads cost.
	results = append(results,
		getItem(t, client, table, "read, 12 KB snapshot", "strongly consistent GetItem of the snapshot", "snap-12k", 0),
		getItem(t, client, table, "read, head item", "strongly consistent GetItem of the head item", "headed", -1),
	)

	printResults(t, table, results)

	for _, m := range results {
		if !m.reported {
			t.Logf("NOTE: no consumed capacity was reported (an emulator does not bill); the figures above are not measurements")

			break
		}
	}
}

func getItem(t *testing.T, client *dynamodb.Client, table, name, description, pk string, sk int64) measurement {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table), Key: key(pk, sk), ConsistentRead: aws.Bool(true),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	require.NoError(t, err, name)

	m := measurement{name: name, description: description}

	if out.ConsumedCapacity != nil {
		m.reported = true
		m.read = aws.ToFloat64(out.ConsumedCapacity.CapacityUnits)
	}

	return m
}

func createTable(t *testing.T, client *dynamodb.Client, table string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(table),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		describeCtx, describeCancel := context.WithTimeout(context.Background(), callTimeout)
		defer describeCancel()

		out, derr := client.DescribeTable(describeCtx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})

		return derr == nil && out.Table != nil && out.Table.TableStatus == types.TableStatusActive
	}, 2*time.Minute, time.Second)
}

func printResults(t *testing.T, table string, results []measurement) {
	t.Helper()

	var out strings.Builder

	fmt.Fprintf(&out, "\nConsumed capacity on %s (ReturnConsumedCapacity=TOTAL, one action per request unless noted)\n\n", table)
	fmt.Fprintf(&out, "| %-26s | %6s | %5s | %s |\n", "request", "WCU", "RCU", "what it is")
	fmt.Fprintf(&out, "| %s | %s | %s | %s |\n", strings.Repeat("-", 26), strings.Repeat("-", 6), strings.Repeat("-", 5), strings.Repeat("-", 10))

	for _, m := range results {
		fmt.Fprintf(&out, "| %-26s | %6.1f | %5.1f | %s |\n", m.name, m.write, m.read, m.description)
	}

	fmt.Fprintf(&out, "\nPaste this table into ADR 0007 (WCU and RCU) and replace the unconfirmed-assumption wording.\n")
	t.Log(out.String())
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

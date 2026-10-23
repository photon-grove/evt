package compat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	"github.com/photon-grove/evt/go/viewstore"
)

// ViewRepoFixtureFile holds the view repository and typed view codec vectors (P1, P2, P10): the
// items and requests Go's dynamo.ViewRepository sends, its pagination cursors, and the payload
// bytes of viewstore.Codec.
const ViewRepoFixtureFile = "views.json"

// ViewRepoFixtures pins the Go view repository's encoding for the Rust crate.
type ViewRepoFixtures struct {
	// Limits are the DynamoDB per-call limits the batch writers and readers chunk to.
	Limits ViewLimits `json:"limits"`

	// Puts are the items PutView sends in its single-item BatchWriteItem.
	Puts []ViewPutVector `json:"puts"`

	// Deletes are the keys DeleteView sends.
	Deletes []ViewDeleteVector `json:"deletes"`

	// Gets are the GetItem requests of GetView and the rows its responses decode to.
	Gets []ViewGetVector `json:"gets"`

	// Decodes are items read back into rows (or refused).
	Decodes []ViewDecodeVector `json:"decodes"`

	// Queries are the Query requests of the paged lists.
	Queries []ViewQueryVector `json:"queries"`

	// Cursors are pagination cursors: a LastEvaluatedKey and the cursor Go encodes it to.
	Cursors []ViewCursorVector `json:"cursors"`

	// StartKeys are cursors read back into an ExclusiveStartKey (or refused).
	StartKeys []ViewStartKeyVector `json:"startKeys"`

	// Codec are the items typed Codec.Put sends for typed values.
	Codec []ViewCodecPutVector `json:"codec"`

	// CodecDecode are payloads typed decoding accepts or refuses.
	CodecDecode []ViewCodecDecodeVector `json:"codecDecode"`
}

// ViewLimits are the batch sizes of the view repository.
type ViewLimits struct {
	Write int `json:"write"`
	Get   int `json:"get"`
}

// ViewRowInput is a serialized view field by field. The payload is given as text.
type ViewRowInput struct {
	PK         string `json:"pk"`
	SK         string `json:"sk"`
	EntityID   string `json:"entityId"`
	EntityType string `json:"entityType"`
	Payload    string `json:"payload"`
	TTL        int64  `json:"ttl"`
}

func (r ViewRowInput) view() *evt.SerializedView {
	return &evt.SerializedView{
		PK: r.PK, SK: r.SK, EntityID: evt.EntityID(r.EntityID), EntityType: evt.EntityType(r.EntityType),
		Payload: []byte(r.Payload), TTL: r.TTL,
	}
}

func rowInput(view *evt.SerializedView) ViewRowInput {
	return ViewRowInput{
		PK: view.PK, SK: view.SK, EntityID: string(view.EntityID), EntityType: string(view.EntityType),
		Payload: string(view.Payload), TTL: view.TTL,
	}
}

// ViewPutVector is one PutView call and the item it wrote.
type ViewPutVector struct {
	Name string         `json:"name"`
	Row  ViewRowInput   `json:"row"`
	Item map[string]any `json:"dynamoItem"`
}

// ViewDeleteVector is one DeleteView call and the key it sent.
type ViewDeleteVector struct {
	Name string         `json:"name"`
	PK   string         `json:"pk"`
	SK   string         `json:"sk"`
	Key  map[string]any `json:"key"`
}

// ViewGetVector is the GetItem request of a GetView.
type ViewGetVector struct {
	Name           string         `json:"name"`
	PK             string         `json:"pk"`
	Key            map[string]any `json:"key"`
	ConsistentRead bool           `json:"consistentRead"`
}

// ViewDecodeVector is an item the repository read, and the row it produced.
type ViewDecodeVector struct {
	Name  string         `json:"name"`
	Item  map[string]any `json:"dynamoItem"`
	Row   *ViewRowInput  `json:"row"`
	Error bool           `json:"error"`
}

// ViewQueryVector is the Query request one paged list sent.
type ViewQueryVector struct {
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Key    string `json:"key"`
	Limit  int    `json:"limit"`
	Cursor string `json:"cursor"`

	Index         string         `json:"index"`
	KeyCondition  string         `json:"keyCondition"`
	Values        map[string]any `json:"values"`
	RequestLimit  *int32         `json:"requestLimit"`
	StartKey      map[string]any `json:"startKey"`
	ConsistentSet bool           `json:"consistentSet"`
	// ConsistentRead is the request's ConsistentRead: true for a partition query of the base
	// table, unset (false) for the entity-type index, which cannot be read consistently.
	ConsistentRead bool `json:"consistentRead"`
}

// ViewCursorVector is a LastEvaluatedKey and its cursor.
type ViewCursorVector struct {
	Name             string         `json:"name"`
	LastEvaluatedKey map[string]any `json:"lastEvaluatedKey"`
	Cursor           string         `json:"cursor"`
}

// ViewStartKeyVector is a cursor and the ExclusiveStartKey it decodes to.
type ViewStartKeyVector struct {
	Name     string         `json:"name"`
	Cursor   string         `json:"cursor"`
	StartKey map[string]any `json:"startKey"`
	Error    bool           `json:"error"`
}

// ViewCodecPutVector is a typed value written through viewstore.Codec and the item that resulted.
type ViewCodecPutVector struct {
	Name     string         `json:"name"`
	PK       string         `json:"pk"`
	SK       string         `json:"sk"`
	EntityID string         `json:"entityId"`
	Value    string         `json:"value"`
	Item     map[string]any `json:"dynamoItem"`
}

// ViewCodecDecodeVector is a stored payload decoded into the typed value. When it decodes, Canonical
// is the value encoded again.
type ViewCodecDecodeVector struct {
	Name      string `json:"name"`
	Payload   string `json:"payload"`
	Valid     bool   `json:"valid"`
	Canonical string `json:"canonical"`
}

// CodecEntityType is the entity type the typed codec vectors bind.
const CodecEntityType evt.EntityType = "profile"

// CodecProfile is the typed view of the codec vectors. Declared in this order, it encodes the same
// bytes as the Rust struct.
type CodecProfile struct {
	Name   string            `json:"name"`
	Note   string            `json:"note"`
	Bio    *string           `json:"bio"`
	Tags   []string          `json:"tags"`
	Labels map[string]string `json:"labels"`
	Count  int64             `json:"count"`
	Big    uint64            `json:"big"`
}

// viewScript is a DynamoDB client that records the view requests and answers with scripted output.
type viewScript struct {
	*FakeDynamo

	writes []*dynamodb.BatchWriteItemInput
	gets   []*dynamodb.GetItemInput
	query  []*dynamodb.QueryInput

	getOutput   map[string]types.AttributeValue
	queryOutput *dynamodb.QueryOutput
}

func (s *viewScript) BatchWriteItem(
	_ context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options),
) (*dynamodb.BatchWriteItemOutput, error) {
	s.writes = append(s.writes, params)

	return &dynamodb.BatchWriteItemOutput{}, nil
}

func (s *viewScript) GetItem(
	_ context.Context, params *dynamodb.GetItemInput, _ ...func(*dynamodb.Options),
) (*dynamodb.GetItemOutput, error) {
	s.gets = append(s.gets, params)

	return &dynamodb.GetItemOutput{Item: s.getOutput}, nil
}

func (s *viewScript) Query(
	_ context.Context, params *dynamodb.QueryInput, _ ...func(*dynamodb.Options),
) (*dynamodb.QueryOutput, error) {
	s.query = append(s.query, params)

	if s.queryOutput == nil {
		return &dynamodb.QueryOutput{}, nil
	}

	return s.queryOutput, nil
}

const viewsFixtureTable = "views"

func viewRepoRows() []struct {
	name string
	row  ViewRowInput
} {
	return []struct {
		name string
		row  ViewRowInput
	}{
		{"full", ViewRowInput{PK: "account#acct-1", SK: "VIEW", EntityID: "acct-1", EntityType: "account", Payload: `{"status":"open"}`, TTL: 1791849601}},
		{"no-ttl", ViewRowInput{PK: "handles", SK: "ada", EntityID: "acct-1", EntityType: "account", Payload: `{"owner":"acct-1"}`}},
		{"empty-sk-defaults", ViewRowInput{PK: "account#acct-2", SK: "", EntityID: "acct-2", EntityType: "account", Payload: `{}`, TTL: 5}},
		{"negative-ttl", ViewRowInput{PK: "x", SK: "y", EntityID: "e", EntityType: "t", Payload: `{}`, TTL: -5}},
		{"escaped", ViewRowInput{PK: "team#é<&>", SK: "k\u2028", EntityID: "e<&>", EntityType: "t", Payload: "{\"note\":\"<&> \\u2028 é\"}"}},
		{"empty-strings", ViewRowInput{PK: "p", SK: "s"}},
	}
}

// GenerateViewRepoFixtures sends every case through the real Go view repository and viewstore
// codec against a recording client.
func GenerateViewRepoFixtures(ctx context.Context) (ViewRepoFixtures, error) {
	fixtures := ViewRepoFixtures{Limits: ViewLimits{Write: 25, Get: 100}}

	for _, c := range viewRepoRows() {
		script := &viewScript{FakeDynamo: NewFakeDynamo()}
		repo := dynamo.NewViewRepository(script, viewsFixtureTable)

		if err := repo.PutView(ctx, c.row.view()); err != nil {
			return fixtures, fmt.Errorf("put %s: %w", c.name, err)
		}

		request, err := onlyWrite(script)
		if err != nil {
			return fixtures, fmt.Errorf("put %s: %w", c.name, err)
		}

		if request.PutRequest == nil {
			return fixtures, fmt.Errorf("put %s: not a put", c.name)
		}

		fixtures.Puts = append(fixtures.Puts, ViewPutVector{Name: c.name, Row: c.row, Item: DynamoItemJSON(request.PutRequest.Item)})
	}

	for _, c := range []struct{ name, pk, sk string }{
		{"explicit", "handles", "ada"},
		{"empty-sk-defaults", "account#acct-1", ""},
	} {
		script := &viewScript{FakeDynamo: NewFakeDynamo()}
		repo, ok := dynamo.NewViewRepository(script, viewsFixtureTable).(*dynamo.ViewRepository)
		if !ok {
			return fixtures, errors.New("the view repository is not a *dynamo.ViewRepository")
		}

		if err := repo.DeleteView(ctx, c.pk, c.sk); err != nil {
			return fixtures, fmt.Errorf("delete %s: %w", c.name, err)
		}

		request, err := onlyWrite(script)
		if err != nil {
			return fixtures, fmt.Errorf("delete %s: %w", c.name, err)
		}

		if request.DeleteRequest == nil {
			return fixtures, fmt.Errorf("delete %s: not a delete", c.name)
		}

		fixtures.Deletes = append(fixtures.Deletes, ViewDeleteVector{
			Name: c.name, PK: c.pk, SK: c.sk, Key: DynamoItemJSON(request.DeleteRequest.Key),
		})
	}

	gets, err := viewGetVectors(ctx)
	if err != nil {
		return fixtures, err
	}

	fixtures.Gets = gets

	decodes, err := viewDecodeVectors(ctx)
	if err != nil {
		return fixtures, err
	}

	fixtures.Decodes = decodes

	if err := fillViewQueries(ctx, &fixtures); err != nil {
		return fixtures, err
	}

	if err := fillViewCodec(ctx, &fixtures); err != nil {
		return fixtures, err
	}

	return fixtures, nil
}

func onlyWrite(script *viewScript) (types.WriteRequest, error) {
	if len(script.writes) != 1 {
		return types.WriteRequest{}, fmt.Errorf("want one batch write, got %d", len(script.writes))
	}

	requests := script.writes[0].RequestItems[viewsFixtureTable]
	if len(requests) != 1 {
		return types.WriteRequest{}, fmt.Errorf("want one write request, got %d", len(requests))
	}

	return requests[0], nil
}

func str(value string) types.AttributeValue { return &types.AttributeValueMemberS{Value: value} }

func num(value int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)}
}

func viewGetVectors(ctx context.Context) ([]ViewGetVector, error) {
	var vectors []ViewGetVector

	for _, pk := range []string{"account#acct-1", "team#é<&>"} {
		script := &viewScript{FakeDynamo: NewFakeDynamo()}
		repo := dynamo.NewViewRepository(script, viewsFixtureTable)

		view, err := repo.GetView(ctx, pk)
		if err != nil || view != nil {
			return nil, fmt.Errorf("get %s: view %v err %w", pk, view, err)
		}

		if len(script.gets) != 1 {
			return nil, fmt.Errorf("get %s: want one request", pk)
		}

		request := script.gets[0]
		vectors = append(vectors, ViewGetVector{
			Name: pk, PK: pk, Key: DynamoItemJSON(request.Key), ConsistentRead: aws.ToBool(request.ConsistentRead),
		})
	}

	return vectors, nil
}

func viewDecodeVectors(ctx context.Context) ([]ViewDecodeVector, error) {
	cases := []struct {
		name string
		item map[string]types.AttributeValue
	}{
		{"full", map[string]types.AttributeValue{
			"pk": str("account#acct-1"), "sk": str("VIEW"), "entityID": str("acct-1"), "entityType": str("account"),
			"payload": str(`{"a":1}`), "ttl": num(1791849601),
		}},
		{"no-ttl", map[string]types.AttributeValue{
			"pk": str("p"), "sk": str("s"), "entityID": str("e"), "entityType": str("t"), "payload": str("{}"),
		}},
		{"missing-attributes", map[string]types.AttributeValue{"pk": str("p"), "sk": str("s")}},
		{"extra-attributes", map[string]types.AttributeValue{
			"pk": str("p"), "sk": str("s"), "other": str("ignored"), "payload": str("x"),
		}},
		{"negative-ttl", map[string]types.AttributeValue{"pk": str("p"), "sk": str("s"), "ttl": num(-5)}},
		{"ttl-wrong-type", map[string]types.AttributeValue{"pk": str("p"), "sk": str("s"), "ttl": str("soon")}},
		{"payload-wrong-type", map[string]types.AttributeValue{"pk": str("p"), "sk": str("s"), "payload": num(1)}},
		{"null-attributes", map[string]types.AttributeValue{
			"pk": str("p"), "sk": str("s"), "entityType": &types.AttributeValueMemberNULL{Value: true},
			"payload": &types.AttributeValueMemberNULL{Value: true}, "ttl": &types.AttributeValueMemberNULL{Value: true},
		}},
		{"ttl-fraction", map[string]types.AttributeValue{"pk": str("p"), "sk": str("s"), "ttl": &types.AttributeValueMemberN{Value: "1.5"}}},
	}

	vectors := make([]ViewDecodeVector, 0, len(cases))

	for _, c := range cases {
		script := &viewScript{FakeDynamo: NewFakeDynamo(), getOutput: c.item}
		repo := dynamo.NewViewRepository(script, viewsFixtureTable)

		view, err := repo.GetView(ctx, "ignored")

		vector := ViewDecodeVector{Name: c.name, Item: DynamoItemJSON(c.item)}
		if err != nil {
			vector.Error = true
		} else {
			input := rowInput(view)
			vector.Row = &input
		}

		vectors = append(vectors, vector)
	}

	return vectors, nil
}

func fillViewQueries(ctx context.Context, fixtures *ViewRepoFixtures) error {
	cases := []struct {
		name, kind, key string
		limit           int
		cursor          string
	}{
		{"pk-unbounded", "pk", "USER#1#teams", 0, ""},
		{"pk-limit", "pk", "USER#1#teams", 2, ""},
		{"pk-negative-limit", "pk", "USER#1#teams", -3, ""},
		{"entity-type-limit", "entityType", "account", 25, ""},
		{"entity-type-unbounded", "entityType", "account", 0, ""},
		{"pk-cursor", "pk", "USER#1#teams", 2, "eyJwayI6IlVTRVIjMSN0ZWFtcyIsInNrIjoidDIifQ=="},
	}

	for _, c := range cases {
		script := &viewScript{FakeDynamo: NewFakeDynamo()}
		repo := dynamo.NewViewRepository(script, viewsFixtureTable)

		var err error
		if c.kind == "pk" {
			_, err = repo.ListViewsByPKPaged(ctx, c.key, c.limit, c.cursor)
		} else {
			_, err = repo.ListViewsByEntityTypePaged(ctx, evt.EntityType(c.key), c.limit, c.cursor)
		}

		if err != nil {
			return fmt.Errorf("query %s: %w", c.name, err)
		}

		if len(script.query) != 1 {
			return fmt.Errorf("query %s: want one request, got %d", c.name, len(script.query))
		}

		request := script.query[0]
		values := map[string]any{}
		for name, value := range request.ExpressionAttributeValues {
			values[name] = DynamoJSON(value)
		}

		var startKey map[string]any
		if request.ExclusiveStartKey != nil {
			startKey = DynamoItemJSON(request.ExclusiveStartKey)
		}

		fixtures.Queries = append(fixtures.Queries, ViewQueryVector{
			Name: c.name, Kind: c.kind, Key: c.key, Limit: c.limit, Cursor: c.cursor,
			Index: aws.ToString(request.IndexName), KeyCondition: aws.ToString(request.KeyConditionExpression),
			Values: values, RequestLimit: request.Limit, StartKey: startKey,
			ConsistentSet: request.ConsistentRead != nil, ConsistentRead: aws.ToBool(request.ConsistentRead),
		})
	}

	cursors := []struct {
		name string
		key  map[string]types.AttributeValue
	}{
		{"table-key", map[string]types.AttributeValue{"pk": str("USER#1#teams"), "sk": str("t2")}},
		{"index-key", map[string]types.AttributeValue{"pk": str("account#acct-1"), "sk": str("VIEW"), "entityType": str("account")}},
		{"escaped", map[string]types.AttributeValue{"pk": str("team#é<&>"), "sk": str("k\u2028"), "entityType": str("t")}},
		{"non-string-dropped", map[string]types.AttributeValue{"pk": str("p"), "sk": str("s"), "n": num(1)}},
		{"padding-and-url-alphabet", map[string]types.AttributeValue{"pk": str("???>>>"), "sk": str("~~~")}},
	}

	for _, c := range cursors {
		script := &viewScript{
			FakeDynamo:  NewFakeDynamo(),
			queryOutput: &dynamodb.QueryOutput{LastEvaluatedKey: c.key},
		}
		repo := dynamo.NewViewRepository(script, viewsFixtureTable)

		page, err := repo.ListViewsByPKPaged(ctx, "ignored", 1, "")
		if err != nil {
			return fmt.Errorf("cursor %s: %w", c.name, err)
		}

		fixtures.Cursors = append(fixtures.Cursors, ViewCursorVector{
			Name: c.name, LastEvaluatedKey: DynamoItemJSON(c.key), Cursor: page.NextCursor,
		})
	}

	starts := []struct{ name, cursor string }{
		{"table-key", fixtures.Cursors[0].Cursor},
		{"index-key", fixtures.Cursors[1].Cursor},
		{"escaped", fixtures.Cursors[2].Cursor},
		{"not-base64", "not base64!"},
		{"not-json", "bm90IGpzb24="},
		{"not-strings", "eyJwayI6MX0="},
		{"unpadded", "eyJwayI6InAiLCJzayI6InMifQ"},
	}

	for _, c := range starts {
		script := &viewScript{FakeDynamo: NewFakeDynamo()}
		repo := dynamo.NewViewRepository(script, viewsFixtureTable)

		_, err := repo.ListViewsByPKPaged(ctx, "ignored", 1, c.cursor)

		vector := ViewStartKeyVector{Name: c.name, Cursor: c.cursor}
		if err != nil {
			vector.Error = true
		} else {
			vector.StartKey = DynamoItemJSON(script.query[0].ExclusiveStartKey)
		}

		fixtures.StartKeys = append(fixtures.StartKeys, vector)
	}

	return nil
}

func fillViewCodec(ctx context.Context, fixtures *ViewRepoFixtures) error {
	bio := "likes <b>bold</b> & more"

	values := []struct {
		name, pk, sk, entityID string
		value                  CodecProfile
	}{
		{"full", "profile#ada", "VIEW", "ada", CodecProfile{
			Name: "Ada <&> \u2028", Note: "line\nbreak \"quoted\" \\ é 😀", Bio: &bio,
			Tags: []string{"b", "a"}, Labels: map[string]string{"tier": "gold", "region": "emea"},
			Count: -9007199254740993, Big: 18446744073709551615,
		}},
		{"zero", "profile#zero", "", "zero", CodecProfile{}},
		{"empty-collections", "profile#empty", "VIEW", "", CodecProfile{Tags: []string{}, Labels: map[string]string{}}},
	}

	for _, c := range values {
		script := &viewScript{FakeDynamo: NewFakeDynamo()}
		codec := viewstore.New[CodecProfile](dynamo.NewViewRepository(script, viewsFixtureTable), CodecEntityType)

		if err := codec.Put(ctx, c.pk, c.sk, evt.EntityID(c.entityID), c.value); err != nil {
			return fmt.Errorf("codec %s: %w", c.name, err)
		}

		request, err := onlyWrite(script)
		if err != nil {
			return fmt.Errorf("codec %s: %w", c.name, err)
		}

		encoded, err := json.Marshal(c.value)
		if err != nil {
			return err
		}

		fixtures.Codec = append(fixtures.Codec, ViewCodecPutVector{
			Name: c.name, PK: c.pk, SK: c.sk, EntityID: c.entityID, Value: string(encoded),
			Item: DynamoItemJSON(request.PutRequest.Item),
		})
	}

	payloads := []struct{ name, payload string }{
		{"full", `{"name":"Ada","note":"n","bio":"b","tags":["x"],"labels":{"k":"v"},"count":3,"big":7}`},
		{"unknown-field-ignored", `{"name":"Ada","extra":{"nested":[1,2]}}`},
		{"empty-object", `{}`},
		{"nulls", `{"name":"x","bio":null,"tags":null,"labels":null}`},
		{"escaped", `{"name":"\u003c\u0026\u003e \u2028 \ud83d\ude00"}`},
		{"truncated", `{"name":`},
		{"empty-payload", ``},
		{"not-json", `plain text`},
		{"array", `[1,2]`},
		{"wrong-type", `{"name":5}`},
		{"count-fraction", `{"count":1.5}`},
		{"count-overflow", `{"count":9223372036854775808}`},
		{"negative-big", `{"big":-1}`},
		{"trailing-garbage", `{} x`},
	}

	for _, c := range payloads {
		view := &evt.SerializedView{PK: "p", SK: "s", Payload: []byte(c.payload)}

		var value CodecProfile

		vector := ViewCodecDecodeVector{Name: c.name, Payload: c.payload}
		if err := evt.DecodeJSONView(view, &value); err == nil {
			canonical, marshalErr := json.Marshal(value)
			if marshalErr != nil {
				return marshalErr
			}

			vector.Valid = true
			vector.Canonical = string(canonical)
		}

		fixtures.CodecDecode = append(fixtures.CodecDecode, vector)
	}

	return nil
}

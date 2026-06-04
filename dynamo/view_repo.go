package dynamo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
)

// ViewRepository provides Dynamo-backed views.
type ViewRepository struct {
	ViewsTable string

	client  Client
	encoder *attributevalue.Encoder
	decoder *attributevalue.Decoder
}

const entityTypeIndexName = "entityType-index"
const maxBatchWriteRetries = 3

// maxBatchWriteItems and maxBatchGetItems are the DynamoDB per-call limits for
// BatchWriteItem and BatchGetItem respectively. PutViews/BatchGetViews chunk
// their inputs to these sizes.
const maxBatchWriteItems = 25
const maxBatchGetItems = 100
const maxBatchGetRetries = 3

// Compile-time checks that the DynamoDB view repository satisfies both the core read/write
// interface and the optional streaming interface.
var (
	_ evt.ViewRepository = (*ViewRepository)(nil)
	_ evt.ViewStreamer   = (*ViewRepository)(nil)
)

// NewViewRepository constructs a view repository for the given table.
func NewViewRepository(client Client, viewsTable string) evt.ViewRepository {
	encoder := attributevalue.NewEncoder(func(opts *attributevalue.EncoderOptions) {
		opts.TagKey = tagKey
	})
	decoder := attributevalue.NewDecoder(func(opts *attributevalue.DecoderOptions) {
		opts.TagKey = tagKey
	})
	return &ViewRepository{ViewsTable: viewsTable, client: client, encoder: encoder, decoder: decoder}
}

// View is the Dynamo serialization for a materialized view.
type View struct {
	PK         string         `json:"pk"`
	SK         string         `json:"sk"`
	EntityID   evt.EntityID   `json:"entityID"`
	EntityType evt.EntityType `json:"entityType"`
	Payload    string         `json:"payload"`
	TTL        int64          `json:"ttl,omitempty"`
}

// GetView retrieves a view by PK using the default sort key "VIEW".
// For views with custom sort keys, use ListViewsByPK instead.
func (repo *ViewRepository) GetView(ctx context.Context, pk string) (*evt.SerializedView, error) {
	input := dynamodb.GetItemInput{
		TableName:      &repo.ViewsTable,
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
			"sk": &types.AttributeValueMemberS{Value: evt.DefaultViewSK},
		},
	}

	result, err := repo.client.GetItem(ctx, &input)
	if err != nil {
		return nil, err
	}
	if len(result.Item) == 0 {
		return nil, nil
	}

	var view View
	if err := repo.unmarshalMap(result.Item, &view); err != nil {
		return nil, err
	}

	return repo.toSerializedView(view), nil
}

// BatchGetViews retrieves multiple views by partition key using the default sort
// key "VIEW". Keys are de-duplicated, requested via BatchGetItem in chunks of
// maxBatchGetItems, and UnprocessedKeys are retried with backoff. Only views
// that exist are returned; missing keys are omitted and order is not guaranteed,
// so callers should index the result by PK or by a payload identifier. Reads are
// strongly consistent to match GetView. Use BatchGetViewsAtSK to batch rows
// stored at a non-default sort key.
func (repo *ViewRepository) BatchGetViews(ctx context.Context, pks []string) ([]*evt.SerializedView, error) {
	return repo.BatchGetViewsAtSK(ctx, pks, evt.DefaultViewSK)
}

// BatchGetViewsAtSK retrieves multiple views by partition key at the given sort
// key. It shares BatchGetViews' de-duplication, chunking, retry, and strong-read
// semantics; only the sort key differs. Callers reading rows that live at a custom
// sort key batch them through here instead of the DefaultViewSK shorthand.
func (repo *ViewRepository) BatchGetViewsAtSK(ctx context.Context, pks []string, sk string) ([]*evt.SerializedView, error) {
	keys := make([]map[string]types.AttributeValue, 0, len(pks))
	seen := make(map[string]struct{}, len(pks))
	for _, pk := range pks {
		if pk == "" {
			continue
		}
		if _, ok := seen[pk]; ok {
			continue
		}
		seen[pk] = struct{}{}
		keys = append(keys, map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
			"sk": &types.AttributeValueMemberS{Value: sk},
		})
	}
	if len(keys) == 0 {
		return nil, nil
	}

	views := make([]*evt.SerializedView, 0, len(keys))
	for start := 0; start < len(keys); start += maxBatchGetItems {
		end := start + maxBatchGetItems
		if end > len(keys) {
			end = len(keys)
		}

		chunk, err := repo.batchGetChunk(ctx, keys[start:end])
		if err != nil {
			return nil, err
		}

		views = append(views, chunk...)
	}

	return views, nil
}

// batchGetChunk issues a single BatchGetItem for the given keys (<= maxBatchGetItems)
// and retries UnprocessedKeys with backoff.
func (repo *ViewRepository) batchGetChunk(ctx context.Context, keys []map[string]types.AttributeValue) ([]*evt.SerializedView, error) {
	requestItems := map[string]types.KeysAndAttributes{
		repo.ViewsTable: {
			Keys:           keys,
			ConsistentRead: aws.Bool(true),
		},
	}

	views := make([]*evt.SerializedView, 0, len(keys))
	for attempt := 1; attempt <= maxBatchGetRetries; attempt++ {
		result, err := repo.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: requestItems})
		if err != nil {
			return nil, err
		}

		if result != nil {
			for _, item := range result.Responses[repo.ViewsTable] {
				var view View
				if unmarshalErr := repo.unmarshalMap(item, &view); unmarshalErr != nil {
					return nil, unmarshalErr
				}

				views = append(views, repo.toSerializedView(view))
			}
		}

		if result == nil || len(result.UnprocessedKeys) == 0 {
			return views, nil
		}

		if attempt == maxBatchGetRetries {
			return nil, fmt.Errorf("batch get returned unprocessed keys after %d attempts", attempt)
		}

		requestItems = result.UnprocessedKeys

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt*25) * time.Millisecond):
		}
	}

	return views, nil
}

// PutView writes/replaces a view in the table.
// If SK is empty, it defaults to "VIEW".
func (repo *ViewRepository) PutView(ctx context.Context, view *evt.SerializedView) error {
	item, err := repo.marshalViewItem(view)
	if err != nil {
		return err
	}

	// Use BatchWriteItem to persist the view through the shared DynamoDB client interface.
	return repo.writeBatch(ctx, []types.WriteRequest{{PutRequest: &types.PutRequest{Item: item}}})
}

// PutViews writes/replaces multiple views, coalescing them into BatchWriteItem
// requests of up to maxBatchWriteItems each and retrying UnprocessedItems. nil
// views are skipped. This lets hot-path callers that emit many views per record
// (e.g. one record fanning out to several views) collapse N single-item writes
// into ceil(N/25) round trips. If SK is empty on a view, it defaults to "VIEW".
func (repo *ViewRepository) PutViews(ctx context.Context, views []*evt.SerializedView) error {
	writes := make([]types.WriteRequest, 0, len(views))
	for _, view := range views {
		if view == nil {
			continue
		}

		item, err := repo.marshalViewItem(view)
		if err != nil {
			return err
		}

		writes = append(writes, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
	}

	for start := 0; start < len(writes); start += maxBatchWriteItems {
		end := start + maxBatchWriteItems
		if end > len(writes) {
			end = len(writes)
		}

		if err := repo.writeBatch(ctx, writes[start:end]); err != nil {
			return err
		}
	}

	return nil
}

// DeleteView removes a view row by primary key. If SK is empty, it defaults to "VIEW".
func (repo *ViewRepository) DeleteView(ctx context.Context, pk string, sk string) error {
	if sk == "" {
		sk = evt.DefaultViewSK
	}

	writeReq := types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: pk},
		"sk": &types.AttributeValueMemberS{Value: sk},
	}}}

	return repo.writeBatch(ctx, []types.WriteRequest{writeReq})
}

// writeBatch issues a single BatchWriteItem for the given requests and retries
// UnprocessedItems with backoff. Callers must keep len(writes) <= maxBatchWriteItems.
func (repo *ViewRepository) writeBatch(ctx context.Context, writes []types.WriteRequest) error {
	if len(writes) == 0 {
		return nil
	}

	input := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{repo.ViewsTable: writes}}
	for attempt := 1; attempt <= maxBatchWriteRetries; attempt++ {
		result, batchErr := repo.client.BatchWriteItem(ctx, input)
		if batchErr != nil {
			return batchErr
		}

		if result == nil || len(result.UnprocessedItems) == 0 {
			return nil
		}

		if attempt == maxBatchWriteRetries {
			return fmt.Errorf("batch write returned unprocessed items after %d attempts", attempt)
		}

		input = &dynamodb.BatchWriteItemInput{RequestItems: result.UnprocessedItems}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt*25) * time.Millisecond):
		}
	}

	return nil
}

// marshalViewItem encodes a serialized view into a Dynamo item map, applying the
// default sort key and preserving the TTL field. (MarshalViewToItem, used by the
// event-sourced projector path, intentionally omits TTL.)
func (repo *ViewRepository) marshalViewItem(view *evt.SerializedView) (map[string]types.AttributeValue, error) {
	sk := view.SK
	if sk == "" {
		sk = evt.DefaultViewSK
	}

	return repo.marshalMap(View{
		PK:         view.PK,
		SK:         sk,
		EntityID:   view.EntityID,
		EntityType: view.EntityType,
		Payload:    string(view.Payload),
		TTL:        view.TTL,
	})
}

// toSerializedView converts the Dynamo serialization back into the shared view type.
func (repo *ViewRepository) toSerializedView(view View) *evt.SerializedView {
	return &evt.SerializedView{
		PK:         view.PK,
		SK:         view.SK,
		EntityID:   view.EntityID,
		EntityType: view.EntityType,
		Payload:    []byte(view.Payload),
		TTL:        view.TTL,
	}
}

// ListViewsByEntityType queries the entity views table via a GSI for the provided entity type.
//
// It buffers the full result set; for large entity types use ListViewsByEntityTypePaged or
// ListViewsByEntityTypeEach instead.
func (repo *ViewRepository) ListViewsByEntityType(ctx context.Context, entityType evt.EntityType) ([]*evt.SerializedView, error) {
	views := make([]*evt.SerializedView, 0)

	err := repo.ListViewsByEntityTypeEach(ctx, entityType, func(view *evt.SerializedView) error {
		views = append(views, view)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return views, nil
}

// ListViewsByEntityTypeEach streams views for the provided entity type via the entityType GSI,
// invoking fn per view without buffering the whole result set.
func (repo *ViewRepository) ListViewsByEntityTypeEach(ctx context.Context, entityType evt.EntityType, fn func(*evt.SerializedView) error) error {
	input := dynamodb.QueryInput{
		TableName:              &repo.ViewsTable,
		IndexName:              aws.String(entityTypeIndexName),
		KeyConditionExpression: aws.String("entityType = :entityType"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":entityType": &types.AttributeValueMemberS{Value: string(entityType)},
		},
	}

	return repo.eachViewPage(ctx, &input, fn)
}

// ListViewsByEntityTypePaged queries by entity type with cursor-based pagination.
func (repo *ViewRepository) ListViewsByEntityTypePaged(ctx context.Context, entityType evt.EntityType, limit int, cursor string) (*evt.PagedResult, error) {
	input := dynamodb.QueryInput{
		TableName:              &repo.ViewsTable,
		IndexName:              aws.String(entityTypeIndexName),
		KeyConditionExpression: aws.String("entityType = :entityType"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":entityType": &types.AttributeValueMemberS{Value: string(entityType)},
		},
	}

	if limit > 0 && limit <= math.MaxInt32 {
		input.Limit = aws.Int32(int32(limit)) //nolint:gosec // bounds checked above
	}

	if cursor != "" {
		startKey, err := decodeCursor(cursor)
		if err != nil {
			return nil, fmt.Errorf("invalid pagination cursor: %w", err)
		}
		input.ExclusiveStartKey = startKey
	}

	result, err := repo.client.Query(ctx, &input)
	if err != nil {
		return nil, err
	}

	views := make([]*evt.SerializedView, 0, len(result.Items))
	for _, item := range result.Items {
		var view View
		if unmarshalErr := repo.unmarshalMap(item, &view); unmarshalErr != nil {
			return nil, unmarshalErr
		}

		views = append(views, repo.toSerializedView(view))
	}

	var nextCursor string
	if result.LastEvaluatedKey != nil {
		nextCursor, err = encodeCursor(result.LastEvaluatedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encode pagination cursor: %w", err)
		}
	}

	return &evt.PagedResult{
		Views:      views,
		NextCursor: nextCursor,
	}, nil
}

// encodeCursor encodes a DynamoDB LastEvaluatedKey as a base64 string.
func encodeCursor(key map[string]types.AttributeValue) (string, error) {
	// Serialize to a simple map of string values for the GSI keys
	raw := make(map[string]string)
	for k, v := range key {
		if sv, ok := v.(*types.AttributeValueMemberS); ok {
			raw[k] = sv.Value
		}
	}
	jsonBytes, err := json.Marshal(raw)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(jsonBytes), nil
}

// decodeCursor decodes a base64 pagination cursor back to a DynamoDB ExclusiveStartKey.
func decodeCursor(cursor string) (map[string]types.AttributeValue, error) {
	jsonBytes, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, err
	}
	var raw map[string]string
	if err := json.Unmarshal(jsonBytes, &raw); err != nil {
		return nil, err
	}
	key := make(map[string]types.AttributeValue, len(raw))
	for k, v := range raw {
		key[k] = &types.AttributeValueMemberS{Value: v}
	}
	return key, nil
}

// ListViewsByPK queries all views with the given partition key.
// Used for composite key tables where pk identifies a collection (e.g., USER#<id>#teams).
//
// It buffers the full result set; for partition keys with many rows use ListViewsByPKPaged or
// ListViewsByPKEach instead.
func (repo *ViewRepository) ListViewsByPK(ctx context.Context, pk string) ([]*evt.SerializedView, error) {
	views := make([]*evt.SerializedView, 0)

	err := repo.ListViewsByPKEach(ctx, pk, func(view *evt.SerializedView) error {
		views = append(views, view)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return views, nil
}

// ListViewsByPKEach streams views for the given partition key, invoking fn per view without
// buffering the whole result set.
func (repo *ViewRepository) ListViewsByPKEach(ctx context.Context, pk string, fn func(*evt.SerializedView) error) error {
	input := dynamodb.QueryInput{
		TableName:              &repo.ViewsTable,
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: pk},
		},
	}

	return repo.eachViewPage(ctx, &input, fn)
}

// eachViewPage paginates the given query and invokes fn for each decoded view. Iteration stops and
// the error is returned when fn returns an error, a page fails to load, or the context is cancelled.
func (repo *ViewRepository) eachViewPage(ctx context.Context, input *dynamodb.QueryInput, fn func(*evt.SerializedView) error) error {
	paginator := dynamodb.NewQueryPaginator(repo.client, input)

	for paginator.HasMorePages() {
		if err := ctx.Err(); err != nil {
			return err
		}

		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		if page == nil {
			continue
		}

		for _, item := range page.Items {
			var view View
			if err := repo.unmarshalMap(item, &view); err != nil {
				return err
			}

			if err := fn(repo.toSerializedView(view)); err != nil {
				return err
			}
		}
	}

	return nil
}

// ListViewsByPKPaged queries one partition key with a bounded result page.
func (repo *ViewRepository) ListViewsByPKPaged(ctx context.Context, pk string, limit int, cursor string) (*evt.PagedResult, error) {
	input := dynamodb.QueryInput{
		TableName:              &repo.ViewsTable,
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: pk},
		},
	}

	if limit > 0 && limit <= math.MaxInt32 {
		input.Limit = aws.Int32(int32(limit)) //nolint:gosec // bounds checked above
	}

	if cursor != "" {
		startKey, err := decodeCursor(cursor)
		if err != nil {
			return nil, fmt.Errorf("invalid pagination cursor: %w", err)
		}
		input.ExclusiveStartKey = startKey
	}

	result, err := repo.client.Query(ctx, &input)
	if err != nil {
		return nil, err
	}

	views := make([]*evt.SerializedView, 0, len(result.Items))
	for _, item := range result.Items {
		var view View
		if unmarshalErr := repo.unmarshalMap(item, &view); unmarshalErr != nil {
			return nil, unmarshalErr
		}

		views = append(views, repo.toSerializedView(view))
	}

	var nextCursor string
	if result.LastEvaluatedKey != nil {
		nextCursor, err = encodeCursor(result.LastEvaluatedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encode pagination cursor: %w", err)
		}
	}

	return &evt.PagedResult{
		Views:      views,
		NextCursor: nextCursor,
	}, nil
}

// marshalMap converts a struct into a DynamoDB attribute map using the repo encoder.
func (repo *ViewRepository) marshalMap(in any) (map[string]types.AttributeValue, error) {
	av, err := repo.encoder.Encode(in)
	if err != nil {
		return nil, err
	}
	if av == nil {
		return nil, fmt.Errorf("encoder returned nil AttributeValue")
	}
	asMap, ok := av.(*types.AttributeValueMemberM)
	if !ok {
		return nil, fmt.Errorf("encoder returned unexpected AttributeValue type: %T, expected *types.AttributeValueMemberM", av)
	}
	return asMap.Value, nil
}

// unmarshalMap decodes an attribute map into a struct.
func (repo *ViewRepository) unmarshalMap(value map[string]types.AttributeValue, out any) error {
	return repo.decoder.Decode(&types.AttributeValueMemberM{Value: value}, out)
}

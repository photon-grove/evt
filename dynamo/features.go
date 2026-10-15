package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
)

// Feature-marker row keys and attribute names, pinned by the storage spec (Feature Marker).
const (
	featuresPK      = evt.SystemNamespacePrefix + "FEATURES"
	featuresSK      = int64(0)
	featuresVersion = "version"
	featuresMap     = "features"
	featuresHistory = "historyDependent"
	featuresReten   = "retention"
)

// maxDeclareAttempts bounds the read-modify-write retry loop when two declarations race.
const maxDeclareAttempts = 8

// FeatureMarker reads, declares, and validates the feature manifest a store keeps on the reserved
// SYSTEM#FEATURES row (ADR-0004 C9). The manifest is durable, shared state: any writer that does
// not support a declared (feature, formatVersion) pair refuses to write to the table.
//
// A writer validates the feature set once, at first write, and caches the version it validated.
// Every commit then carries a ConditionCheck on the marker — `version = :v`, or
// `attribute_not_exists(pk)` when no marker row existed at validation — so a declaration bumps the
// version in the same conditional write that updates the feature set and every writer cached at
// the old version fails and re-validates.
type FeatureMarker struct {
	client      Client
	eventsTable string
	supported   evt.SupportedFeatures

	//mu guards the cached validation below; commits and declarations run concurrently.
	mu            sync.Mutex
	validated     bool
	hasMarker     bool  // whether the cached version came from an existing row
	cachedVersion int64 // the validated version; meaningful only when validated
}

// NewFeatureMarker constructs a FeatureMarker over the events table. The supported set declares
// the (feature, formatVersion) pairs this writer models; a nil or empty set supports no
// extensions, so a table that declares any feature refuses this writer.
func NewFeatureMarker(client Client, eventsTable string, supported evt.SupportedFeatures) *FeatureMarker {
	return &FeatureMarker{
		client:      client,
		eventsTable: eventsTable,
		supported:   supported,
	}
}

// Manifest reads the feature marker with a strongly consistent read and returns nil when the row
// does not exist.
func (m *FeatureMarker) Manifest(ctx context.Context) (*evt.FeatureManifest, error) {
	input := dynamodb.GetItemInput{
		TableName:      &m.eventsTable,
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: featuresPK},
			"sk": &types.AttributeValueMemberN{Value: strconv.FormatInt(featuresSK, 10)},
		},
	}

	out, err := m.client.GetItem(ctx, &input)
	if err != nil {
		return nil, fmt.Errorf("features: reading manifest: %w", err)
	}

	if len(out.Item) == 0 {
		return nil, nil
	}

	manifest, err := manifestFromItem(out.Item)
	if err != nil {
		return nil, fmt.Errorf("features: decoding manifest: %w", err)
	}

	return &manifest, nil
}

// manifestFromItem decodes a feature-marker item into a FeatureManifest. historyDependent and
// retention are absent when their set is empty.
func manifestFromItem(item map[string]types.AttributeValue) (evt.FeatureManifest, error) {
	version, ok := numAttr(item, featuresVersion)
	if !ok {
		return evt.FeatureManifest{}, fmt.Errorf("missing %s attribute", featuresVersion)
	}

	featuresText, err := stringAttr(item, featuresMap)
	if err != nil {
		return evt.FeatureManifest{}, err
	}

	historyText, err := stringAttr(item, featuresHistory)
	if err != nil {
		return evt.FeatureManifest{}, err
	}

	retentionText, err := stringAttr(item, featuresReten)
	if err != nil {
		return evt.FeatureManifest{}, err
	}

	manifest, err := evt.ParseFeatureManifest(featuresText, historyText, retentionText)
	if err != nil {
		return evt.FeatureManifest{}, err
	}

	manifest.Version = int64(version)

	return manifest, nil
}

// stringAttr reads a string attribute, treating absence as the empty text.
func stringAttr(item map[string]types.AttributeValue, name string) (string, error) {
	attr, ok := item[name].(*types.AttributeValueMemberS)
	if !ok {
		if _, present := item[name]; present {
			return "", fmt.Errorf("unexpected type for attribute %s", name)
		}

		return "", nil
	}

	return attr.Value, nil
}

// Declare applies update to the current manifest in a read-modify-write that bumps the version by
// exactly one. Creating the row asserts attribute_not_exists(pk) and writes version 1; updating
// asserts version = :observed. Two racing declarations retry the read-modify-write a bounded
// number of times.
func (m *FeatureMarker) Declare(
	ctx context.Context,
	update func(*evt.FeatureManifest),
) (evt.FeatureManifest, error) {
	for attempt := 0; ; attempt++ {
		manifest, err := m.Manifest(ctx)
		if err != nil {
			return evt.FeatureManifest{}, err
		}

		current := evt.FeatureManifest{Version: 1, Features: map[string]evt.FeatureFormat{}}
		observed := int64(0)
		created := manifest == nil
		if !created {
			observed = manifest.Version
			current = *manifest
		}

		update(&current)

		// Each declaration transaction adds exactly one to the version, no matter how many features
		// it adds or bumps.
		current.Version = observed + 1

		item, err := m.manifestItem(current)
		if err != nil {
			return evt.FeatureManifest{}, err
		}

		input := &dynamodb.PutItemInput{
			TableName: &m.eventsTable,
			Item:      item,
		}
		if created {
			input.ConditionExpression = aws.String("attribute_not_exists(pk)")
		} else {
			input.ConditionExpression = aws.String("version = :observed")
			input.ExpressionAttributeValues = map[string]types.AttributeValue{
				":observed": &types.AttributeValueMemberN{Value: strconv.FormatInt(manifest.Version, 10)},
			}
		}

		_, err = m.client.PutItem(ctx, input)
		if err == nil {
			m.invalidate()

			return current, nil
		}

		var condFailed *types.ConditionalCheckFailedException
		if errors.As(err, &condFailed) && attempt < maxDeclareAttempts {
			// Another declaration won the race; re-read and retry the read-modify-write.
			continue
		}

		return evt.FeatureManifest{}, fmt.Errorf("features: declaring manifest: %w", err)
	}
}

// manifestItem builds the full feature-marker item. historyDependent and retention are omitted
// when their set is empty; the caller has already set the version (1 on create, observed + 1 on
// update).
func (m *FeatureMarker) manifestItem(manifest evt.FeatureManifest) (map[string]types.AttributeValue, error) {
	featuresText, err := manifest.FeaturesJSON()
	if err != nil {
		return nil, err
	}

	item := map[string]types.AttributeValue{
		"pk":       &types.AttributeValueMemberS{Value: featuresPK},
		"sk":       &types.AttributeValueMemberN{Value: strconv.FormatInt(featuresSK, 10)},
		"version":  &types.AttributeValueMemberN{Value: strconv.FormatInt(manifest.Version, 10)},
		"features": &types.AttributeValueMemberS{Value: featuresText},
	}

	if len(manifest.HistoryDependent) > 0 {
		text, err := manifest.HistoryDependentJSON()
		if err != nil {
			return nil, err
		}

		item[featuresHistory] = &types.AttributeValueMemberS{Value: text}
	}

	if len(manifest.Retention) > 0 {
		text, err := manifest.RetentionJSON()
		if err != nil {
			return nil, err
		}

		item[featuresReten] = &types.AttributeValueMemberS{Value: text}
	}

	return item, nil
}

// Validate reads the manifest once, refuses an unsupported declared pair, and caches the version
// this writer validated. Later commits only condition on the cached version; a failed condition
// invalidates the cache (see invalidate) and the next commit re-validates.
func (m *FeatureMarker) Validate(ctx context.Context) error {
	m.mu.Lock()
	validated := m.validated
	m.mu.Unlock()

	if validated {
		return nil
	}

	manifest, err := m.Manifest(ctx)
	if err != nil {
		return err
	}

	if manifest == nil {
		// No marker row: nothing is declared; cache "no marker" and condition on absence.
		m.cacheVersion(0, false)

		return nil
	}

	if err := manifest.Validate(m.supported); err != nil {
		return err
	}

	m.cacheVersion(manifest.Version, true)

	return nil
}

// cacheVersion stores the validated version.
func (m *FeatureMarker) cacheVersion(version int64, hasMarker bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.validated = true
	m.hasMarker = hasMarker
	m.cachedVersion = version
}

// invalidate drops the cached validation so the next commit re-reads the manifest.
func (m *FeatureMarker) invalidate() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.validated = false
	m.hasMarker = false
	m.cachedVersion = 0
}

// checkItem builds the per-commit ConditionCheck on the marker: version = :v against the cached
// validation, or attribute_not_exists(pk) when no marker row existed at validation. Validate must
// have run first.
func (m *FeatureMarker) checkItem() (types.TransactWriteItem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.validated {
		return types.TransactWriteItem{}, errors.New("features: committing before validating the manifest")
	}

	check := types.ConditionCheck{
		TableName:           &m.eventsTable,
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: featuresPK},
			"sk": &types.AttributeValueMemberN{Value: strconv.FormatInt(featuresSK, 10)},
		},
	}

	if m.hasMarker {
		check.ConditionExpression = aws.String("attribute_not_exists(pk) OR version = :v")
		check.ExpressionAttributeValues = map[string]types.AttributeValue{
			":v": &types.AttributeValueMemberN{Value: strconv.FormatInt(m.cachedVersion, 10)},
		}
	}

	return types.TransactWriteItem{ConditionCheck: &check}, nil
}

// handleCheckFailure classifies a failed per-commit marker check: the manifest changed under the
// writer, so it re-validates immediately — an unsupported pair refuses the writer outright with
// UnsupportedFeatureError, and anything else is a transient revalidate-and-retry.
func (m *FeatureMarker) handleCheckFailure(ctx context.Context, err error) error {
	m.invalidate()

	if verr := m.Validate(ctx); verr != nil {
		return verr
	}

	return transientFeatureChanged(err.Error())
}

// transientFeatureChanged returns the transient error a failed marker check classifies as, so
// policy.ExecuteWithRetry retries with a re-validation.
func transientFeatureChanged(detail string) error {
	return &policy.ClassifiedError{
		Class: policy.ClassTransient,
		Err:   evt.FeatureManifestChanged(detail),
	}
}

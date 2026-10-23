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

	// mu guards the cached validation below; commits and declarations run concurrently.
	mu            sync.Mutex
	validated     bool
	hasMarker     bool  // whether the cached version came from an existing row
	cachedVersion int64 // the validated version; meaningful only when validated
}

// FeatureMarker constructs a marker using this repository's client and events table.
// The supported set declares the (feature, formatVersion) pairs this writer models;
// a nil or empty set supports no extensions.
func (repo *Repository) FeatureMarker(supported evt.SupportedFeatures) *FeatureMarker {
	return newFeatureMarker(repo.client, repo.EventsTable, supported)
}

func newFeatureMarker(client Client, eventsTable string, supported evt.SupportedFeatures) *FeatureMarker {
	return &FeatureMarker{
		client:      client,
		eventsTable: eventsTable,
		// Every writer of this format models stream-heads, whatever else the caller configured.
		supported: supported.WithStreamHeads(),
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
	declared, err := m.declare(ctx, update)
	if err != nil {
		return evt.FeatureManifest{}, err
	}

	m.invalidate()

	return declared, nil
}

// DeclareStreamHeads declares the stream-heads feature (evt.FeatureStreamHeads at the format this
// build writes) in the ordinary declaration transaction, without checking that the store holds
// no data. It is the migration's declaring step (storage spec, Stream Heads, Migration): the
// caller has backfilled and verified a head item for every stream with every older writer
// stopped, and declaring is what lets this build's writers start. A writer that finds a fresh
// store declares the feature itself; it never calls this.
func (m *FeatureMarker) DeclareStreamHeads(ctx context.Context) (evt.FeatureManifest, error) {
	return m.Declare(ctx, declareStreamHeads)
}

func declareStreamHeads(manifest *evt.FeatureManifest) {
	if manifest.Features == nil {
		manifest.Features = map[string]evt.FeatureFormat{}
	}

	manifest.Features[evt.FeatureStreamHeads] = evt.StreamHeadsFormat
}

// declare is Declare without touching the validation cache, for callers that hold the cache lock.
func (m *FeatureMarker) declare(
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

// StoreIsEmpty reports whether the table holds no non-reserved item: nothing but engine rows
// (the feature marker, the rebuild fence, command outcomes). It reads with a strongly consistent
// Scan until it finds one ordinary item or the table ends, so a populated table stops it on the
// first page, and an eventually consistent read cannot miss a just-written stream.
func (m *FeatureMarker) StoreIsEmpty(ctx context.Context) (bool, error) {
	pages := dynamodb.NewScanPaginator(m.client, &dynamodb.ScanInput{
		TableName:            &m.eventsTable,
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("pk"),
		Limit:                aws.Int32(scanProbePageSize),
	})

	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return false, fmt.Errorf("features: scanning for existing streams: %w", err)
		}

		for _, item := range page.Items {
			pk, ok := item["pk"].(*types.AttributeValueMemberS)
			if !ok || !evt.IsReservedEntityID(evt.EntityID(pk.Value)) {
				return false, nil
			}
		}
	}

	return true, nil
}

// scanProbePageSize bounds each page of the emptiness scan: it exists to find one item.
const scanProbePageSize = 100

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
//
// A store that does not declare stream-heads is refused with *evt.StreamHeadsNotMigratedError
// unless it holds no data. Validate only checks: a writer's first commit is what declares the
// feature on an empty store.
func (m *FeatureMarker) Validate(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.validateLocked(ctx, false)
}

// validateLocked reads and caches validation while mu is held. Besides refusing a declared pair
// this writer does not model, it requires the stream-heads feature (ADR 0007): a store that does not
// declare it holds streams with no head items, which a head-aware writer would fail on every
// stream, so it refuses with *evt.StreamHeadsNotMigratedError. A store that holds no data yet is
// declared by the writer itself when declare is set (a commit), so a fresh table needs no step;
// otherwise it passes without being written or cached.
func (m *FeatureMarker) validateLocked(ctx context.Context, declare bool) error {
	if m.validated {
		return nil
	}

	manifest, err := m.Manifest(ctx)
	if err != nil {
		return err
	}

	if manifest != nil {
		if err := manifest.Validate(m.supported); err != nil {
			return err
		}
	}

	if manifest == nil || !manifest.DeclaresStreamHeads() {
		empty, serr := m.StoreIsEmpty(ctx)
		if serr != nil {
			return serr
		}

		if !empty {
			return &evt.StreamHeadsNotMigratedError{}
		}

		if !declare {
			return nil
		}

		declared, derr := m.declareOnEmptyStore(ctx)
		if derr != nil {
			return derr
		}

		// A concurrent declaration may have added a feature this writer does not model.
		if verr := declared.Validate(m.supported); verr != nil {
			return verr
		}

		manifest = &declared
	}

	m.validated = true
	m.hasMarker = true
	m.cachedVersion = manifest.Version

	return nil
}

// declareOnEmptyStore declares stream-heads on a store that held no data when it was looked at,
// and closes the window between that look and the declaration.
//
// No transaction can exclude a writer that never reads the marker, so the order does the work:
// the declaration is committed first, which makes every writer that reads the marker fail its
// per-commit check or refuse the store from then on, and only then is the store looked at once
// more. A stream a marker-reading writer committed before the declaration is durable by then, so
// the second look sees it. A stream without a head item means the store was not fresh after all:
// the declaration is retracted (a new version, so every cached validation is dropped) and the
// store is refused as unmigrated. Streams other head-aware writers created in the meantime have a
// head item and are no reason to retract.
func (m *FeatureMarker) declareOnEmptyStore(ctx context.Context) (evt.FeatureManifest, error) {
	declared, err := m.declare(ctx, declareStreamHeads)
	if err != nil {
		return evt.FeatureManifest{}, err
	}

	headless, err := m.hasHeadlessStream(ctx)
	if err != nil {
		return evt.FeatureManifest{}, err
	}

	if !headless {
		return declared, nil
	}

	if _, err := m.declare(ctx, retractStreamHeads); err != nil {
		return evt.FeatureManifest{}, err
	}

	return evt.FeatureManifest{}, &evt.StreamHeadsNotMigratedError{}
}

func retractStreamHeads(manifest *evt.FeatureManifest) {
	delete(manifest.Features, evt.FeatureStreamHeads)
}

// hasHeadlessStream reports whether any stream in the table has items but no head item. A scan
// is not atomic across items, so a stream that looks headless is confirmed with a strongly
// consistent read of its head item before it counts.
func (m *FeatureMarker) hasHeadlessStream(ctx context.Context) (bool, error) {
	pages := dynamodb.NewScanPaginator(m.client, &dynamodb.ScanInput{
		TableName:            &m.eventsTable,
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("pk, sk"),
		Limit:                aws.Int32(scanProbePageSize),
	})

	headed := map[string]bool{}
	streams := map[string]bool{}

	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return false, fmt.Errorf("features: scanning for headless streams: %w", err)
		}

		for _, item := range page.Items {
			pk, ok := item["pk"].(*types.AttributeValueMemberS)
			if !ok || evt.IsReservedEntityID(evt.EntityID(pk.Value)) {
				continue
			}

			streams[pk.Value] = true

			if sk, isNumber := item["sk"].(*types.AttributeValueMemberN); isNumber && sk.Value == "-1" {
				headed[pk.Value] = true
			}
		}
	}

	for pk := range streams {
		if headed[pk] {
			continue
		}

		out, err := m.client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      &m.eventsTable,
			ConsistentRead: aws.Bool(true),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: pk},
				"sk": &types.AttributeValueMemberN{Value: "-1"},
			},
		})
		if err != nil {
			return false, fmt.Errorf("features: reading the head item of %s: %w", pk, err)
		}

		if len(out.Item) == 0 {
			return true, nil
		}
	}

	return false, nil
}

// invalidate drops the cached validation so the next commit re-reads the manifest.
func (m *FeatureMarker) invalidate() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.validated = false
	m.hasMarker = false
	m.cachedVersion = 0
}

// checkItem validates and builds the per-commit condition under one cache lock. A concurrent
// declaration may invalidate the cache afterward, but cannot change this check's validated version.
func (m *FeatureMarker) checkItem(ctx context.Context) (types.TransactWriteItem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.validateLocked(ctx, true); err != nil {
		return types.TransactWriteItem{}, err
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
// a retry re-validates the marker.
func transientFeatureChanged(detail string) error {
	return &policy.ClassifiedError{
		Class: policy.ClassTransient,
		Err:   evt.FeatureManifestChanged(detail),
	}
}

package evt

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// FeatureFormat is the stored-format version of a declared feature. A feature's stored format
// changes only by bumping this version, which re-fails every writer cached at the old version.
type FeatureFormat int64

// The features this storage format defines (ADR-0005). Both are Rust-first extensions: a writer
// that does not model them refuses to write to a table that declares either.
const (
	// FeatureJournal is the journal extension (X1).
	FeatureJournal = "journal"

	// FeatureJournalCompaction is the journal-compaction extension (X2).
	FeatureJournalCompaction = "journal-compaction"
)

// FeatureManifest is the parsed form of the feature-marker row a store keeps at
// SYSTEM#FEATURES (storage spec, Feature Marker; ADR-0004 C9). The manifest is durable, shared
// state: every writer and maintenance tool that opens the log observes the same declaration, so
// the refusals it drives hold across independently configured instances.
type FeatureManifest struct {
	// Version is bumped by exactly one on every declaration transaction.
	Version int64

	// Features maps a declared feature name to the stored-format version the declaring writer
	// models. Keys are sorted when encoded.
	Features map[string]FeatureFormat

	// HistoryDependent holds the entity types whose commit-wide projectors are history-dependent;
	// compaction is forbidden for them. Sorted when encoded; empty means unset.
	HistoryDependent []EntityType

	// Retention holds the entity types configured with retention (TTL); read fences are refused
	// for them. Sorted when encoded; empty means unset.
	Retention []EntityType
}

// SupportedFeatures declares the (feature, formatVersion) pairs a writer models. A writer may
// commit only if it supports every pair declared in the manifest: it models the feature and reads
// and writes its stored format at exactly the declared version. A nil or empty set supports no
// extensions, which is the Go v0.9 writer's default — the features this format defines are
// Rust-first.
type SupportedFeatures map[string]FeatureFormat

// Validate returns an UnsupportedFeatureError when the manifest declares a (name, formatVersion)
// pair the supported set does not model exactly.
func (m FeatureManifest) Validate(supported SupportedFeatures) error {
	var unsupported []UnsupportedPair

	for _, name := range sortedFeatureNames(m.Features) {
		declared := m.Features[name]

		modeled, ok := supported[name]
		if ok && modeled == declared {
			continue
		}

		unsupported = append(unsupported, UnsupportedPair{Name: name, Format: declared})
	}

	if len(unsupported) == 0 {
		return nil
	}

	return &UnsupportedFeatureError{Unsupported: unsupported}
}

// UnsupportedPair is one declared (feature, formatVersion) pair a writer does not model.
type UnsupportedPair struct {
	Name   string
	Format FeatureFormat
}

// UnsupportedFeatureError is returned when a writer encounters a declared feature pair it does not
// support: the writer reads the feature set and refuses to write.
type UnsupportedFeatureError struct {
	Unsupported []UnsupportedPair
}

// IsUnsupportedFeatureErr returns true when err is an UnsupportedFeatureError.
func IsUnsupportedFeatureErr(err error) bool {
	var unsupported *UnsupportedFeatureError
	return errors.As(err, &unsupported)
}

// Error returns the error message string.
func (e *UnsupportedFeatureError) Error() string {
	pairs := make([]string, 0, len(e.Unsupported))
	for _, pair := range e.Unsupported {
		pairs = append(pairs, fmt.Sprintf("%s@%d", pair.Name, pair.Format))
	}

	return fmt.Sprintf(
		"store declares unsupported features (%s); refusing to write",
		strings.Join(pairs, ", "),
	)
}

// HistoryDependentProjectionError is returned when compaction is refused for an entity type whose
// commit-wide projectors are history-dependent (declared in the feature manifest's
// historyDependent set). The refusal holds in every process that opens the log, whether or not it
// registered the projector.
type HistoryDependentProjectionError struct {
	EntityType EntityType
}

// NewHistoryDependentProjectionError creates a HistoryDependentProjectionError.
func NewHistoryDependentProjectionError(entityType EntityType) *HistoryDependentProjectionError {
	return &HistoryDependentProjectionError{EntityType: entityType}
}

// IsHistoryDependentProjectionErr returns true when err is a HistoryDependentProjectionError.
func IsHistoryDependentProjectionErr(err error) bool {
	var historyDependent *HistoryDependentProjectionError
	return errors.As(err, &historyDependent)
}

// Error returns the error message string.
func (e *HistoryDependentProjectionError) Error() string {
	return fmt.Sprintf(
		"entity type %q is declared history-dependent; compaction would delete the commits its rebuild replays",
		e.EntityType,
	)
}

// ErrFeatureManifestChanged is returned when a commit's feature-marker check fails: the manifest
// changed under the writer. The writer re-validates the feature set and retries the commit; a
// re-validation that finds an unsupported pair refuses with UnsupportedFeatureError instead.
var ErrFeatureManifestChanged = errors.New("feature manifest changed under this writer")

// FeatureManifestChanged returns the error a failed feature-marker check classifies as. It is
// transient (revalidate, then retry); wrapping callers should keep the sentinel matchable with
// errors.Is.
func FeatureManifestChanged(detail string) error {
	if detail == "" {
		return ErrFeatureManifestChanged
	}

	return fmt.Errorf("%w: %s", ErrFeatureManifestChanged, detail)
}

// FeaturesJSON encodes the manifest's feature map with the shared JSON rules: keys sorted, no
// insignificant whitespace. An empty map encodes as `{}`.
func (m FeatureManifest) FeaturesJSON() (string, error) {
	if m.Features == nil {
		m.Features = map[string]FeatureFormat{}
	}

	data, err := json.Marshal(m.Features)
	if err != nil {
		return "", fmt.Errorf("encoding feature manifest: %w", err)
	}

	return string(data), nil
}

// HistoryDependentJSON encodes the manifest's history-dependent entity types as a sorted JSON
// array with the shared JSON rules. Callers omit the stored attribute when the set is empty.
func (m FeatureManifest) HistoryDependentJSON() (string, error) {
	return entityTypeSetJSON(m.HistoryDependent)
}

// RetentionJSON encodes the manifest's retention entity types as a sorted JSON array with the
// shared JSON rules. Callers omit the stored attribute when the set is empty.
func (m FeatureManifest) RetentionJSON() (string, error) {
	return entityTypeSetJSON(m.Retention)
}

// entityTypeSetJSON sorts a copy of the set and encodes it as a JSON array of strings.
func entityTypeSetJSON(set []EntityType) (string, error) {
	sorted := make([]EntityType, len(set))
	copy(sorted, set)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	data, err := json.Marshal(sorted)
	if err != nil {
		return "", fmt.Errorf("encoding entity-type set: %w", err)
	}

	return string(data), nil
}

// ParseFeatureManifest decodes the stored JSON texts of a feature-marker row. Empty texts decode
// to empty sets, so a row that omits historyDependent or retention parses as the empty set.
func ParseFeatureManifest(features, historyDependent, retention string) (FeatureManifest, error) {
	manifest := FeatureManifest{Features: map[string]FeatureFormat{}}

	if features != "" {
		if err := json.Unmarshal([]byte(features), &manifest.Features); err != nil {
			return manifest, fmt.Errorf("decoding feature manifest: %w", err)
		}
	}

	var err error

	manifest.HistoryDependent, err = parseEntityTypeSet(historyDependent)
	if err != nil {
		return manifest, fmt.Errorf("decoding historyDependent set: %w", err)
	}

	manifest.Retention, err = parseEntityTypeSet(retention)
	if err != nil {
		return manifest, fmt.Errorf("decoding retention set: %w", err)
	}

	return manifest, nil
}

// parseEntityTypeSet decodes a stored JSON array of entity types; an empty text is the empty set.
func parseEntityTypeSet(text string) ([]EntityType, error) {
	if text == "" {
		return nil, nil
	}

	var set []EntityType
	if err := json.Unmarshal([]byte(text), &set); err != nil {
		return nil, err
	}

	return set, nil
}

// sortedFeatureNames returns the manifest's feature names sorted, so validation and encoding are
// deterministic.
func sortedFeatureNames(features map[string]FeatureFormat) []string {
	names := make([]string, 0, len(features))
	for name := range features {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

//! Reserved namespaces and the feature marker (v0.9, C8, C9).
//!
//! Two partition prefixes are reserved for engine-managed rows: [`SYSTEM_PREFIX`] (the feature
//! marker and the rebuild fence) and [`COMMAND_OUTCOME_PREFIX`] (command outcomes). Enumeration,
//! entity heads, and rebuild scans skip them, and every entity-addressed write path refuses an
//! entity ID that begins with one, before any request.
//!
//! A store declares the extensions it uses on a single [`FEATURES_ID`] marker row. Any writer
//! that does not support a declared `(name, formatVersion)` pair refuses to write to the table:
//! a writer validates the feature set once, at first write, caches the version it validated, and
//! every commit then carries a condition on that version. A declaration bumps `version` in the
//! same conditional write that updates the feature set, so every writer cached at the old version
//! fails its condition, re-validates, and refuses when the feature is not one it models.
//!
//! See `docs/reference/storage-format.md` ("Reserved Namespaces", "Feature Marker") for the stored
//! shapes; the manifest's JSON encoding is byte-identical across languages.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

use crate::error::Error;
use crate::ids::{EntityId, EntityType};
use crate::json;

/// The reserved `SYSTEM#` partition prefix (the feature marker, the rebuild fence).
pub const SYSTEM_PREFIX: &str = "SYSTEM#";

/// The reserved `command-outcome#` partition prefix (command outcomes).
pub const COMMAND_OUTCOME_PREFIX: &str = "command-outcome#";

/// The partition key of the feature-marker row.
pub const FEATURES_ID: &str = "SYSTEM#FEATURES";

/// The sort key of the feature-marker row.
pub const FEATURES_SORT_KEY: i64 = 0;

/// The feature name a table declares once every stream has a head item (ADR 0007, C12).
pub const STREAM_HEADS_FEATURE: &str = "stream-heads";

/// The stored-format version of [`STREAM_HEADS_FEATURE`] this build reads and writes.
pub const STREAM_HEADS_VERSION: i64 = 1;

/// The feature name a table declares once a writer journals its events (ADR 0005, X1).
pub const JOURNAL_FEATURE: &str = "journal";

/// The stored-format version of [`JOURNAL_FEATURE`] this build reads and writes when the `journal`
/// cargo feature is enabled.
pub const JOURNAL_VERSION: i64 = 1;

/// The feature name a table declares before its journal is first compacted (ADR 0005, X2).
pub const JOURNAL_COMPACTION_FEATURE: &str = "journal-compaction";

/// The stored-format version of [`JOURNAL_COMPACTION_FEATURE`] this build reads and writes when the
/// `journal` cargo feature is enabled.
pub const JOURNAL_COMPACTION_VERSION: i64 = 1;

/// The `(name, stored-format version)` pairs this build of the crate models. A writer refuses a
/// table whose manifest declares anything else. The extensions this spec defines (`journal`,
/// `journal-compaction`) are Rust-first: this build models `stream-heads`, which every writer here
/// also *requires* (see [`validate_marker_for_write`]), and, with the `journal` cargo feature,
/// `journal` and `journal-compaction` at format version 1. A build without the feature refuses a
/// journal table, so a writer that cannot maintain the journal cannot leave it short, and a build
/// that predates compaction refuses a compacted one.
#[cfg(not(feature = "journal"))]
pub const SUPPORTED_FEATURES: &[(&str, i64)] = &[(STREAM_HEADS_FEATURE, STREAM_HEADS_VERSION)];

/// The `(name, stored-format version)` pairs this build of the crate models. A writer refuses a
/// table whose manifest declares anything else. The extensions this spec defines (`journal`,
/// `journal-compaction`) are Rust-first: this build models `stream-heads`, which every writer here
/// also *requires* (see [`validate_marker_for_write`]), and, with the `journal` cargo feature,
/// `journal` and `journal-compaction` at format version 1. A build without the feature refuses a
/// journal table, so a writer that cannot maintain the journal cannot leave it short, and a build
/// that predates compaction refuses a compacted one.
#[cfg(feature = "journal")]
pub const SUPPORTED_FEATURES: &[(&str, i64)] = &[
    (JOURNAL_FEATURE, JOURNAL_VERSION),
    (JOURNAL_COMPACTION_FEATURE, JOURNAL_COMPACTION_VERSION),
    (STREAM_HEADS_FEATURE, STREAM_HEADS_VERSION),
];

/// Reports whether an entity ID falls in a reserved namespace: a case-sensitive byte prefix of
/// [`SYSTEM_PREFIX`] or [`COMMAND_OUTCOME_PREFIX`].
pub fn is_reserved(entity_id: &EntityId) -> bool {
    let id = entity_id.as_str();

    id.starts_with(SYSTEM_PREFIX) || id.starts_with(COMMAND_OUTCOME_PREFIX)
}

/// Refuses a reserved entity ID on a write path, before any request.
pub fn check_not_reserved(entity_id: &EntityId) -> Result<(), Error> {
    if is_reserved(entity_id) {
        return Err(Error::ReservedEntityId {
            entity_id: entity_id.clone(),
        });
    }

    Ok(())
}

/// The feature manifest stored on the marker row: the declared features with their stored-format
/// versions, and the durable `historyDependent` and `retention` entity-type sets.
///
/// The map and both sets encode as JSON with sorted keys, exactly as Go's `encoding/json` writes
/// them, so the stored bytes match across languages. An empty set is stored as an absent
/// attribute on DynamoDB and as `[]` in PostgreSQL.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FeatureManifest {
    /// Bumped by exactly one on every change to the manifest.
    pub version: i64,
    /// Declared feature name → its stored-format version.
    pub features: BTreeMap<String, i64>,
    /// Entity types whose commit-wide projectors are history-dependent; compaction is forbidden
    /// for them.
    pub history_dependent: BTreeSet<String>,
    /// Entity types configured with retention (TTL); fences are refused for them.
    pub retention: BTreeSet<String>,
}

impl FeatureManifest {
    /// Decodes a manifest from its stored attributes. `historyDependent` and `retention` are
    /// absent when their set is empty.
    pub fn from_attributes(
        version: i64,
        features: &str,
        history_dependent: Option<&str>,
        retention: Option<&str>,
    ) -> Result<Self, Error> {
        let features = json_text(features, "features")?;
        let history_dependent = optional_json(history_dependent, "historyDependent")?;
        let retention = optional_json(retention, "retention")?;

        Ok(Self {
            version,
            features,
            history_dependent,
            retention,
        })
    }

    /// The `features` attribute text: a JSON object with sorted keys.
    pub fn features_json(&self) -> String {
        json::to_string(&self.features).expect("a map of strings serializes")
    }

    /// The `historyDependent` attribute text: a sorted JSON array; `None` when empty.
    pub fn history_dependent_json(&self) -> Option<String> {
        set_json(&self.history_dependent)
    }

    /// The `retention` attribute text: a sorted JSON array; `None` when empty.
    pub fn retention_json(&self) -> Option<String> {
        set_json(&self.retention)
    }

    /// Reports whether the entity type appears in the durable `historyDependent` set, which
    /// forbids compaction for it.
    pub fn is_history_dependent(&self, entity_type: &EntityType) -> bool {
        self.history_dependent.contains(entity_type.as_str())
    }

    /// Reports whether the entity type appears in the durable `retention` set, which refuses
    /// fences for it.
    pub fn is_retained(&self, entity_type: &EntityType) -> bool {
        self.retention.contains(entity_type.as_str())
    }

    /// Reports whether the manifest declares [`STREAM_HEADS_FEATURE`] at [`STREAM_HEADS_VERSION`].
    pub fn declares_stream_heads(&self) -> bool {
        self.features.get(STREAM_HEADS_FEATURE) == Some(&STREAM_HEADS_VERSION)
    }

    /// Reports whether the manifest declares [`JOURNAL_FEATURE`] at [`JOURNAL_VERSION`].
    pub fn declares_journal(&self) -> bool {
        self.features.get(JOURNAL_FEATURE) == Some(&JOURNAL_VERSION)
    }

    /// Declares [`JOURNAL_FEATURE`] at [`JOURNAL_VERSION`]. The usual way to apply it is
    /// [`FeatureMarker::update_manifest`], which also bumps the manifest version.
    pub fn declare_journal(&mut self) {
        self.features
            .insert(JOURNAL_FEATURE.to_owned(), JOURNAL_VERSION);
    }

    /// Declares [`STREAM_HEADS_FEATURE`] at [`STREAM_HEADS_VERSION`]. The usual way to apply it is
    /// [`FeatureMarker::declare_stream_heads`], which also bumps the manifest version.
    pub fn declare_stream_heads(&mut self) {
        self.features
            .insert(STREAM_HEADS_FEATURE.to_owned(), STREAM_HEADS_VERSION);
    }

    /// Refuses the manifest when it declares a `(name, formatVersion)` pair this build does not
    /// support.
    pub fn validate_supported(&self) -> Result<(), Error> {
        for (name, version) in &self.features {
            if !SUPPORTED_FEATURES
                .iter()
                .any(|(supported, at)| supported == name && at == version)
            {
                return Err(Error::UnsupportedFeature {
                    feature: name.clone(),
                    version: *version,
                });
            }
        }

        Ok(())
    }
}

fn set_json(set: &BTreeSet<String>) -> Option<String> {
    if set.is_empty() {
        return None;
    }

    Some(json::to_string(set).expect("a set of strings serializes"))
}

fn json_text<T: serde::de::DeserializeOwned>(text: &str, what: &str) -> Result<T, Error> {
    json::from_slice(text.as_bytes())
        .map_err(|err| Error::InvalidRecord(format!("feature manifest {what}: {err}")))
}

fn optional_json<T: Default + serde::de::DeserializeOwned>(
    text: Option<&str>,
    what: &str,
) -> Result<T, Error> {
    match text.filter(|text| !text.trim().is_empty()) {
        Some(text) => json_text(text, what),
        None => Ok(Default::default()),
    }
}

/// The marker state a writer validated: the row's version, or that the table had no marker row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValidatedMarker {
    /// The writer validated a table with no marker row.
    Absent,
    /// The writer validated the marker row at this version.
    Version(i64),
}

/// A writer's cached marker state: not yet validated, or the result of the first validation.
pub type CachedMarker = Option<ValidatedMarker>;

/// Reports whether the observed manifest row still matches the writer's cached validation.
/// A row that changed version, appeared, or disappeared must be re-validated.
pub fn marker_matches(validated: CachedMarker, observed: Option<&FeatureManifest>) -> bool {
    match (validated, observed) {
        (Some(ValidatedMarker::Absent), None) => true,
        (Some(ValidatedMarker::Version(version)), Some(manifest)) => manifest.version == version,
        _ => false,
    }
}

/// Validates a freshly observed manifest for a *write path* (a commit, a snapshot write, or
/// compaction) and returns the state to cache. On top of [`validate_marker`] it requires the table
/// to declare `stream-heads` at format version 1 (ADR 0007): a missing row or a row without the
/// feature is [`Error::StreamHeadsNotMigrated`]. A manifest that declares a pair this build does
/// not support is refused first, with [`Error::UnsupportedFeature`].
///
/// A backend that finds the feature missing either refuses with [`Error::StoreNotInitialized`]
/// (DynamoDB by default) or initializes the store itself, but only one it has confirmed holds no
/// data (see [`FeatureMarker::initialize`], and [`FeatureMarker::declare_stream_heads`] for the
/// unconditional declaration a migration performs).
pub fn validate_marker_for_write(
    observed: Option<&FeatureManifest>,
) -> Result<ValidatedMarker, Error> {
    let Some(manifest) = observed else {
        return Err(Error::StreamHeadsNotMigrated);
    };

    manifest.validate_supported()?;
    if !manifest.declares_stream_heads() {
        return Err(Error::StreamHeadsNotMigrated);
    }

    Ok(ValidatedMarker::Version(manifest.version))
}

/// A repository that stores the feature-marker row ([`FEATURES_ID`]). Every backend implements it,
/// so the declaration and refusal behavior is testable on the in-memory backend and enforced on
/// the live ones.
pub trait FeatureMarker: Send + Sync {
    /// Reads the manifest row, if one exists. Strongly consistent where the backend
    /// distinguishes, so a declaration another writer just wrote cannot be missed.
    fn read_manifest(&self) -> impl Future<Output = Result<Option<FeatureManifest>, Error>> + Send;

    /// Applies `change` to the stored manifest in one conditional write that bumps `version` by
    /// exactly one, no matter how many entries the change touches. Declaring does not require
    /// support: the refusal binds data writes (commits, [`crate::Repository::put_snapshot`],
    /// [`crate::Repository::compact_below`]), and this is how a table is prepared for, or tested
    /// against, a feature this build does not model.
    fn update_manifest(
        &self,
        change: impl Fn(&mut FeatureManifest) + Send,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Declares `stream-heads: 1` through the ordinary declaration transaction (one version
    /// bump), **without** checking that the table is empty or that every stream has a head item.
    ///
    /// This is the hook for a migration tool (`evt-doctor migrate-heads`): after it has backfilled
    /// and verified a head item for every stream, with every v0.8 writer stopped, it declares the
    /// feature with this call. Calling it on a populated table whose streams have no head items
    /// makes every later commit to those streams fail with [`Error::StreamHeadDrift`]. A fresh
    /// store is declared with [`initialize`](Self::initialize) instead, which verifies it first.
    fn declare_stream_heads(&self) -> impl Future<Output = Result<(), Error>> + Send {
        self.update_manifest(FeatureManifest::declare_stream_heads)
    }

    /// Reports whether a write to a store that does not declare `stream-heads` is refused with
    /// [`Error::StoreNotInitialized`] (DynamoDB's default), rather than initialized by the writer
    /// itself. A wrapper that declares extensions on the marker consults this before declaring,
    /// so a store its writer would refuse is refused without any declaration of its own. The
    /// default is `false`: backends keep their first-write initialization unless they say
    /// otherwise.
    fn refuses_uninitialized(&self) -> bool {
        false
    }

    /// Initializes the store: declares `stream-heads` on a store that holds no data, with the
    /// verification ADR 0007 requires, and returns whether this call declared it.
    ///
    /// It is an operator step, run once at deploy time before any writer starts (`evt-doctor init`
    /// runs it), and it is idempotent: a store that already declares the feature is left alone
    /// and returns `Ok(false)` after one manifest read, with nothing scanned or written. A store
    /// that does not declare it must hold no item outside the reserved namespaces, or the call
    /// refuses with [`Error::StreamHeadsNotMigrated`] (migrate it with `evt-doctor migrate-heads`
    /// instead). The declaration is the ordinary declaration transaction, and on DynamoDB the
    /// table is then looked at once more for a stream with no head item, retracting the
    /// declaration and refusing if one appeared in the window. A manifest that declares a feature
    /// this build does not support is refused with [`Error::UnsupportedFeature`]. Only
    /// `stream-heads` is declared: extensions such as `journal` declare themselves on first use,
    /// which needs no scan.
    ///
    /// On DynamoDB the verification is a strongly consistent Scan, so the caller needs
    /// `dynamodb:Scan` on the event log; writers, which refuse an uninitialized store with
    /// [`Error::StoreNotInitialized`] by default, then never scan.
    fn initialize(&self) -> impl Future<Output = Result<bool, Error>> + Send;
}

/// Durably adds the entity types to the manifest's `historyDependent` set, once: types already
/// declared are left alone and nothing is written. It is a repository's
/// [`declare_history_dependent`](crate::Repository::declare_history_dependent).
pub(crate) async fn declare_history<M: FeatureMarker>(
    marker: &M,
    types: &[EntityType],
) -> Result<(), Error> {
    let manifest = marker.read_manifest().await?;
    let missing: Vec<String> = types
        .iter()
        .filter(|entity_type| {
            !manifest
                .as_ref()
                .is_some_and(|manifest| manifest.is_history_dependent(entity_type))
        })
        .map(|entity_type| entity_type.as_str().to_owned())
        .collect();

    if missing.is_empty() {
        return Ok(());
    }

    marker
        .update_manifest(move |manifest| {
            manifest.history_dependent.extend(missing.iter().cloned());
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_prefixes_match_bytewise() {
        assert!(is_reserved(&EntityId::new("SYSTEM#FEATURES")));
        assert!(is_reserved(&EntityId::new("SYSTEM#account")));
        assert!(is_reserved(&EntityId::new("command-outcome#cmd-1")));
        assert!(!is_reserved(&EntityId::new("system#lowercase")));
        assert!(!is_reserved(&EntityId::new("command-outcome")));
        assert!(!is_reserved(&EntityId::new("acct-1")));
        assert!(!is_reserved(&EntityId::new("")));
    }

    #[test]
    fn encodes_the_spec_example() {
        let mut manifest = FeatureManifest {
            version: 3,
            ..FeatureManifest::default()
        };
        manifest.features.insert("journal".into(), 1);
        manifest.features.insert("journal-compaction".into(), 1);
        manifest.history_dependent.insert("audit-feed".into());
        manifest.retention.insert("session".into());

        assert_eq!(
            manifest.features_json(),
            r#"{"journal":1,"journal-compaction":1}"#
        );
        assert_eq!(
            manifest.history_dependent_json().as_deref(),
            Some(r#"["audit-feed"]"#)
        );
        assert_eq!(manifest.retention_json().as_deref(), Some(r#"["session"]"#));
    }

    #[test]
    fn empty_sets_are_absent() {
        let manifest = FeatureManifest {
            version: 1,
            features: BTreeMap::from([("journal".to_owned(), 2)]),
            ..FeatureManifest::default()
        };

        assert_eq!(manifest.features_json(), r#"{"journal":2}"#);
        assert_eq!(manifest.history_dependent_json(), None);
        assert_eq!(manifest.retention_json(), None);
    }

    #[test]
    fn decodes_the_spec_example() {
        let manifest = FeatureManifest::from_attributes(
            3,
            r#"{"journal":1,"journal-compaction":1}"#,
            Some(r#"["audit-feed"]"#),
            Some(r#"["session"]"#),
        )
        .unwrap();

        assert_eq!(manifest.version, 3);
        assert_eq!(manifest.features.get("journal"), Some(&1));
        assert!(manifest.is_history_dependent(&"audit-feed".into()));
        assert!(manifest.is_retained(&"session".into()));
        assert_eq!(
            manifest.features_json(),
            r#"{"journal":1,"journal-compaction":1}"#
        );
    }

    #[test]
    fn empty_attribute_text_decodes_to_an_empty_set() {
        let manifest = FeatureManifest::from_attributes(1, "{}", Some(""), None).unwrap();

        assert!(manifest.features.is_empty());
        assert!(manifest.history_dependent.is_empty());
        assert!(manifest.retention.is_empty());
    }

    #[test]
    fn malformed_manifest_attributes_are_invalid_records() {
        for (features, history, retention) in [
            ("{", None, None),
            ("{}", Some("["), None),
            ("{}", None, Some(r#"["a""#)),
        ] {
            assert!(
                FeatureManifest::from_attributes(1, features, history, retention).is_err(),
                "{features:?}, {history:?}, {retention:?}"
            );
        }
    }

    #[test]
    fn refuses_unsupported_declared_features() {
        let mut manifest = FeatureManifest::default();
        manifest.features.insert("journal-compaction".into(), 2);
        assert!(matches!(
            manifest.validate_supported(),
            Err(Error::UnsupportedFeature { feature, version })
                if feature == "journal-compaction" && version == 2
        ));

        // The declared version matters, not just the name: this build models stream-heads at
        // version 1 and refuses version 2.
        assert!(FeatureManifest::default().validate_supported().is_ok());

        let mut future = FeatureManifest::default();
        future.features.insert(STREAM_HEADS_FEATURE.to_owned(), 2);
        assert!(matches!(
            future.validate_supported(),
            Err(Error::UnsupportedFeature { feature, version: 2 }) if feature == STREAM_HEADS_FEATURE
        ));
    }

    #[test]
    fn the_journal_is_modeled_only_with_its_cargo_feature() {
        let mut manifest = FeatureManifest::default();
        manifest.declare_journal();
        assert!(manifest.declares_journal());
        assert_eq!(manifest.features_json(), r#"{"journal":1}"#);

        if cfg!(feature = "journal") {
            assert!(manifest.validate_supported().is_ok());
        } else {
            assert!(matches!(
                manifest.validate_supported(),
                Err(Error::UnsupportedFeature { feature, version: 1 }) if feature == "journal"
            ));
        }

        // The format version matters: a newer journal is refused by every build.
        let mut newer = FeatureManifest::default();
        newer.features.insert(JOURNAL_FEATURE.to_owned(), 2);
        assert!(!newer.declares_journal());
        assert!(matches!(
            newer.validate_supported(),
            Err(Error::UnsupportedFeature { version: 2, .. })
        ));
    }

    #[test]
    fn writers_require_the_stream_heads_feature() {
        // No marker row, or a row without the feature: not migrated.
        assert!(matches!(
            validate_marker_for_write(None),
            Err(Error::StreamHeadsNotMigrated)
        ));

        let mut manifest = FeatureManifest {
            version: 2,
            ..FeatureManifest::default()
        };
        assert!(matches!(
            validate_marker_for_write(Some(&manifest)),
            Err(Error::StreamHeadsNotMigrated)
        ));

        manifest.declare_stream_heads();
        assert_eq!(
            manifest.features_json(),
            r#"{"stream-heads":1}"#,
            "the canonical encoding of the declaration"
        );
        assert!(matches!(
            validate_marker_for_write(Some(&manifest)),
            Ok(ValidatedMarker::Version(2))
        ));

        // An unsupported pair is refused first, whatever else is missing.
        let mut unsupported = FeatureManifest::default();
        unsupported.features.insert("journal-compaction".into(), 2);
        assert!(matches!(
            validate_marker_for_write(Some(&unsupported)),
            Err(Error::UnsupportedFeature { .. })
        ));
    }

    #[test]
    fn marker_checks_compare_versions() {
        let manifest = FeatureManifest {
            version: 4,
            ..FeatureManifest::default()
        };

        assert!(marker_matches(
            Some(ValidatedMarker::Version(4)),
            Some(&manifest)
        ));
        assert!(!marker_matches(
            Some(ValidatedMarker::Version(3)),
            Some(&manifest)
        ));
        assert!(!marker_matches(
            Some(ValidatedMarker::Absent),
            Some(&manifest)
        ));
        assert!(marker_matches(Some(ValidatedMarker::Absent), None));
        assert!(!marker_matches(Some(ValidatedMarker::Version(4)), None));
        assert!(
            !marker_matches(None, None),
            "an unvalidated writer revalidates"
        );
    }
}

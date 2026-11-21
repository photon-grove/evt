// The feature manifest a store keeps on its SYSTEM#FEATURES marker row (storage format, "Feature
// marker"): the extensions it declares, each at a stored-format version, and the entity types
// declared history-dependent or retained. A writer refuses a store that declares a pair it doesn't
// model, and every writer requires stream-heads.

import {
  EvtError,
  type FeaturePair,
  StreamHeadsNotMigratedError,
  UnsupportedFeatureError,
} from './errors.js'
import {DOCUMENT, int64, invalid} from './fields.js'
import {type JsonValue, parseJson, stringifyJson} from './json.js'
import type {CallOptions} from './repository.js'
import {compareUtf8} from './utf8.js'

/** The feature every writer models and requires: per-stream head items (ADR 0007). */
export const STREAM_HEADS_FEATURE = 'stream-heads'

/** The stored-format version of {@link STREAM_HEADS_FEATURE} this package reads and writes. */
export const STREAM_HEADS_VERSION = 1

/**
 * The `(feature, stored-format version)` pairs this package models. The other features the
 * storage format defines (`journal`, `journal-compaction`, `postgres-views`) are Rust-first, so a
 * store that declares any of them is refused.
 */
export const SUPPORTED_FEATURES: readonly FeaturePair[] = Object.freeze([
  Object.freeze({name: STREAM_HEADS_FEATURE, version: STREAM_HEADS_VERSION}),
])

// What validation reads. Private, so no caller can add a pair to it.
const supported = new Map(SUPPORTED_FEATURES.map((pair) => [pair.name, pair.version]))

/** The journal's feature (ADR 0005), which only a journal writer accepts. */
export const JOURNAL_FEATURE = 'journal'

/** The stored-format version of {@link JOURNAL_FEATURE} this package reads and writes. */
export const JOURNAL_VERSION = 1

/** Journal compaction's feature, which only a journal writer accepts. */
export const JOURNAL_COMPACTION_FEATURE = 'journal-compaction'

/** The stored-format version of {@link JOURNAL_COMPACTION_FEATURE} this package writes. */
export const JOURNAL_COMPACTION_VERSION = 1

// The pairs a journal writer accepts besides the supported ones. Not journal compaction yet: a
// writer that doesn't model it would read compacted rows as a gap, and could allocate below the
// floor.
const journalPairs = new Map([[JOURNAL_FEATURE, JOURNAL_VERSION]])

// The repository handles a journal repository wraps (ADR 0005, #178). Every writer of a journal
// table must commit through the journal, or its events are missing from their scopes with no gap
// to show it, so acceptance is per handle: every other handle refuses a store that declares the
// journal. Private, so only the journal adds to it.
const journalWriters = new WeakSet<object>()

/**
 * The internal hook of a wrapper that sits under a journal: the repository it wraps, which a
 * journal writer's acceptance passes down to.
 */
export const JOURNAL_WRITER_TARGET: unique symbol = Symbol('evt.journalWriterTarget')

/**
 * Makes a repository handle a journal writer: it accepts a store that declares the journal, which
 * every other handle refuses. A wrapper's acceptance passes down to the
 * repository it wraps. Internal: the journal's repository calls it.
 */
export function acceptJournal(repository: object): void {
  journalWriters.add(repository)
  const target = (repository as {[JOURNAL_WRITER_TARGET]?: () => object})[JOURNAL_WRITER_TARGET]
  if (typeof target === 'function') {
    acceptJournal(target.call(repository))
  }
}

/** Whether a repository handle is a journal writer. */
export function isJournalWriter(repository: object): boolean {
  return journalWriters.has(repository)
}

/** Refuses a handle that isn't a journal writer: only a journal declares or retracts it. */
export function checkJournalWriter(repository: object): void {
  if (!journalWriters.has(repository)) {
    throw new UnsupportedFeatureError([{name: JOURNAL_FEATURE, version: JOURNAL_VERSION}])
  }
}

/** The parsed marker row. A store with no marker row has no manifest. */
export interface FeatureManifest {
  /** Bumped by exactly one on every declaration. */
  readonly version: bigint
  /** Each declared feature's stored-format version. */
  readonly features: ReadonlyMap<string, number>
  /** The entity types whose commit-wide projectors are history-dependent: never compacted. */
  readonly historyDependent: readonly string[]
  /** The entity types configured with retention: never fenced. */
  readonly retention: readonly string[]
}

/**
 * Refuses a manifest that declares a pair this package doesn't model, listing every such pair,
 * with an {@link UnsupportedFeatureError}. A journal writer (`journaling`) also accepts the
 * journal's pairs.
 */
export function checkSupported(manifest: FeatureManifest | null, journaling = false): void {
  const unsupported: FeaturePair[] = []
  for (const [name, version] of sortedFeatures(manifest?.features ?? new Map())) {
    const accepted = supported.get(name) ?? (journaling ? journalPairs.get(name) : undefined)
    if (accepted !== version) {
      unsupported.push({name, version})
    }
  }

  if (unsupported.length > 0) {
    throw new UnsupportedFeatureError(unsupported)
  }
}

/** Whether the manifest declares stream-heads at the version this package models. */
export function declaresStreamHeads(manifest: FeatureManifest | null): boolean {
  return manifest?.features.get(STREAM_HEADS_FEATURE) === STREAM_HEADS_VERSION
}

/**
 * Refuses a write to a store whose manifest declares an unsupported pair, or doesn't declare
 * stream-heads, without declaring anything.
 */
export function checkWritable(manifest: FeatureManifest | null, journaling = false): void {
  checkSupported(manifest, journaling)
  if (!declaresStreamHeads(manifest)) {
    throw new StreamHeadsNotMigratedError()
  }
}

/** The manifest with stream-heads declared and its version bumped: a first writer's declaration. */
export function withStreamHeads(manifest: FeatureManifest | null): FeatureManifest {
  const features = new Map(manifest?.features)
  features.set(STREAM_HEADS_FEATURE, STREAM_HEADS_VERSION)

  return {
    version: (manifest?.version ?? 0n) + 1n,
    features,
    historyDependent: manifest?.historyDependent ?? [],
    retention: manifest?.retention ?? [],
  }
}

/**
 * The entity types not yet in the manifest's historyDependent set, in order and without
 * duplicates. A store with no manifest declares nothing, so every type is missing.
 */
export function missingHistoryDependent(
  manifest: FeatureManifest | null,
  types: readonly string[],
): string[] {
  const declared = new Set(manifest?.historyDependent)
  const missing: string[] = []
  for (const entityType of types) {
    if (!declared.has(entityType) && !missing.includes(entityType)) {
      missing.push(entityType)
    }
  }

  return missing
}

/** The manifest with the entity types added to its historyDependent set and its version bumped. */
export function withHistoryDependent(
  manifest: FeatureManifest | null,
  types: readonly string[],
): FeatureManifest {
  return {
    version: (manifest?.version ?? 0n) + 1n,
    features: new Map(manifest?.features),
    historyDependent: entityTypeSet([...(manifest?.historyDependent ?? []), ...types]),
    retention: manifest?.retention ?? [],
  }
}

/**
 * The internal capability of a repository the journal can write through: its own declarations on
 * the store's feature manifest. Symbol-keyed, so it's no part of the repository's public API.
 */
export const JOURNAL_MARKER: unique symbol = Symbol('evt.journalMarker')

/** What {@link JOURNAL_MARKER} names. */
export interface JournalMarker {
  /**
   * Validates the store as a commit would (an uninitialized store is refused, or, initializing
   * lazily, declared, before anything else), then declares the journal feature and then the entity
   * types history-dependent, a declaration each, as Rust does, and resolves to what it wrote:
   * nothing when the manifest declares it all. A manifest that declares the journal at another
   * version is refused with an `UnsupportedFeatureError`, and so is a handle that isn't a journal
   * writer.
   */
  declareJournal(types: readonly string[], options?: CallOptions): Promise<JournalDeclared>
  /** Takes back what a declaration wrote, in one declaration. Best effort: the caller ignores failures. */
  retractJournal(declared: JournalDeclared, options?: CallOptions): Promise<void>
}

/** Whether a declaration wrote anything. */
export function declaredAnything(declared: JournalDeclared): boolean {
  return declared.journal || declared.history.length > 0
}

/** What one journal declaration wrote, which a commit refused as uninitialized takes back. */
export interface JournalDeclared {
  /** The journal feature, declared by this declaration. */
  readonly journal: boolean
  /** The entity types this declaration added to the historyDependent set. */
  readonly history: readonly string[]
}

/** What a journal declaration would write to `manifest`: nothing when it declares it all. */
export function journalDeclaration(
  manifest: FeatureManifest | null,
  types: readonly string[],
): JournalDeclared {
  return {
    journal: manifest?.features.get(JOURNAL_FEATURE) !== JOURNAL_VERSION,
    history: missingHistoryDependent(manifest, types),
  }
}

/** The manifest with a journal declaration applied, its version bumped once. */
export function withJournal(
  manifest: FeatureManifest | null,
  declared: JournalDeclared,
): FeatureManifest {
  const features = new Map(manifest?.features)
  if (declared.journal) {
    features.set(JOURNAL_FEATURE, JOURNAL_VERSION)
  }

  return {
    version: (manifest?.version ?? 0n) + 1n,
    features,
    historyDependent: entityTypeSet([...(manifest?.historyDependent ?? []), ...declared.history]),
    retention: manifest?.retention ?? [],
  }
}

/** The manifest with a journal declaration taken back, its version bumped once. */
export function withoutJournal(
  manifest: FeatureManifest | null,
  declared: JournalDeclared,
): FeatureManifest {
  const features = new Map(manifest?.features)
  if (declared.journal) {
    features.delete(JOURNAL_FEATURE)
  }
  const history = new Set(declared.history)

  return {
    version: (manifest?.version ?? 0n) + 1n,
    features,
    historyDependent: (manifest?.historyDependent ?? []).filter((type) => !history.has(type)),
    retention: manifest?.retention ?? [],
  }
}

/** A manifest's stored JSON texts, as the marker row holds them. */
export interface ManifestTexts {
  /** A JSON object, feature name to stored-format version, keys sorted. `{}` when empty. */
  features: string
  /** A JSON array of entity types, sorted by UTF-8 bytes and deduplicated. `[]` when empty. */
  historyDependent: string
  /** Like `historyDependent`. */
  retention: string
}

/** Encodes a manifest's declarable parts as the marker row stores them. */
export function encodeManifest(manifest: FeatureManifest): ManifestTexts {
  return {
    features: stringifyJson(new Map(manifest.features)),
    historyDependent: stringifyJson(entityTypeSet(manifest.historyDependent)),
    retention: stringifyJson(entityTypeSet(manifest.retention)),
  }
}

/**
 * Decodes a marker row's stored texts. An empty text is an empty map or set, as an absent
 * attribute is.
 */
export function decodeManifest(version: bigint, texts: ManifestTexts): FeatureManifest {
  return {
    version,
    features: texts.features === '' ? new Map() : parseFeatures(texts.features),
    historyDependent: parseEntityTypes(texts.historyDependent, 'historyDependent'),
    retention: parseEntityTypes(texts.retention, 'retention'),
  }
}

/** An entity-type set in its stored order: sorted by UTF-8 bytes and deduplicated. */
export function entityTypeSet(types: readonly string[]): string[] {
  // A fresh array, sorted in place: ES2022 has no toSorted.
  const set = Array.from(new Set(types))
  set.sort(compareUtf8)

  return set
}

function parseFeatures(text: string): Map<string, number> {
  const parsed = parseStored(text, 'features')
  if (!(parsed instanceof Map)) {
    throw invalid('the feature manifest is not a JSON object')
  }

  // Go and Rust read a version as an int64. One past the safe integers is no version this
  // package models, so rounding it changes only the refusal's text.
  const features = new Map<string, number>()
  for (const [name, version] of parsed) {
    features.set(name, Number(int64(version, `the feature manifest's ${name} version`)))
  }

  return features
}

function parseEntityTypes(text: string, what: string): string[] {
  if (text.trim() === '') {
    return []
  }

  const parsed = parseStored(text, what)
  if (!Array.isArray(parsed) || parsed.some((entry) => typeof entry !== 'string')) {
    throw invalid(`the ${what} set is not a JSON array of strings`)
  }

  return entityTypeSet(parsed as string[])
}

function parseStored(text: string, what: string): JsonValue {
  try {
    return parseJson(text, DOCUMENT)
  } catch (error) {
    throw new EvtError('invalidRecord', `the stored ${what} is not JSON`, {cause: error})
  }
}

function sortedFeatures(features: ReadonlyMap<string, number>): [string, number][] {
  const sorted = Array.from(features)
  sorted.sort(([a], [b]) => compareUtf8(a, b))

  return sorted
}

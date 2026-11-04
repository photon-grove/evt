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
 * with an {@link UnsupportedFeatureError}.
 */
export function checkSupported(manifest: FeatureManifest | null): void {
  const unsupported: FeaturePair[] = []
  for (const [name, version] of sortedFeatures(manifest?.features ?? new Map())) {
    if (supported.get(name) !== version) {
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
export function checkWritable(manifest: FeatureManifest | null): void {
  checkSupported(manifest)
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

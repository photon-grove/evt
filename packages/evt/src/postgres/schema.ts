// The PostgreSQL layout of an event log, independent of any driver (storage format, "PostgreSQL"):
// the table names a store is configured with, the engine relations derived from them, and the
// idempotent DDL that Go's EnsureSchema and Rust's ensure_schema run.

import {EvtError} from '../errors.js'

/** The default events table. */
export const DEFAULT_EVENTS_TABLE = 'evt_events'

/** The default snapshots table. */
export const DEFAULT_SNAPSHOTS_TABLE = 'evt_snapshots'

/**
 * The infix the engine relations derive from the events table name with, such as
 * `evt_events__evt_features`. A configured table name may not contain it, in any case, so one
 * log's tables can never equal another log's derived relation.
 */
export const RESERVED_INFIX = '__evt_'

/**
 * The longest events table name, in bytes. Its longest derived relation,
 * `{events}__evt_rebuild_generation_pkey`, then fills PostgreSQL's 63-byte identifier limit.
 */
export const MAX_EVENTS_TABLE_NAME_BYTES = 63 - '__evt_rebuild_generation_pkey'.length

/** The longest snapshots table name, in bytes, which leaves room for its `_pkey` index. */
export const MAX_SNAPSHOTS_TABLE_NAME_BYTES = 63 - '_pkey'.length

// Only pgTables can make a PgTables, so the names the DDL interpolates are always validated.
declare const validated: unique symbol

/**
 * An event log's two configured tables, validated and folded to lower case. Only
 * {@link pgTables} makes one.
 */
export interface PgTables {
  readonly events: string
  readonly snapshots: string
  readonly [validated]: true
}

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/

// PostgreSQL's reserved keywords (pg_get_keywords() categories R and T), which can't be unquoted
// table names. The same list as Go's and Rust's.
const RESERVED_KEYWORDS = new Set(
  `all analyse analyze and any array as asc asymmetric authorization binary both case cast check
  collate collation column concurrently constraint create cross current_catalog current_date
  current_role current_schema current_time current_timestamp current_user default deferrable desc
  distinct do else end except false fetch for foreign freeze from full grant group having ilike in
  initially inner intersect into is isnull join lateral leading left like limit localtime
  localtimestamp natural not notnull null offset on only or order outer overlaps placing primary
  references returning right select session_user similar some symmetric system_user table
  tablesample then to trailing true union unique user using variadic verbose when where window
  with`.split(/\s+/),
)

/**
 * Validates an event log's table names and folds them to lower case, as PostgreSQL folds unquoted
 * identifiers. Each must be a plain identifier that isn't a reserved keyword, within its length
 * limit, without the {@link RESERVED_INFIX}, and no two relations the schema creates may share a
 * name. A name that breaks a rule fails with `invalidArgument`, as Go and Rust refuse it at
 * construction.
 */
export function pgTables(
  events: string = DEFAULT_EVENTS_TABLE,
  snapshots: string = DEFAULT_SNAPSHOTS_TABLE,
): PgTables {
  for (const [name, limit] of [
    [events, MAX_EVENTS_TABLE_NAME_BYTES],
    [snapshots, MAX_SNAPSHOTS_TABLE_NAME_BYTES],
  ] as const) {
    if (!validIdentifier(name, limit)) {
      throw invalidConfig(`invalid table name ${JSON.stringify(name)}`)
    }
    if (name.toLowerCase().includes(RESERVED_INFIX)) {
      throw invalidConfig(
        `table name ${JSON.stringify(name)} must not contain the reserved infix ` +
          JSON.stringify(RESERVED_INFIX),
      )
    }
  }

  const tables = {events: events.toLowerCase(), snapshots: snapshots.toLowerCase()} as PgTables
  const shared = sharedRelation(tables)
  if (shared !== null) {
    throw invalidConfig(
      `tables ${JSON.stringify(events)} and ${JSON.stringify(snapshots)} would both use the ` +
        `name ${JSON.stringify(shared)}`,
    )
  }

  return tables
}

/** The command-outcomes table: `evt_events` yields `evt_events__evt_command_outcomes`. */
export function outcomesTable(tables: PgTables): string {
  return derived(tables.events, 'command_outcomes')
}

/** The feature-marker table: `evt_events` yields `evt_events__evt_features`. */
export function featuresTable(tables: PgTables): string {
  return derived(tables.events, 'features')
}

/** The rebuild-lock table: `evt_events` yields `evt_events__evt_rebuild_lock`. */
export function rebuildLockTable(tables: PgTables): string {
  return derived(tables.events, 'rebuild_lock')
}

/** The rebuild-generation table: `evt_events` yields `evt_events__evt_rebuild_generation`. */
export function rebuildGenerationTable(tables: PgTables): string {
  return derived(tables.events, 'rebuild_generation')
}

/** The stream-heads table: `evt_events` yields `evt_events__evt_stream_heads`. */
export function streamHeadsTable(tables: PgTables): string {
  return derived(tables.events, 'stream_heads')
}

/** The entity-type index of the events table: `evt_events__evt_entity_type_idx`. */
export function entityTypeIndex(tables: PgTables): string {
  return derived(tables.events, 'entity_type_idx')
}

/**
 * The idempotent DDL of an event log, byte for byte the DDL Go's `EnsureSchema` and Rust's
 * `ensure_schema` run: the events and snapshots tables, the entity-type index (dropping its
 * pre-v0.9 name), and the command-outcome, feature-marker, rebuild-fence and stream-heads tables.
 * Run it inside one transaction, after {@link SCHEMA_LOCK} with the events table name, as Go and
 * Rust do.
 */
export function schemaDdl(tables: PgTables): string {
  const {events, snapshots} = tables

  return `
CREATE TABLE IF NOT EXISTS ${events} (
    entity_id   text   NOT NULL,
    sequence    bigint NOT NULL,
    event_id    text   NOT NULL,
    event_type  text   NOT NULL,
    version     bigint NOT NULL,
    entity_type text   NOT NULL,
    payload     bytea  NOT NULL,
    metadata    jsonb  NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_id, sequence)
);
CREATE INDEX IF NOT EXISTS ${entityTypeIndex(tables)} ON ${events} (entity_type);
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_index
        WHERE indexrelid = to_regclass('${events}_entity_type_idx')
          AND indrelid = '${events}'::regclass
    ) THEN
        DROP INDEX ${events}_entity_type_idx;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ${snapshots} (
    entity_id      text   PRIMARY KEY,
    entity_type    text   NOT NULL,
    sequence       bigint NOT NULL,
    event_sequence bigint NOT NULL,
    payload        bytea  NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ${outcomesTable(tables)} (
    command_id text        PRIMARY KEY,
    status     text        NOT NULL CHECK (status IN ('accepted', 'rejected')),
    digest     text        NOT NULL,
    response   bytea,
    events     jsonb       NOT NULL DEFAULT '[]'::jsonb,
    rejection  jsonb,
    code       text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'rejected' AND rejection IS NOT NULL AND code IS NOT NULL)
           OR (status = 'accepted' AND rejection IS NULL AND code IS NULL))
);

CREATE TABLE IF NOT EXISTS ${featuresTable(tables)} (
    id                integer PRIMARY KEY,
    version           bigint NOT NULL,
    features          text   NOT NULL,
    history_dependent text   NOT NULL DEFAULT '[]',
    retention         text   NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS ${rebuildLockTable(tables)} (
    id               integer PRIMARY KEY,
    owner            text        NOT NULL,
    generation       bigint      NOT NULL,
    lease_expires_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS ${rebuildGenerationTable(tables)} (
    id             integer PRIMARY KEY,
    value          bigint   NOT NULL,
    force_released boolean  NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS ${streamHeadsTable(tables)} (
    entity_id   text   PRIMARY KEY,
    entity_type text   NOT NULL,
    head        bigint NOT NULL CHECK (head >= 1)
);
`
}

/**
 * Serializes schema changes across processes, in every language: `IF NOT EXISTS` alone can still
 * race in PostgreSQL's catalogs. `$1` is the events table name.
 */
export const SCHEMA_LOCK = 'SELECT pg_advisory_xact_lock(1702261762, hashtext($1))'

/**
 * The per-entity transaction lock every commit and snapshot write takes, `$1` being the entity
 * ID. Go and Rust run this exact statement, so writers in every language exclude each other.
 */
export const ENTITY_LOCK = 'SELECT pg_advisory_xact_lock(1702261760, hashtext($1))'

/**
 * The shared engine-row lock a commit takes and holds to `COMMIT`, `$1` being a key from
 * {@link featuresLockKey} or {@link rebuildLockKey}. Commits don't serialize against each other.
 */
export const ENGINE_LOCK_SHARED = 'SELECT pg_advisory_xact_lock_shared(1702261761, hashtext($1))'

/**
 * The exclusive engine-row lock a declaration or a rebuild lifecycle step takes for its short
 * transaction, so it never slips between a commit's check and its `COMMIT`.
 */
export const ENGINE_LOCK_EXCLUSIVE = 'SELECT pg_advisory_xact_lock(1702261761, hashtext($1))'

/** The feature marker's engine-row lock key: `{events}:features`. */
export function featuresLockKey(tables: PgTables): string {
  return `${tables.events}:features`
}

/** The rebuild fence's engine-row lock key: `{events}:rebuild`. */
export function rebuildLockKey(tables: PgTables): string {
  return `${tables.events}:rebuild`
}

function derived(events: string, suffix: string): string {
  return `${events}${RESERVED_INFIX}${suffix}`
}

// A valid identifier is ASCII, so its length is its byte count and folding it changes only A-Z.
// The type check refuses a non-string passed from JavaScript, which the pattern would coerce.
function validIdentifier(name: unknown, limit: number): name is string {
  return (
    typeof name === 'string' &&
    name.length <= limit &&
    IDENTIFIER.test(name) &&
    !RESERVED_KEYWORDS.has(name.toLowerCase())
  )
}

// A name two relations of the schema would share, or null, with the names already folded to lower
// case. CREATE TABLE IF NOT EXISTS would otherwise skip a table whose name an index already took.
function sharedRelation(tables: PgTables): string | null {
  const {events, snapshots} = tables
  const derivedTables = [
    outcomesTable(tables),
    featuresTable(tables),
    rebuildLockTable(tables),
    rebuildGenerationTable(tables),
    streamHeadsTable(tables),
  ]
  const relations = [
    events,
    `${events}_pkey`,
    entityTypeIndex(tables),
    ...derivedTables.flatMap((table) => [table, `${table}_pkey`]),
    snapshots,
    `${snapshots}_pkey`,
  ]

  return relations.find((relation, index) => relations.indexOf(relation) !== index) ?? null
}

function invalidConfig(message: string): EvtError {
  return new EvtError('invalidArgument', `postgres: ${message}`)
}

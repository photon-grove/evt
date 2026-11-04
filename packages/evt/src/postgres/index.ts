// @photon-grove/evt/postgres: the PostgreSQL backend. It imports no driver. So far it holds the
// layout an event log is stored in: the table names, the relations derived from them, the DDL,
// and the lock to run it under.

export {
  DEFAULT_EVENTS_TABLE,
  DEFAULT_SNAPSHOTS_TABLE,
  MAX_EVENTS_TABLE_NAME_BYTES,
  MAX_SNAPSHOTS_TABLE_NAME_BYTES,
  type PgTables,
  RESERVED_INFIX,
  SCHEMA_LOCK,
  entityTypeIndex,
  featuresTable,
  outcomesTable,
  pgTables,
  rebuildGenerationTable,
  rebuildLockTable,
  schemaDdl,
  streamHeadsTable,
} from './schema.js'

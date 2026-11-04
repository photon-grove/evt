// @photon-grove/evt/postgres: the PostgreSQL backend. It imports no driver: the repository takes
// `pg`'s Pool, or anything shaped like it, as a PgClient.

export {type PgClient, type PgConnection, type PgQueryable, type PgResult} from './client.js'
export {POSTGRES_BACKEND, PgRepository, type PgRepositoryOptions} from './repository.js'

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

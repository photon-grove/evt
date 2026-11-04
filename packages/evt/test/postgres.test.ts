import {describe, expect, test} from 'vitest'

import {EvtError} from '../src/errors.ts'
import {
  ENGINE_LOCK_EXCLUSIVE,
  ENGINE_LOCK_SHARED,
  ENTITY_LOCK,
  MAX_EVENTS_TABLE_NAME_BYTES,
  MAX_SNAPSHOTS_TABLE_NAME_BYTES,
  featuresLockKey,
  outcomesTable,
  pgTables,
  rebuildGenerationTable,
  rebuildLockKey,
  SCHEMA_LOCK,
  schemaDdl,
} from '../src/postgres/schema.ts'

// The error pgTables throws for a pair of names it refuses.
function refused(events: string, snapshots?: string): EvtError {
  let thrown: unknown = null
  try {
    pgTables(events, snapshots)
  } catch (error) {
    thrown = error
  }

  expect(thrown).toBeInstanceOf(EvtError)
  expect((thrown as EvtError).code).toBe('invalidArgument')

  return thrown as EvtError
}

describe('PostgreSQL table names', () => {
  test('default to evt_events and evt_snapshots', () => {
    expect(pgTables()).toEqual({events: 'evt_events', snapshots: 'evt_snapshots'})
  })

  test('fold to lower case, and the lock keys follow the folded name', () => {
    const tables = pgTables('Ledger_Events', 'Ledger_Snaps')
    expect(tables).toEqual({events: 'ledger_events', snapshots: 'ledger_snaps'})
    expect(featuresLockKey(pgTables('Orders'))).toBe('orders:features')
    expect(rebuildLockKey(pgTables('Orders'))).toBe('orders:rebuild')

    const ddl = schemaDdl(tables)
    expect(ddl).toContain('CREATE TABLE IF NOT EXISTS ledger_events (')
    expect(ddl).toContain('CREATE TABLE IF NOT EXISTS ledger_snaps (')
    expect(ddl).toContain('CREATE TABLE IF NOT EXISTS ledger_events__evt_stream_heads (')
  })

  test('refuse a name that is no plain identifier, or is a reserved keyword', () => {
    expect(refused('events; DROP TABLE x').message).toContain('invalid table name')
    refused('1events')
    refused('')
    refused('Select')
    refused('evt_events', 'table')

    // Non-ASCII is no plain identifier, even where folding would make one: U+212A, the Kelvin
    // sign, lower-cases to k, and U+0130 to i with a dot above.
    refused('\u212Aelvin')
    refused('\u0130tems')
    refused('caf\u00e9')
    refused(null as unknown as string)
  })

  test('fit every derived relation in 63 bytes, and refuse a longer name', () => {
    expect(MAX_EVENTS_TABLE_NAME_BYTES).toBe(34)
    expect(MAX_SNAPSHOTS_TABLE_NAME_BYTES).toBe(58)

    const events = 'e'.repeat(34)
    expect(pgTables(events, 's'.repeat(58))).toEqual({events, snapshots: 's'.repeat(58)})
    const tables = pgTables(events)
    expect(`${rebuildGenerationTable(tables)}_pkey`).toHaveLength(63)
    expect(`${outcomesTable(tables)}_pkey`.length).toBeLessThanOrEqual(63)

    refused('e'.repeat(35))
    refused('evt_events', 's'.repeat(59))
  })

  test('refuse the reserved infix in any case', () => {
    expect(refused('orders__evt_features').message).toContain('reserved infix')
    refused('orders', 'ORDERS__EVT_features')
    refused('orders', 'orders__evt_rebuild_lock_pkey')
  })

  test('refuse two names whose relations would share a name', () => {
    for (const [events, snapshots] of [
      ['evt_events', 'Evt_Events'],
      ['events_pkey', 'events'],
      ['events', 'events_pkey'],
    ] as const) {
      expect(refused(events, snapshots).message).toContain('would both use the name')
    }
  })

  test("leave the pre-v0.9 entity-type index's name to the snapshots table", () => {
    const tables = pgTables('events', 'events_entity_type_idx')
    expect(tables.snapshots).toBe('events_entity_type_idx')
    expect(schemaDdl(tables)).toContain(
      'CREATE INDEX IF NOT EXISTS events__evt_entity_type_idx ON events (entity_type);',
    )
  })
})

describe('PostgreSQL locks', () => {
  // The exact statements Go and Rust run, so writers in every language exclude each other.
  test('are the statements Go and Rust take', () => {
    expect(SCHEMA_LOCK).toBe('SELECT pg_advisory_xact_lock(1702261762, hashtext($1))')
    expect(ENTITY_LOCK).toBe('SELECT pg_advisory_xact_lock(1702261760, hashtext($1))')
    expect(ENGINE_LOCK_SHARED).toBe('SELECT pg_advisory_xact_lock_shared(1702261761, hashtext($1))')
    expect(ENGINE_LOCK_EXCLUSIVE).toBe('SELECT pg_advisory_xact_lock(1702261761, hashtext($1))')
  })
})

// The shared account domain. It mirrors go/internal/compat/account.go and
// crates/evt-compat/src/account.rs field for field and rule for rule, so all three languages write
// identical payloads and snapshots. Its codecs are written by hand: the labels are a Go map, so
// they are a Map here, and decoding is lenient the way Go's is.

import {
  type Codec,
  type Decision,
  type EventOf,
  JsonNumber,
  type JsonValue,
  defineAggregate,
  defineEvents,
  jsonUpcaster,
  parseJson,
  stringifyJson,
} from '@photon-grove/evt'

/** Go's zero `time.Time`, the opening time of an account opened without one. */
export const GO_ZERO_TIME = '0001-01-01T00:00:00Z'

/** The account state. Its field order is the snapshot's key order, as in the Go struct. */
export interface Account {
  owner: string
  status: string
  balanceCents: number
  deposits: number
  openedAt: string
  tags: string[] | null
  labels: Map<string, string> | null
  lastMemo: string | null
}

/**
 * Account commands, decoded from the scenario JSON. `withdrawAudited` waits for rejections with
 * events (phase 4 of ADR 0009).
 */
export type AccountCommand =
  | {type: 'open'; owner: string; openedAt: string | null}
  | {type: 'deposit'; amountCents: number; memo: string | null}
  | {type: 'batchDeposit'; amounts: number[]}
  | {type: 'withdraw'; amountCents: number}
  | {type: 'tag'; tags: string[]}
  | {type: 'setLabels'; labels: Map<string, string>}
  | {type: 'rename'; owner: string}
  | {type: 'close'; reason: string | null}
  | {type: 'ping'}
  | {type: 'balance'}

const encoder = new TextEncoder()
const decoder = new TextDecoder('utf-8', {fatal: true})

// A struct codec: `write` gives the fields in Go's order, and `read` reads them leniently from the
// parsed object, with a missing or null field at its zero value.
function struct<T>(write: (value: T) => object, read: (fields: Fields) => T): Codec<T> {
  return {
    encode: (value) => encoder.encode(stringifyJson(write(value))),
    decode: (bytes) => read(new Fields(parseJson(decoder.decode(bytes)))),
  }
}

class Fields {
  readonly #object: Map<string, JsonValue>

  constructor(value: JsonValue) {
    if (!(value instanceof Map)) {
      throw new Error('payload is not an object')
    }

    this.#object = value
  }

  string(name: string, zero = ''): string {
    return this.nullableString(name) ?? zero
  }

  nullableString(name: string): string | null {
    const value = this.#object.get(name)
    if (value === undefined || value === null) {
      return null
    }
    if (typeof value !== 'string') {
      throw new Error(`${name} is not a string`)
    }

    return value
  }

  int(name: string): number {
    const value = this.#object.get(name)
    if (value === undefined || value === null) {
      return 0
    }

    return safeInteger(value, name)
  }

  // Refuses a field a command must have, as Rust's serde refuses a missing one.
  required(...names: string[]): this {
    for (const name of names) {
      const value = this.#object.get(name)
      if (value === undefined || value === null) {
        throw new Error(`the command has no ${name}`)
      }
    }

    return this
  }

  strings(name: string): string[] | null {
    const value = this.#object.get(name)
    if (value === undefined || value === null) {
      return null
    }
    if (!Array.isArray(value) || value.some((entry) => typeof entry !== 'string')) {
      throw new Error(`${name} is not a list of strings`)
    }

    return value as string[]
  }

  labels(name: string): Map<string, string> | null {
    const value = this.#object.get(name)
    if (value === undefined || value === null) {
      return null
    }
    if (!(value instanceof Map)) {
      throw new Error(`${name} is not an object`)
    }

    const labels = new Map<string, string>()
    for (const [key, entry] of value) {
      if (typeof entry !== 'string') {
        throw new Error(`${name}.${key} is not a string`)
      }
      labels.set(key, entry)
    }

    return labels
  }
}

const events = defineEvents({
  'account:opened': {
    version: 1,
    codec: struct(
      (data: {owner: string; openedAt: string}) => ({owner: data.owner, openedAt: data.openedAt}),
      (fields) => ({
        owner: fields.string('owner'),
        openedAt: fields.string('openedAt', GO_ZERO_TIME),
      }),
    ),
  },
  'account:deposited': {
    version: 1,
    codec: struct(
      (data: {amountCents: number; memo: string | null}) => ({
        amountCents: data.amountCents,
        memo: data.memo,
      }),
      (fields) => ({amountCents: fields.int('amountCents'), memo: fields.nullableString('memo')}),
    ),
  },
  'account:withdrawn': {
    version: 1,
    codec: struct(
      (data: {amountCents: number}) => ({amountCents: data.amountCents}),
      (fields) => ({amountCents: fields.int('amountCents')}),
    ),
  },
  'account:tagged': {
    version: 1,
    codec: struct(
      (data: {tags: string[]}) => ({tags: data.tags}),
      (fields) => ({tags: fields.strings('tags') ?? []}),
    ),
  },
  'account:labeled': {
    version: 1,
    codec: struct(
      (data: {labels: Map<string, string>}) => ({labels: data.labels}),
      (fields) => ({labels: fields.labels('labels') ?? new Map<string, string>()}),
    ),
  },
  // Version 1 stored the owner under `name`.
  'account:renamed': {
    version: 2,
    codec: struct(
      (data: {owner: string}) => ({owner: data.owner}),
      (fields) => ({owner: fields.string('owner')}),
    ),
    upcasters: [
      jsonUpcaster(1, 2, (payload) => {
        if (!(payload instanceof Map)) {
          throw new Error('account:renamed v1 payload is not an object')
        }
        if (!payload.has('name')) {
          throw new Error('account:renamed v1 payload has no name')
        }

        const upcast = new Map(payload)
        upcast.delete('name')
        upcast.set('owner', payload.get('name')!)

        return upcast
      }),
    ],
  },
  'account:closed': {
    version: 1,
    codec: struct(
      (data: {reason: string | null}) => ({reason: data.reason}),
      (fields) => ({reason: fields.nullableString('reason')}),
    ),
  },
})

type AccountEvent = EventOf<typeof events>

const state = struct<Account>(
  (account) => ({
    owner: account.owner,
    status: account.status,
    balanceCents: account.balanceCents,
    deposits: account.deposits,
    openedAt: account.openedAt,
    tags: account.tags,
    labels: account.labels,
    lastMemo: account.lastMemo,
  }),
  (fields) => ({
    owner: fields.string('owner'),
    status: fields.string('status'),
    balanceCents: fields.int('balanceCents'),
    deposits: fields.int('deposits'),
    openedAt: fields.string('openedAt', GO_ZERO_TIME),
    tags: fields.strings('tags'),
    labels: fields.labels('labels'),
    lastMemo: fields.nullableString('lastMemo'),
  }),
)

// A business-rule rejection: the code is the reason with spaces as `-`, as Go maps it.
function reject(reason: string, details?: unknown): Decision<AccountEvent> {
  const rejection = {code: reason.replaceAll(' ', '-'), message: reason}

  return {kind: 'reject', rejection: details === undefined ? rejection : {...rejection, details}}
}

function accept(...decided: AccountEvent[]): Decision<AccountEvent> {
  return {kind: 'accept', events: decided}
}

export const account = defineAggregate({
  type: 'account',
  events,
  state,
  initial: (): Account => ({
    owner: '',
    status: '',
    balanceCents: 0,
    deposits: 0,
    openedAt: GO_ZERO_TIME,
    tags: null,
    labels: null,
    lastMemo: null,
  }),

  handle(current: Account, command: AccountCommand): Decision<AccountEvent> {
    if (command.type === 'open') {
      if (current.status !== '') {
        return reject('account already opened')
      }

      const openedAt = command.openedAt || GO_ZERO_TIME
      return accept({type: 'account:opened', data: {owner: command.owner, openedAt}})
    }

    if (current.status !== 'open') {
      return reject('account is not open')
    }

    switch (command.type) {
      case 'deposit':
        if (command.amountCents <= 0) {
          return reject('amount must be positive')
        }
        return accept({
          type: 'account:deposited',
          data: {amountCents: command.amountCents, memo: command.memo},
        })
      case 'batchDeposit':
        if (command.amounts.length === 0) {
          return reject('no amounts')
        }
        if (command.amounts.some((amount) => amount <= 0)) {
          return reject('amount must be positive')
        }
        return accept(
          ...command.amounts.map(
            (amountCents): AccountEvent => ({
              type: 'account:deposited',
              data: {amountCents, memo: null},
            }),
          ),
        )
      case 'withdraw':
        if (command.amountCents <= 0) {
          return reject('amount must be positive')
        }
        if (command.amountCents > current.balanceCents) {
          return reject('insufficient funds', {
            balanceCents: current.balanceCents,
            requestedCents: command.amountCents,
          })
        }
        return accept({type: 'account:withdrawn', data: {amountCents: command.amountCents}})
      case 'tag':
        if (command.tags.length === 0) {
          return reject('no tags')
        }
        return accept({type: 'account:tagged', data: {tags: command.tags}})
      case 'setLabels':
        if (command.labels.size === 0) {
          return reject('no labels')
        }
        return accept({type: 'account:labeled', data: {labels: command.labels}})
      case 'rename':
        if (command.owner === '') {
          return reject('owner is required')
        }
        return accept({type: 'account:renamed', data: {owner: command.owner}})
      case 'close':
        return accept({type: 'account:closed', data: {reason: command.reason}})
      // Accepted with no events.
      case 'ping':
      case 'balance':
        return accept()
    }
  },

  apply(current: Account, event: AccountEvent): Account {
    switch (event.type) {
      case 'account:opened':
        return {...current, owner: event.data.owner, openedAt: event.data.openedAt, status: 'open'}
      case 'account:deposited':
        return {
          ...current,
          balanceCents: current.balanceCents + event.data.amountCents,
          deposits: current.deposits + 1,
          lastMemo: event.data.memo ?? current.lastMemo,
        }
      case 'account:withdrawn':
        return {...current, balanceCents: current.balanceCents - event.data.amountCents}
      case 'account:tagged':
        return {...current, tags: [...(current.tags ?? []), ...event.data.tags]}
      case 'account:labeled':
        return {...current, labels: new Map([...(current.labels ?? []), ...event.data.labels])}
      case 'account:renamed':
        return {...current, owner: event.data.owner}
      case 'account:closed':
        return {...current, status: 'closed'}
    }
  },
})

/** Decodes a scenario step's command object. */
export function accountCommand(value: JsonValue): AccountCommand {
  const fields = new Fields(value)
  const type = fields.required('type').string('type')

  switch (type) {
    case 'open':
      fields.required('owner')
      return {type, owner: fields.string('owner'), openedAt: fields.nullableString('openedAt')}
    case 'deposit':
      fields.required('amountCents')
      return {type, amountCents: fields.int('amountCents'), memo: fields.nullableString('memo')}
    case 'batchDeposit':
      return {type, amounts: ints(value, 'amounts')}
    case 'withdraw':
      return {type, amountCents: fields.required('amountCents').int('amountCents')}
    case 'tag':
      return {type, tags: fields.required('tags').strings('tags') ?? []}
    case 'setLabels':
      return {type, labels: fields.required('labels').labels('labels') ?? new Map()}
    case 'rename':
      return {type, owner: fields.required('owner').string('owner')}
    case 'close':
      return {type, reason: fields.nullableString('reason')}
    case 'ping':
    case 'balance':
      return {type}
    default:
      throw new Error(`unknown account command ${type}`)
  }
}

function ints(value: JsonValue, name: string): number[] {
  const list = value instanceof Map ? value.get(name) : undefined
  if (!Array.isArray(list)) {
    throw new Error(`${name} is not a list`)
  }

  return list.map((entry) => safeInteger(entry, name))
}

// An integer as a number, refused past the safe range rather than rounded.
function safeInteger(value: JsonValue, name: string): number {
  if (!(value instanceof JsonNumber) || !value.isInteger) {
    throw new Error(`${name} is not an integer`)
  }

  const number = Number(value.text)
  if (!Number.isSafeInteger(number)) {
    throw new Error(`${name} is ${value.text}, past the integers this domain holds`)
  }

  return number
}

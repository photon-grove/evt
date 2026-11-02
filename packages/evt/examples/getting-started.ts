// The program docs/guide/getting-started.md builds in TypeScript, step by step: the bank account
// aggregate of the Rust walkthrough, run against the in-memory backend. Its tests are in
// getting-started.test.ts beside it.
//
// The guide's TypeScript code blocks are copied from these two files, and
// test/getting-started.test.ts fails if they drift apart. Build the package, then run it with
// `node examples/getting-started.ts` from packages/evt.

import {
  type EventOf,
  type Loaded,
  Store,
  defineAggregate,
  defineEvents,
  json,
} from '@photon-grove/evt'
import {MemRepository} from '@photon-grove/evt/memory'

export const accountEvents = defineEvents({
  'bank_account.opened': {
    version: 1,
    codec: json<{accountId: string; balance: number}>(),
  },
  'bank_account.money_deposited': {
    version: 1,
    codec: json<{accountId: string; amount: number}>(),
  },
})

export type AccountEvent = EventOf<typeof accountEvents>

export type AccountCommand =
  | {type: 'open'; accountId: string; initialBalance: number}
  | {type: 'deposit'; amount: number}

export interface Account {
  id: string
  balance: number
  opened: boolean
}

export const account = defineAggregate({
  type: 'bank_account',
  events: accountEvents,
  state: json<Account>(),
  initial: (): Account => ({id: '', balance: 0, opened: false}),

  handle(state, command: AccountCommand) {
    switch (command.type) {
      case 'open':
        if (state.opened) {
          return reject('already-opened', 'account already opened')
        }
        return accept({
          type: 'bank_account.opened',
          data: {accountId: command.accountId, balance: command.initialBalance},
        })

      case 'deposit':
        if (!state.opened) {
          return reject('not-opened', 'account not opened')
        }
        if (command.amount <= 0) {
          return reject('invalid-amount', 'amount must be positive')
        }
        return accept({
          type: 'bank_account.money_deposited',
          data: {accountId: state.id, amount: command.amount},
        })
    }
  },

  apply(state, event) {
    switch (event.type) {
      case 'bank_account.opened':
        return {id: event.data.accountId, balance: event.data.balance, opened: true}
      case 'bank_account.money_deposited':
        return {...state, balance: state.balance + event.data.amount}
    }
  },
})

function accept(...events: AccountEvent[]) {
  return {kind: 'accept', events} as const
}

function reject(code: string, message: string) {
  return {kind: 'reject', rejection: {code, message}} as const
}

export async function openAndDeposit(): Promise<Loaded<Account>> {
  const store = new Store(new MemRepository())

  await store.execute(account, 'acct-1', {type: 'open', accountId: 'acct-1', initialBalance: 100})
  await store.execute(account, 'acct-1', {type: 'deposit', amount: 25})

  // A rejected command commits nothing, and its result carries the rejection.
  const result = await store.execute(account, 'acct-1', {type: 'deposit', amount: 0})
  if (result.kind !== 'rejected' || result.rejection.code !== 'invalid-amount') {
    throw new Error(`expected a rejection, got ${result.kind}`)
  }

  // Each load folds the stream (from its latest snapshot, if any) into a fresh state.
  return store.load(account, 'acct-1')
}

if (import.meta.main) {
  console.log((await openAndDeposit()).state)
}

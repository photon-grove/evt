import {scenario} from '@photon-grove/evt/testing'
import {expect, test} from 'vitest'

import {type AccountEvent, account} from './getting-started.ts'

function opened(accountId: string, balance: number): AccountEvent {
  return {type: 'bank_account.opened', data: {accountId, balance}}
}

test('opening emits an opened event', async () => {
  await scenario(account)
    .when({type: 'open', accountId: 'acct-1', initialBalance: 100})
    .thenEvents(opened('acct-1', 100))
})

test('depositing adds to the balance', async () => {
  await scenario(account)
    .given(opened('acct-1', 100))
    .when({type: 'deposit', amount: 25})
    .thenState((state) => {
      expect(state.balance).toBe(125)
    })
})

test('depositing before opening is rejected', async () => {
  await scenario(account).when({type: 'deposit', amount: 10}).thenRejected('not-opened')
})

// The payload Lambda delivers, read as it is, and its images read as evt items.
import type {DynamoDBStreamEvent} from 'aws-lambda'
import {describe, expect, test} from 'vitest'

import {processRecords} from '../src/batch.ts'
import {
  type StreamEvent,
  eventName,
  isTtlDeletion,
  itemFromStream,
  recordKey,
  recordPk,
  sequenceNumber,
  streamItem,
  streamViewType,
} from '../src/record.ts'

const PAYLOAD = `{"Records":[
  {"eventID":"1","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb",
   "awsRegion":"us-west-2",
   "dynamodb":{"ApproximateCreationDateTime":1760918400,"Keys":{"pk":{"S":"acct-1"},"sk":{"N":"1"}},
     "NewImage":{"pk":{"S":"acct-1"},"sk":{"N":"1"},"flags":{"SS":["a"]},"blob":{"B":"AAE="},
       "nested":{"M":{"x":{"L":[{"NULL":true},{"BOOL":false}]}}}},
     "SequenceNumber":"100","SizeBytes":42,"StreamViewType":"NEW_IMAGE"},
   "eventSourceARN":"arn:aws:dynamodb:us-west-2:000000000000:table/events/stream/x"},
  {"eventName":"SOMETHING_NEW","dynamodb":{"Keys":{},"SequenceNumber":"101","StreamViewType":"LATER"}},
  {"eventName":"REMOVE","dynamodb":{"Keys":{"pk":{"S":"v"}},"SequenceNumber":"102"},
   "userIdentity":{"type":"Service","principalId":"dynamodb.amazonaws.com"}}
]}`

describe('stream records', () => {
  test('a Lambda payload reads as it is, and unknown names do not fail the batch', () => {
    const event = JSON.parse(PAYLOAD) as StreamEvent
    const [insert, unknown, ttl] = event.Records!

    expect(eventName(insert!)).toBe('INSERT')
    expect(sequenceNumber(insert!)).toBe('100')
    expect(recordPk(insert!)).toBe('acct-1')
    expect(streamViewType(insert!)).toBe('NEW_IMAGE')
    expect(isTtlDeletion(insert!)).toBe(false)

    const image = itemFromStream(insert!.dynamodb!.NewImage!)
    expect(image['blob']).toEqual({B: new Uint8Array([0, 1])})
    expect(image['nested']).toEqual({M: {x: {L: [{NULL: true}, {BOOL: false}]}}})
    expect(streamItem(image)).toEqual(insert!.dynamodb!.NewImage)

    expect(eventName(unknown!)).toBe('UNKNOWN')
    expect(streamViewType(unknown!)).toBe('UNKNOWN')
    expect(recordPk(unknown!)).toBeUndefined()

    expect(isTtlDeletion(ttl!)).toBe(true)
    expect(streamViewType(ttl!)).toBeUndefined()
  })

  test('the pk comes from the keys, or from the new image when the keys lack it', () => {
    const keysOnly = {
      eventName: 'INSERT',
      dynamodb: {Keys: {pk: {N: '1'}}, NewImage: {pk: {S: 'x'}}},
    }
    expect(recordPk(keysOnly)).toBeUndefined()

    const imageOnly = {eventName: 'INSERT', dynamodb: {NewImage: {pk: {S: 'x'}}}}
    expect(recordPk(imageOnly)).toBe('x')
    expect(sequenceNumber(imageOnly)).toBe('')
  })

  test('an attribute named __proto__ is an ordinary attribute', () => {
    const image = JSON.parse(
      '{"pk":{"S":"a"},"__proto__":{"S":"x"},"m":{"M":{"__proto__":{"N":"1"}}}}',
    ) as Parameters<typeof itemFromStream>[0]

    const item = itemFromStream(image)

    expect(Object.keys(item)).toEqual(['pk', '__proto__', 'm'])
    expect(Object.getPrototypeOf(item)).toBe(Object.prototype)
    expect(Object.keys(streamItem(item))).toEqual(['pk', '__proto__', 'm'])
    expect(recordPk({dynamodb: {Keys: {}, NewImage: {}}})).toBeUndefined()
    expect(recordKey({dynamodb: {Keys: {}}}, 'toString')).toBeUndefined()
  })

  test('an image that is not DynamoDB JSON is refused', () => {
    const images = [
      {a: {}},
      {a: {S: 'x', N: '1'}},
      {a: {S: 1}},
      {a: {X: 'x'}},
      {a: {B: 'not base64!'}},
      {a: {L: [{BOOL: 'yes'}]}},
      {a: {M: []}},
      {a: {SS: 'x'}},
      {a: null},
    ]
    const accepted = images.filter((image) => {
      try {
        itemFromStream(image as never)
        return true
      } catch {
        return false
      }
    })

    expect(accepted).toEqual([])
  })

  test('a DynamoDBStreamEvent from @types/aws-lambda passes in as it is', async () => {
    const event: DynamoDBStreamEvent = JSON.parse(PAYLOAD) as DynamoDBStreamEvent
    const seen: string[] = []

    const outcome = await processRecords(event, (record) => {
      seen.push(sequenceNumber(record))
    })

    expect(outcome.isSuccess()).toBe(true)
    expect(seen).toEqual(['100', '101', '102'])
  })
})

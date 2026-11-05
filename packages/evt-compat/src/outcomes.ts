// The command-outcome vectors (compat/fixtures/outcomes.json): the bounds, the retry defaults, the
// identity digests, every stored row as a DynamoDB item and a PostgreSQL row, the malformed rows
// every reader refuses, the canonical text of a rejection's numbers, and the recorded identity's
// bound.
import {
  DEFAULT_RETRY_POLICY,
  EvtError,
  type JsonValue,
  MAX_COMMAND_ID_BYTES,
  MAX_OUTCOME_BYTES,
  type Outcome,
  OutcomeTooLargeError,
  REJECTION_CODES,
  type RecordedIdentity,
  base64Decode,
  base64Encode,
  canonicalDigestBytes,
  canonicalNumbers,
  checkOutcome,
  decodeRejection,
  defaultDigest,
  encodeRejection,
  identitySize,
  retriesExhaustedRejection,
  staleFenceRejection,
  staleVersionRejection,
} from '@photon-grove/evt'
import {type Item, outcomeFromItem, outcomeItem, outcomeItemSize} from '@photon-grove/evt/dynamodb'
import {outcomeFromRow, outcomeRow} from '@photon-grove/evt/postgres'

import {type Checks, canonical, field, list, loadFixture, num, plain, str} from './harness.js'

const encoder = new TextEncoder()

export async function outcomeVectors(checks: Checks): Promise<void> {
  const fixtures = await loadFixture('outcomes.json')

  limits(checks, field(fixtures, 'limits'))
  retryDefaults(checks, field(fixtures, 'retry'))
  await digests(checks, list(field(fixtures, 'digests')))
  rows(checks, list(field(fixtures, 'rows')))
  engineRejections(checks, list(field(fixtures, 'rows')))
  malformedRows(checks, list(field(fixtures, 'malformedRows')))
  rejectionNumbers(checks, list(field(fixtures, 'rejectionNumbers')))
  identityBounds(checks, list(field(fixtures, 'identityBounds')))
}

function limits(checks: Checks, vector: JsonValue): void {
  checks.equal(
    'outcomes maxCommandIdBytes',
    MAX_COMMAND_ID_BYTES,
    num(field(vector, 'maxCommandIdBytes')),
  )
  checks.equal('outcomes maxOutcomeBytes', MAX_OUTCOME_BYTES, num(field(vector, 'maxOutcomeBytes')))
}

// The default policy Go and Rust retry with, and the engine's rejection codes.
function retryDefaults(checks: Checks, retry: JsonValue): void {
  for (const key of ['maxAttempts', 'baseDelayMs', 'maxDelayMs'] as const) {
    checks.equal(`retry ${key}`, DEFAULT_RETRY_POLICY[key], num(field(retry, key)))
  }
  checks.equal('retry staleCode', REJECTION_CODES.staleFence, str(field(retry, 'staleCode')))
  checks.equal(
    'retry staleVersionCode',
    REJECTION_CODES.staleVersion,
    str(field(retry, 'staleVersionCode')),
  )
}

// Each identity's canonical bytes, built from its fields, and their SHA-256.
async function digests(checks: Checks, vectors: JsonValue[]): Promise<void> {
  checks.equal('outcomes digests present', vectors.length > 0, true)
  const got = await Promise.all(
    vectors.map(async (vector) => {
      const input = field(vector, 'input')
      const payload = field(input, 'payload')
      const digestInput = {
        commandType: str(field(input, 'commandType')),
        targets: list(field(input, 'targets')).map(str),
        payload: encoder.encode(payload === null ? '' : str(payload)),
        principal: str(field(input, 'principal')),
      }

      return {
        bytes: hex(canonicalDigestBytes(digestInput)),
        digest: await defaultDigest(digestInput),
      }
    }),
  )

  vectors.forEach((vector, i) => {
    const name = `outcomes digest ${str(field(vector, 'name'))}`
    checks.equal(`${name}: canonical bytes`, got[i]!.bytes, str(field(vector, 'canonicalHex')))
    checks.equal(`${name}: digest`, got[i]!.digest, str(field(vector, 'digest')))
  })
}

// Each row encodes to Go's DynamoDB item and PostgreSQL row, at Go's budget size, and Go's item and
// row decode back to it.
function rows(checks: Checks, vectors: JsonValue[]): void {
  for (const vector of vectors) {
    const name = `outcomes row ${str(field(vector, 'name'))}`
    checks.run(name, () => {
      const outcome = rowOutcome(vector)
      const item = outcomeItem(outcome)
      checks.equal(
        `${name}: dynamo item`,
        canonical(item),
        canonical(plain(field(vector, 'dynamoItem'))),
      )
      checks.equal(`${name}: item size`, outcomeItemSize(outcome), num(field(vector, 'itemSize')))
      checks.equal(
        `${name}: decode Go item`,
        outcomeText(outcomeFromItem(plain(field(vector, 'dynamoItem')) as Item)),
        outcomeText(outcome),
      )

      const row = outcomeRow(outcome)
      const pg = {...row, response: row.response === null ? null : base64Encode(row.response)}
      checks.equal(
        `${name}: postgres row`,
        canonical(pg),
        canonical(plain(field(vector, 'postgresRow'))),
      )
      const rejectionJson = optional(vector, 'rejectionJson')
      if (rejectionJson !== undefined) {
        checks.equal(`${name}: rejection text`, row.rejection, rejectionJson)
      }
      const eventsJson = optional(vector, 'eventsJson')
      checks.equal(`${name}: events text`, row.events, eventsJson ?? '[]')
      checks.equal(`${name}: decode row`, outcomeText(outcomeFromRow(row)), outcomeText(outcome))
    })
  }
}

// The engine's own rejections encode to the bytes Go stored for them.
function engineRejections(checks: Checks, vectors: JsonValue[]): void {
  const stored = (name: string): string => {
    const vector = vectors.find((each) => str(field(each, 'name')) === name)

    return str(field(field(vector, 'input'), 'rejection'))
  }

  checks.run('outcomes engine rejections', () => {
    checks.equal(
      'stale fence',
      encodeRejection(staleFenceRejection()),
      stored('rejected stale fence'),
    )
    checks.equal(
      'retries exhausted',
      encodeRejection(retriesExhaustedRejection(5)),
      stored('rejected stale fence, retries exhausted'),
    )
    // Out of order on purpose: the constructor sorts by entity ID.
    const staleVersion = staleVersionRejection([
      {entityId: 'profile#u-1', expected: 4n, actual: 6n},
      {entityId: 'claim#ada', expected: 0n, actual: 1n},
    ])
    checks.equal('stale version', encodeRejection(staleVersion), stored('rejected stale version'))
  })
}

// Every malformed row is refused as a corrupt record, never replayed.
function malformedRows(checks: Checks, vectors: JsonValue[]): void {
  checks.equal('outcomes malformed rows present', vectors.length > 0, true)
  for (const vector of vectors) {
    const name = `outcomes malformed ${str(field(vector, 'name'))}`
    let code = 'decoded'
    try {
      outcomeFromItem(plain(field(vector, 'dynamoItem')) as Item)
    } catch (error) {
      code = error instanceof EvtError ? error.code : String(error)
    }
    checks.equal(name, code, 'invalidRecord')
  }
}

// A rejection's numbers take the text PostgreSQL's jsonb prints, literal by literal.
function rejectionNumbers(checks: Checks, vectors: JsonValue[]): void {
  checks.equal('outcomes rejection numbers present', vectors.length > 0, true)
  for (const vector of vectors) {
    const name = `outcomes rejection numbers ${str(field(vector, 'name'))}`
    const error = optional(vector, 'error') ?? ''
    let got: string
    try {
      got = canonicalNumbers(str(field(vector, 'input')))
    } catch (failure) {
      got = errorName(failure)
    }
    checks.equal(name, got, error === '' ? str(field(vector, 'stored')) : error)
  }
}

// The recorded identity's bound counts the UTF-8 bytes of its fields, escapes included.
function identityBounds(checks: Checks, vectors: JsonValue[]): void {
  checks.equal('outcomes identity bounds present', vectors.length > 0, true)
  for (const vector of vectors) {
    const name = `outcomes identity bound ${str(field(vector, 'name'))}`
    const identity = recordedIdentity(field(vector, 'identity'))
    checks.equal(`${name}: size`, identitySize(identity), num(field(vector, 'size')))

    let got = ''
    try {
      checkOutcome({...accepted('c', 'd'), identity})
    } catch (failure) {
      got =
        failure instanceof OutcomeTooLargeError && failure.what === 'identity'
          ? `outcome-too-large ${failure.size}`
          : String(failure)
    }
    const error = optional(vector, 'error') ?? ''
    checks.equal(name, got, error === '' ? '' : `${error} ${num(field(vector, 'size'))}`)
  }
}

// The outcome a row vector describes. The response's raw text is lossy for binary bytes, so it
// comes from the PostgreSQL row's base64.
function rowOutcome(vector: JsonValue): Outcome {
  const input = field(vector, 'input')
  const status = str(field(input, 'status'))
  if (status !== 'accepted' && status !== 'rejected') {
    throw new Error(`unknown status ${status}`)
  }

  const response = field(field(vector, 'postgresRow'), 'response')
  const rejection = field(input, 'rejection')
  const identity = field(input, 'identity')

  return {
    commandId: str(field(input, 'commandId')),
    status,
    digest: str(field(input, 'digest')),
    response: response === null ? null : base64Decode(str(response)),
    eventIds: list(field(input, 'eventIds')).map(str),
    rejection: rejection === null ? null : decodeRejection(str(rejection)),
    identity: identity === null ? null : recordedIdentity(identity),
  }
}

function recordedIdentity(value: JsonValue): RecordedIdentity {
  const principal = field(value, 'principal')

  return {
    commandType: str(field(value, 'commandType')),
    targets: list(field(value, 'targets')).map(str),
    principal: principal === null ? null : str(principal),
  }
}

function accepted(commandId: string, digest: string): Outcome {
  return {
    commandId,
    status: 'accepted',
    digest,
    response: null,
    eventIds: [],
    rejection: null,
    identity: null,
  }
}

// An outcome as text, for comparing two of them: the rejection in its stored encoding.
function outcomeText(outcome: Outcome): string {
  return canonical({
    ...outcome,
    response: outcome.response === null ? null : base64Encode(outcome.response),
    eventIds: [...outcome.eventIds],
    rejection: outcome.rejection === null ? null : encodeRejection(outcome.rejection),
    identity:
      outcome.identity === null
        ? null
        : {...outcome.identity, targets: [...outcome.identity.targets]},
  })
}

function optional(vector: JsonValue, name: string): string | undefined {
  const value = vector instanceof Map ? vector.get(name) : undefined

  return value === undefined || value === null ? undefined : str(value)
}

function errorName(error: unknown): string {
  if (error instanceof OutcomeTooLargeError) {
    return 'outcome-too-large'
  }
  if (error instanceof EvtError && error.code === 'invalidArgument') {
    return 'invalid-argument'
  }

  return String(error)
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')
}

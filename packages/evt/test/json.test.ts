import {describe, expect, test} from 'vitest'

import {EvtError} from '../src/errors.ts'
import {JsonNumber, parseJson, stringifyJson} from '../src/json.ts'

function code(run: () => unknown): string | undefined {
  try {
    run()
  } catch (error) {
    return error instanceof EvtError ? error.code : 'not an EvtError'
  }

  return undefined
}

describe('stringifyJson', () => {
  test('escapes strings as Go does', () => {
    expect(stringifyJson('<a & b> \u2028\u2029 "q" \\')).toBe(
      '"\\u003ca \\u0026 b\\u003e \\u2028\\u2029 \\"q\\" \\\\"',
    )
    expect(stringifyJson('\b\f\n\r\t \u0001 \u007f é 🚀')).toBe(
      '"\\b\\f\\n\\r\\t \\u0001 \u007f é 🚀"',
    )
  })

  // Go strings are bytes and Rust strings are valid UTF-8, so no vector can hold a lone
  // surrogate. Go writes invalid UTF-8 as U+FFFD, which is what a lone surrogate is to UTF-8.
  test('writes a lone surrogate as \\ufffd', () => {
    expect(stringifyJson('a\ud800b')).toBe('"a\\ufffdb"')
    expect(stringifyJson('\udc00')).toBe('"\\ufffd"')
    expect(stringifyJson('\ud83d')).toBe('"\\ufffd"')
    expect(stringifyJson('\ud83d\ude80')).toBe('"🚀"')
  })

  test('writes plain objects in property order and Maps sorted by UTF-8 bytes', () => {
    expect(stringifyJson({b: 1, a: [true, null]})).toBe('{"b":1,"a":[true,null]}')

    // U+FF5E sorts before U+1F600 by UTF-16 code units, and after it by UTF-8 bytes.
    const keys = new Map([['\u{1f600}', 1], ['\uff5e', 2], ['b', 3], ['a', 4]])
    expect(stringifyJson(keys)).toBe('{"a":4,"b":3,"\uff5e":2,"\u{1f600}":1}')
  })

  test('writes integer Map keys as text, sorted as text', () => {
    expect(stringifyJson(new Map<number | bigint, string>([[10, 'x'], [2n, 'y'], [-1, 'z']]))).toBe(
      '{"-1":"z","10":"x","2":"y"}',
    )
    expect(code(() => stringifyJson(new Map<unknown, number>([[1, 1], ['1', 2]])))).toBe(
      'invalidArgument',
    )
    expect(code(() => stringifyJson(new Map([[1.5, 1]])))).toBe('invalidArgument')
  })

  test('writes bigints, bytes and JsonNumbers exactly', () => {
    expect(stringifyJson(2n ** 63n - 1n)).toBe('9223372036854775807')
    expect(stringifyJson(new Uint8Array([102, 111, 111, 98]))).toBe('"Zm9vYg=="')
    expect(stringifyJson(new JsonNumber('1.50e+3'))).toBe('1.50e+3')
  })

  test('writes numbers as Go writes a float64, and negative zero as a Go int', () => {
    expect(stringifyJson([0, -0, 1.5, 1e21, 1e-7, 123456789012])).toBe(
      '[0,0,1.5,1e+21,1e-7,123456789012]',
    )
  })

  test('skips undefined properties', () => {
    expect(stringifyJson({a: undefined, b: 1})).toBe('{"b":1}')
  })

  test('refuses what Go would not write, or would write differently', () => {
    expect(code(() => stringifyJson({10: 'ten'}))).toBe('invalidArgument')
    expect(code(() => stringifyJson(Number.NaN))).toBe('invalidArgument')
    expect(code(() => stringifyJson(Number.POSITIVE_INFINITY))).toBe('invalidArgument')
    expect(code(() => stringifyJson(new Date(0)))).toBe('invalidArgument')
    expect(code(() => stringifyJson(undefined))).toBe('invalidArgument')
    expect(code(() => stringifyJson([undefined]))).toBe('invalidArgument')
    expect(code(() => stringifyJson(() => 1))).toBe('invalidArgument')

    const cyclic: Record<string, unknown> = {}
    cyclic['self'] = cyclic
    expect(code(() => stringifyJson(cyclic))).toBe('invalidArgument')
  })

  test('allows a value that appears twice without a cycle', () => {
    const shared = {a: 1}
    expect(stringifyJson({x: shared, y: shared})).toBe('{"x":{"a":1},"y":{"a":1}}')
  })

  test('accepts keys that look numeric but are not array indices', () => {
    const keys = {'01': 1, '-1': 2, '4294967295': 3}
    expect(stringifyJson(keys)).toBe('{"01":1,"-1":2,"4294967295":3}')
  })
})

describe('parseJson', () => {
  test('keeps number text and key order', () => {
    const value = parseJson('{"b":9007199254740993,"10":1.50,"2":-0}')
    expect(value).toBeInstanceOf(Map)

    const object = value as Map<string, JsonNumber>
    expect([...object.keys()]).toEqual(['b', '10', '2'])
    expect(object.get('b')?.text).toBe('9007199254740993')
    expect(object.get('10')?.text).toBe('1.50')
    expect(object.get('10')?.isInteger).toBe(false)
  })

  test('keeps the first place and the last value of a repeated key, or refuses it', () => {
    const object = parseJson('{"a":1,"b":2,"a":3}') as Map<string, JsonNumber>
    expect([...object].map(([key, value]) => [key, value.text])).toEqual([
      ['a', '3'],
      ['b', '2'],
    ])

    const refuse = {duplicateKeys: 'refuse'} as const
    expect(code(() => parseJson('{"a":1,"a":3}', refuse))).toBe('invalidJson')
    expect(code(() => parseJson('[{"a":{"b":1,"b":2}}]', refuse))).toBe('invalidJson')
    expect(parseJson('[{"a":1},{"a":2}]', refuse)).toHaveLength(2)
  })

  test('reads escapes, and a lone surrogate as U+FFFD', () => {
    expect(parseJson('"\\u0041\\/\\b\\f\\n\\r\\t\\"\\\\"')).toBe('A/\b\f\n\r\t"\\')
    expect(parseJson('"\\ud83d\\ude80"')).toBe('🚀')
    expect(parseJson('"\\ud800x"')).toBe('\ufffdx')
    expect(parseJson('"\\udc00"')).toBe('\ufffd')
    expect(parseJson('"\\ud800\\u0041"')).toBe('\ufffdA')
    expect(parseJson('"\\ud800\\ud800\\udc00"')).toBe('\ufffd\u{10000}')
    expect(parseJson('"a\ud800b"')).toBe('a\ufffdb')
  })

  test('refuses what isn’t JSON', () => {
    for (const text of [
      '',
      '01',
      '1.',
      '.5',
      '+1',
      '1e',
      '"a',
      '"\\x"',
      '"\\u12"',
      '"\u0001"',
      '{"a":1,}',
      '[1,]',
      '{a:1}',
      'nul',
      'true false',
      '\u00a01',
    ]) {
      expect({text, code: code(() => parseJson(text))}).toEqual({text, code: 'invalidJson'})
    }
  })

  test('refuses nesting past Go’s limit', () => {
    expect(parseJson('['.repeat(10_000) + ']'.repeat(10_000))).toBeInstanceOf(Array)
    expect(code(() => parseJson('['.repeat(10_001) + ']'.repeat(10_001)))).toBe('invalidJson')
    expect(code(() => parseJson('[{"a":'.repeat(5_001) + '1' + '}]'.repeat(5_001)))).toBe(
      'invalidJson',
    )
  })

  test('round-trips through the writer', () => {
    const text = '{"z":[1,2.5e-3,"x"],"a":{"k":null,"j":true}}'
    expect(stringifyJson(parseJson(text))).toBe('{"a":{"j":true,"k":null},"z":[1,2.5e-3,"x"]}')
  })
})

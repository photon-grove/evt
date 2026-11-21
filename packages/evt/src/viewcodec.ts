// Typed views over a ViewRepository (ADR 0009 phase 5): the counterpart of Go's `viewcodec.Codec`
// and `viewcodec.Single` with the `evt.GetJSONView` family, and of Rust's `ViewCodec<T>`. A codec
// writes a value as a row's payload with a payload `Codec<T>` (evt's JSON writer by default, whose
// bytes are Go's `json.Marshal` of the same shape) and decodes a row's payload back.

import {type Codec, json} from './codec.js'
import {EvtError, messageOf} from './errors.js'
import type {CallOptions} from './repository.js'
import type {PageOptions, ViewRepository, ViewRow} from './views.js'

/**
 * A row whose payload doesn't decode into the codec's value type (Go's `JSONViewDecodeError`). It
 * names the row, and its cause is the codec's error. Code `invalidRecord`.
 */
export class ViewDecodeError extends EvtError {
  override readonly name = 'ViewDecodeError'
  override readonly code = 'invalidRecord'
  readonly pk: string
  readonly sk: string

  constructor(pk: string, sk: string, cause: unknown) {
    super(
      'invalidRecord',
      `decode JSON view pk=${JSON.stringify(pk)} sk=${JSON.stringify(sk)}: ${messageOf(cause)}`,
      {cause},
    )
    this.pk = pk
    this.sk = sk
  }
}

/** Called for each row a lenient listing skips because it doesn't decode. */
export type DecodeErrorHandler = (row: ViewRow, error: ViewDecodeError) => void

/** Options for the lenient listings. */
export interface LenientListOptions extends CallOptions {
  /** Called for each skipped row. Without it, skipped rows go unreported. */
  onDecodeError?: DecodeErrorHandler | undefined
}

/** One page of decoded values, and the cursor of the next page or null. */
export interface ValuePage<T> {
  readonly values: T[]
  readonly nextCursor: string | null
}

/**
 * Decodes a row's payload with `codec` (evt's JSON codec by default), Go's `evt.DecodeJSONView`.
 * A failure is a {@link ViewDecodeError} naming the row.
 */
export function decodeJsonView<T>(row: ViewRow, codec: Codec<T> = json<T>()): T {
  try {
    return codec.decode(row.payload)
  } catch (error) {
    throw new ViewDecodeError(row.pk, row.sk, error)
  }
}

/**
 * A {@link ViewRepository} bound to a value type and an entity type: the caller gives the keys and
 * the owner per call (Go `viewcodec.Codec`, Rust `ViewCodec`).
 *
 * The payload codec decides the bytes. The default, `json<T>()`, writes plain objects in property
 * order with Go's escaping and decodes plain JSON, so a view a Go or Rust service reads must
 * declare its properties in the Go struct's order, and a map that Go writes sorted needs a codec
 * of its own that writes a `Map`. A custom codec also stands in for Go's factory constructors: its
 * `decode` can build the value with whatever dependencies it needs.
 *
 * A value that may itself be `null` can't be told from a missing row by {@link get}; read the
 * row from {@link repo} and {@link decode} it instead.
 */
export class ViewCodec<T> {
  readonly #repo: ViewRepository
  readonly #entityType: string
  readonly #codec: Codec<T>

  constructor(repo: ViewRepository, entityType: string, codec: Codec<T> = json<T>()) {
    this.#repo = repo
    this.#entityType = entityType
    this.#codec = codec
  }

  /** The repository, for raw access such as an existence probe that shouldn't pay the decode. */
  get repo(): ViewRepository {
    return this.#repo
  }

  /** The entity type this codec writes. */
  get entityType(): string {
    return this.#entityType
  }

  /** Decodes a row's payload, or throws a {@link ViewDecodeError}. */
  decode(row: ViewRow): T {
    return decodeJsonView(row, this.#codec)
  }

  /** The decoded single-item view at `pk` (`sk = "VIEW"`), or null when there is none. */
  async get(pk: string, options?: CallOptions): Promise<T | null> {
    const row = await this.#repo.getView(pk, options)

    return row === null ? null : this.decode(row)
  }

  /** The decoded view at `pk` and `sk`, or null when there is none. */
  async getAt(pk: string, sk: string, options?: CallOptions): Promise<T | null> {
    const row = await this.#repo.getViewAt(pk, sk, options)

    return row === null ? null : this.decode(row)
  }

  /** Writes `value` at `pk` and `sk`, owned by `entityId`, under the codec's entity type. */
  async put(
    pk: string,
    sk: string,
    entityId: string,
    value: T,
    options?: CallOptions,
  ): Promise<void> {
    const template = {
      pk,
      sk,
      entityId,
      entityType: this.#entityType,
      payload: new Uint8Array(),
    }

    return this.putRow(template, value, options)
  }

  /** Writes `value` at `pk` with an empty sort key, which is stored as `"VIEW"`. */
  async putAt(pk: string, entityId: string, value: T, options?: CallOptions): Promise<void> {
    return this.put(pk, '', entityId, value, options)
  }

  /**
   * Writes `value` as the payload of a copy of `template` (Go `PutJSONView`): its keys, owner,
   * entity type and `ttl` are kept, and its payload is replaced. The template isn't changed. A
   * value the codec refuses fails with `invalidArgument`, naming the row.
   */
  async putRow(template: ViewRow, value: T, options?: CallOptions): Promise<void> {
    let payload: Uint8Array
    try {
      payload = this.#codec.encode(value)
    } catch (error) {
      throw new EvtError(
        'invalidArgument',
        `encode JSON view pk=${JSON.stringify(template.pk)} sk=${JSON.stringify(template.sk)}: ` +
          messageOf(error),
        {cause: error},
      )
    }

    return this.#repo.putView({...template, payload}, options)
  }

  /**
   * The values of a partition, ascending by `sk`. A row that doesn't decode is skipped and
   * reported to `onDecodeError`; {@link listByPkStrict} fails instead.
   */
  async listByPk(pk: string, options: LenientListOptions = {}): Promise<T[]> {
    return this.#decodeValid(await this.#repo.listViewsByPk(pk, options), options.onDecodeError)
  }

  /** The values of the codec's entity type, skipping rows that don't decode, as {@link listByPk}. */
  async listAll(options: LenientListOptions = {}): Promise<T[]> {
    const rows = await this.#repo.listViewsByEntityType(this.#entityType, options)

    return this.#decodeValid(rows, options.onDecodeError)
  }

  /** Every value of a partition; the first row that doesn't decode fails the listing. */
  async listByPkStrict(pk: string, options?: CallOptions): Promise<T[]> {
    return (await this.#repo.listViewsByPk(pk, options)).map((row) => this.decode(row))
  }

  /** Every value of the codec's entity type; the first row that doesn't decode fails it. */
  async listAllStrict(options?: CallOptions): Promise<T[]> {
    const rows = await this.#repo.listViewsByEntityType(this.#entityType, options)

    return rows.map((row) => this.decode(row))
  }

  /** One page of a partition's values, strictly decoded, and the cursor of the next page. */
  async listByPkPaged(pk: string, options?: PageOptions): Promise<ValuePage<T>> {
    const page = await this.#repo.listViewsByPkPaged(pk, options)

    return {
      values: page.views.map((row) => this.decode(row)),
      nextCursor: page.nextCursor,
    }
  }

  /**
   * Binds a function from a key to its `pk`, for the common one-row-per-entity shape (Go
   * `viewcodec.NewSingle`). `entityIdOf` derives the owner a put stores; without it the owner is
   * empty.
   */
  single<K>(pkFor: (id: K) => string, entityIdOf?: (value: T) => string): SingleView<K, T> {
    return new SingleView(this, pkFor, entityIdOf)
  }

  #decodeValid(rows: ViewRow[], onDecodeError: DecodeErrorHandler | undefined): T[] {
    const values: T[] = []
    for (const row of rows) {
      try {
        values.push(this.decode(row))
      } catch (error) {
        if (!(error instanceof ViewDecodeError)) {
          throw error
        }
        onDecodeError?.(row, error)
      }
    }

    return values
  }
}

/**
 * A {@link ViewCodec} with a `pk` function: concise `get(id)` and `put(id, value)` for a view with
 * one row per entity (Go `viewcodec.Single`, Rust `SingleView`).
 */
export class SingleView<K, T> {
  readonly #codec: ViewCodec<T>
  readonly #pkFor: (id: K) => string
  readonly #entityIdOf: ((value: T) => string) | undefined

  constructor(codec: ViewCodec<T>, pkFor: (id: K) => string, entityIdOf?: (value: T) => string) {
    this.#codec = codec
    this.#pkFor = pkFor
    this.#entityIdOf = entityIdOf
  }

  /** The codec, for lookups outside the bound `pk` function, such as a legacy-key fallback. */
  get codec(): ViewCodec<T> {
    return this.#codec
  }

  /** The view keyed by `id`, or null. */
  async get(id: K, options?: CallOptions): Promise<T | null> {
    return this.#codec.get(this.#pkFor(id), options)
  }

  /** Writes `value` at the `pk` of `id`, owned by the entity ID the value derives. */
  async put(id: K, value: T, options?: CallOptions): Promise<void> {
    const entityId = this.#entityIdOf?.(value) ?? ''

    return this.#codec.putAt(this.#pkFor(id), entityId, value, options)
  }

  /** The values of the codec's entity type; see {@link ViewCodec.listAll}. */
  async listAll(options?: LenientListOptions): Promise<T[]> {
    return this.#codec.listAll(options)
  }
}

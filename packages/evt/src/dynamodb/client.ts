// The DynamoDB calls the repository makes, as a structural interface. The AWS SDK's aggregated
// `DynamoDB` client (`@aws-sdk/client-dynamodb`) satisfies it, so this package imports nothing
// from the SDK. Requests and responses use the low-level `AttributeValue` maps, never the document
// client's marshalling, because the storage format pins every attribute's type.

import type {Item} from './item.js'

/** The options the repository passes with every call. */
export interface RequestOptions {
  abortSignal?: AbortSignal
}

/** One action of a `TransactWriteItems` call. */
export interface TransactItem {
  ConditionCheck?: ConditionCheckAction
  Put?: PutAction
  Update?: UpdateAction
  Delete?: DeleteAction
}

/** The expression parts an action or a query may carry. */
export interface Expressions {
  ConditionExpression?: string
  ExpressionAttributeNames?: Record<string, string>
  ExpressionAttributeValues?: Item
}

export interface ConditionCheckAction extends Expressions {
  TableName: string
  Key: Item
  ConditionExpression: string
}

export interface PutAction extends Expressions {
  TableName: string
  Item: Item
}

export interface UpdateAction extends Expressions {
  TableName: string
  Key: Item
  UpdateExpression: string
}

export interface DeleteAction extends Expressions {
  TableName: string
  Key: Item
}

export interface GetItemInput {
  TableName: string
  Key: Item
  ConsistentRead?: boolean
  ProjectionExpression?: string
}

export interface PutItemInput extends Expressions {
  TableName: string
  Item: Item
}

export interface QueryInput {
  TableName: string
  KeyConditionExpression: string
  ExpressionAttributeValues: Item
  ConsistentRead?: boolean
  ProjectionExpression?: string
  ScanIndexForward?: boolean
  Limit?: number
  ExclusiveStartKey?: Item
}

export interface ScanInput {
  TableName: string
  ConsistentRead?: boolean
  ProjectionExpression?: string
  Limit?: number
  ExclusiveStartKey?: Item
}

/** A page of a query or a scan. Items come back as the SDK decodes them. */
export interface Page {
  Items?: Record<string, unknown>[] | undefined
  LastEvaluatedKey?: Record<string, unknown> | undefined
}

/** The calls {@link DynamoRepository} makes: the SDK's `DynamoDB` client has them all. */
export interface DynamoClient {
  getItem(
    input: GetItemInput,
    options?: RequestOptions,
  ): Promise<{Item?: Record<string, unknown> | undefined}>
  putItem(input: PutItemInput, options?: RequestOptions): Promise<unknown>
  query(input: QueryInput, options?: RequestOptions): Promise<Page>
  scan(input: ScanInput, options?: RequestOptions): Promise<Page>
  transactWriteItems(
    input: {TransactItems: TransactItem[]},
    options?: RequestOptions,
  ): Promise<unknown>
}

// @photon-grove/evt/dynamodb: the DynamoDB backend. It imports nothing from the AWS SDK: the
// repository takes the SDK's `DynamoDB` client, or anything shaped like it, as a DynamoClient.

export {
  type AttributeValue,
  type Item,
  DEFAULT_VIEW_SORT_KEY,
  HEAD_SORT_KEY,
  SNAPSHOT_SORT_KEY,
  eventFromItem,
  eventItem,
  expiry,
  headFromItem,
  headItem,
  isSnapshotItem,
  itemSize,
  snapshotFromItem,
  snapshotItem,
  sortKey,
  ttlFromItem,
  viewItem,
} from './item.js'
export {
  type ConditionCheckAction,
  type DeleteAction,
  type DynamoClient,
  type Expressions,
  type GetItemInput,
  type Page,
  type PutAction,
  type PutItemInput,
  type QueryInput,
  type RequestOptions,
  type ScanInput,
  type TransactItem,
  type UpdateAction,
} from './client.js'
export {FEATURES_PK, REBUILD_LOCK_PK, manifestFromItem, markerItem} from './marker.js'
export {
  OUTCOME_CONDITION,
  OUTCOME_SORT_KEY,
  outcomeFromItem,
  outcomeItem,
  outcomeItemSize,
  outcomeKey,
} from './outcome.js'
export {DynamoRepository, type DynamoRepositoryOptions} from './repository.js'
export {DYNAMODB_BACKEND, transactItemSize, viewProjector} from './views.js'
export {
  type CommitTtl,
  type SnapshotTtl,
  commitTransaction,
  snapshotTransaction,
  transactionItems,
} from './requests.js'

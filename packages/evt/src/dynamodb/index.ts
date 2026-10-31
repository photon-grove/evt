// @photon-grove/evt/dynamodb: the DynamoDB backend. So far it holds the item codec, which imports
// nothing from the AWS SDK.

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
  viewItem,
} from './item.js'

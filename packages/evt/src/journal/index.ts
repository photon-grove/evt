// @photon-grove/evt/journal: the journal (ADR 0005), per-scope ordered, replayable delivery rows
// written in the commit that wrote the events. The stored shapes are the storage format's
// "Journal" section, and compat/fixtures/journal.json, which Rust generates, pins them.

export {
  type Allocated,
  Journal,
  type JournalOptions,
  JournalRowTooLargeError,
  type ScopeResolver,
  allocate,
} from './config.js'
export {PROJECTOR_NAME, PositionTaken, journalProjector} from './projector.js'
export {
  type Gap,
  type JournalPage,
  type JournalStore,
  MAX_PAGE_ROWS,
  highWatermark,
  page,
} from './read.js'
export {type JournalState, replay, stateViews} from './replay.js'
export {
  DEFAULT_MAX_ROW_BYTES,
  type JournalRow,
  MAX_ROW_BYTES,
  counterPayload,
  counterView,
  decodeRowPayload,
  encodeRow,
  journalRowFromView,
  readCounter,
  rowSize,
  rowView,
} from './row.js'
export {
  ALLOCATION_KEY,
  type Allocation,
  COUNTER_ENTITY_TYPE,
  COUNTER_OWNER,
  COUNTER_PARTITION_PREFIX,
  COUNTER_SORT_KEY,
  MAX_POSITION,
  MAX_SCOPE_BYTES,
  POSITION_WIDTH,
  ROW_ENTITY_TYPE,
  ROW_PARTITION_PREFIX,
  allocations,
  checkScope,
  counterPartitionKey,
  encodeAllocations,
  parseRowSortKey,
  rowPartitionKey,
  rowSortKey,
  setAllocations,
} from './scope.js'

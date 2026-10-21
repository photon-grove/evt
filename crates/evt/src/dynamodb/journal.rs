//! [`JournalStore`] and [`CompactionStore`] for the DynamoDB repository, over the views table the
//! commit path writes to. Every read is strongly consistent: a journal reader must see every row a
//! committed transaction wrote, or its gap detection would report rows still in flight as lost.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::{
    AttributeValue, ConditionCheck, Delete, KeysAndAttributes, TransactWriteItem,
};

use super::{
    DynamoRepository, cancellation_reasons, from_sdk_item, put, s, sdk_error, to_sdk_item,
};
use crate::error::Error;
use crate::journal::{
    COUNTER_OWNER, COUNTER_PARTITION_PREFIX, COUNTER_SORT_KEY, CompactionStore, CompactionWrite,
    CompactionWritten, Expect, JournalCompactionStore, JournalRow, JournalStore, Scope,
    compaction_pk, parse_row_sk, read_counter, row_sk,
};
use crate::projection::ViewRow;
use crate::rebuild::RebuildEpoch;
use crate::wire::dynamo::{self as wire, MAX_VIEW_BATCH_GET, view_row_from_item};

impl DynamoRepository {
    fn journal_table(&self) -> Result<&str, Error> {
        self.views_table().ok_or(Error::ViewsNotConfigured {
            backend: "dynamodb",
        })
    }
}

impl JournalStore for DynamoRepository {
    async fn read_counter(&self, scope: &Scope) -> Result<u64, Error> {
        let output = self
            .client
            .get_item()
            .table_name(self.journal_table()?)
            .consistent_read(true)
            .key("pk", s(&scope.counter_pk()))
            .key("sk", s(COUNTER_SORT_KEY))
            .send()
            .await
            .map_err(sdk_error)?;

        match output.item {
            Some(item) if !item.is_empty() => {
                read_counter(scope, &view_row_from_item(&from_sdk_item(&item))?)
            }
            _ => Ok(0),
        }
    }

    async fn read_counters(&self, scopes: &[Scope]) -> Result<Vec<u64>, Error> {
        // One strongly consistent BatchGetItem per 100 scopes, re-sending what DynamoDB leaves
        // unprocessed; a single scope is a plain GetItem.
        if let [scope] = scopes {
            return Ok(vec![self.read_counter(scope).await?]);
        }

        let table = self.journal_table()?;
        let mut found: HashMap<String, u64> = HashMap::new();
        let distinct: Vec<&Scope> = scopes
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();

        for chunk in distinct.chunks(MAX_VIEW_BATCH_GET) {
            let keys: Vec<HashMap<String, AttributeValue>> = chunk
                .iter()
                .map(|scope| {
                    HashMap::from([
                        ("pk".to_owned(), s(&scope.counter_pk())),
                        ("sk".to_owned(), s(COUNTER_SORT_KEY)),
                    ])
                })
                .collect();
            let mut pending = HashMap::from([(
                table.to_owned(),
                KeysAndAttributes::builder()
                    .set_keys(Some(keys))
                    .consistent_read(true)
                    .build()
                    .map_err(Error::backend)?,
            )]);

            for attempt in 1..=3 {
                let output = self
                    .client
                    .batch_get_item()
                    .set_request_items(Some(pending))
                    .send()
                    .await
                    .map_err(sdk_error)?;

                for item in output
                    .responses
                    .unwrap_or_default()
                    .remove(table)
                    .unwrap_or_default()
                {
                    let row = view_row_from_item(&from_sdk_item(&item))?;
                    let scope = row
                        .pk
                        .strip_prefix(COUNTER_PARTITION_PREFIX)
                        .and_then(|name| Scope::new(name).ok())
                        .ok_or_else(|| {
                            Error::InvalidRecord(format!("{:?} is not a journal counter", row.pk))
                        })?;

                    found.insert(row.pk.clone(), read_counter(&scope, &row)?);
                }

                pending = output
                    .unprocessed_keys
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|(_, keys)| !keys.keys().is_empty())
                    .collect();

                if pending.is_empty() {
                    break;
                }

                if attempt == 3 {
                    return Err(Error::Transient(
                        "reading journal counters left unprocessed keys".into(),
                    ));
                }
            }
        }

        Ok(scopes
            .iter()
            .map(|scope| found.get(&scope.counter_pk()).copied().unwrap_or(0))
            .collect())
    }

    async fn read_last_position(&self, scope: &Scope) -> Result<u64, Error> {
        let output = self
            .client
            .query()
            .table_name(self.journal_table()?)
            .consistent_read(true)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", s(&scope.row_pk()))
            .scan_index_forward(false)
            .limit(1)
            .send()
            .await
            .map_err(sdk_error)?;

        match output.items().first() {
            None => Ok(0),
            Some(item) => {
                let row = view_row_from_item(&from_sdk_item(item))?;

                parse_row_sk(&row.sk).ok_or_else(|| {
                    Error::InvalidRecord(format!(
                        "journal row {:?}/{:?} has no position",
                        row.pk, row.sk
                    ))
                })
            }
        }
    }

    async fn read_rows(
        &self,
        scope: &Scope,
        after: u64,
        limit: usize,
    ) -> Result<Vec<JournalRow>, Error> {
        let table = self.journal_table()?;
        let mut rows = Vec::new();
        let mut start: Option<HashMap<String, AttributeValue>> = None;

        while rows.len() < limit {
            let output = self
                .client
                .query()
                .table_name(table)
                .consistent_read(true)
                .key_condition_expression("pk = :pk AND sk > :after")
                .expression_attribute_values(":pk", s(&scope.row_pk()))
                .expression_attribute_values(":after", s(&row_sk(after)))
                .limit(i32::try_from(limit - rows.len()).unwrap_or(i32::MAX))
                .set_exclusive_start_key(start.take())
                .send()
                .await
                .map_err(sdk_error)?;

            for item in output.items() {
                rows.push(JournalRow::from_view(
                    scope,
                    &view_row_from_item(&from_sdk_item(item))?,
                )?);
            }

            match output.last_evaluated_key() {
                Some(key) if !key.is_empty() => start = Some(key.clone()),
                _ => break,
            }
        }

        rows.truncate(limit);

        Ok(rows)
    }
}

impl JournalCompactionStore for DynamoRepository {
    async fn read_compaction_row(
        &self,
        scope: &Scope,
        sort_key: &str,
    ) -> Result<Option<ViewRow>, Error> {
        let output = self
            .client
            .get_item()
            .table_name(self.journal_table()?)
            .consistent_read(true)
            .key("pk", s(&compaction_pk(scope)))
            .key("sk", s(sort_key))
            .send()
            .await
            .map_err(sdk_error)?;

        output
            .item
            .filter(|item| !item.is_empty())
            .map(|item| view_row_from_item(&from_sdk_item(&item)))
            .transpose()
    }
}

/// The text a view payload is stored as: DynamoDB keeps it as a string.
fn payload_text(payload: &[u8]) -> Result<String, Error> {
    String::from_utf8(payload.to_vec())
        .map_err(|_| Error::InvalidArgument("a compaction payload must be UTF-8".into()))
}

impl DynamoRepository {
    /// The transaction item of one compaction write.
    fn compaction_item(
        &self,
        table: &str,
        write: &CompactionWrite,
    ) -> Result<TransactWriteItem, Error> {
        let condition = |expect: &Expect| -> Result<_, Error> {
            Ok(match expect {
                Expect::Any => (None, None),
                Expect::Absent => (Some(wire::VIEW_ABSENT_CONDITION), None),
                Expect::Payload(payload) => (
                    Some(wire::COMPACTION_PAYLOAD_CONDITION),
                    Some(s(&payload_text(payload)?)),
                ),
            })
        };

        match write {
            CompactionWrite::Put { row, expect } => {
                let (condition, value) = condition(expect)?;
                let values: Vec<_> = value
                    .into_iter()
                    .flat_map(|value| [(":expected", value), (":engine", s(COUNTER_OWNER))])
                    .collect();

                put(table, wire::view_row_item(row)?, condition, &values)
            }
            CompactionWrite::Delete { pk, sk, owner } => {
                let delete = Delete::builder()
                    .table_name(table)
                    .key("pk", s(pk))
                    .key("sk", s(sk))
                    .condition_expression(wire::VIEW_DELETE_CONDITION)
                    .expression_attribute_values(wire::OWNER_PLACEHOLDER, s(owner))
                    .build()
                    .map_err(Error::backend)?;

                Ok(TransactWriteItem::builder().delete(delete).build())
            }
            CompactionWrite::Check { pk, sk, expect } => {
                let (condition, value) = condition(expect)?;
                let mut check = ConditionCheck::builder()
                    .table_name(table)
                    .key("pk", s(pk))
                    .key("sk", s(sk))
                    // A check needs a condition; `Any` checks that the key is a key.
                    .condition_expression(
                        condition.unwrap_or("attribute_not_exists(pk) OR attribute_exists(pk)"),
                    );
                if let Some(value) = value {
                    check = check
                        .expression_attribute_values(":expected", value)
                        .expression_attribute_values(":engine", s(COUNTER_OWNER));
                }

                Ok(TransactWriteItem::builder()
                    .condition_check(check.build().map_err(Error::backend)?)
                    .build())
            }
        }
    }

    /// The check that the rebuild generation is still `epoch`'s, on the events table.
    fn epoch_check(&self, epoch: RebuildEpoch) -> Result<TransactWriteItem, Error> {
        let generation = epoch.generation();
        let mut check = ConditionCheck::builder()
            .table_name(&self.table)
            .set_key(Some(to_sdk_item(&wire::rebuild_generation_key())))
            .expression_attribute_names(wire::REBUILD_VALUE_NAME, "value");

        check = if generation == 0 {
            check.condition_expression(wire::COMPACTION_EPOCH_ZERO_CONDITION)
        } else {
            check
                .condition_expression(wire::COMPACTION_EPOCH_CONDITION)
                .expression_attribute_values(":epoch", super::n(generation))
        };

        Ok(TransactWriteItem::builder()
            .condition_check(check.build().map_err(Error::backend)?)
            .build())
    }
}

/// One `TransactWriteItems` per call: the writes in order, then the epoch check on the events
/// table. A failed condition names the write (or the epoch check) that failed it; a concurrent
/// transaction or throttling is transient, since every write here can be sent again.
impl CompactionStore for DynamoRepository {
    async fn write_compaction(
        &self,
        epoch: Option<RebuildEpoch>,
        writes: &[CompactionWrite],
    ) -> Result<CompactionWritten, Error> {
        crate::journal::check_compaction_writes(writes)?;

        let table = self.journal_table()?.to_owned();
        let mut items = writes
            .iter()
            .map(|write| self.compaction_item(&table, write))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(epoch) = epoch {
            items.push(self.epoch_check(epoch)?);
        }

        let Err(err) = self
            .client
            .transact_write_items()
            .set_transact_items(Some(items))
            .send()
            .await
        else {
            return Ok(CompactionWritten::Written);
        };

        let codes = cancellation_reasons(&err);
        if codes.get(writes.len()).copied().flatten() == Some("ConditionalCheckFailed") {
            return Ok(CompactionWritten::EpochChanged);
        }

        if let Some(at) = codes
            .iter()
            .take(writes.len())
            .position(|code| *code == Some("ConditionalCheckFailed"))
        {
            return Ok(CompactionWritten::Refused(at));
        }

        let transient = codes.iter().any(|code| {
            matches!(
                code,
                Some("TransactionConflict" | "ThrottlingError" | "ProvisionedThroughputExceeded")
            )
        });
        if transient {
            return Err(Error::Transient(Box::new(err)));
        }

        Err(sdk_error(err))
    }

    fn compaction_now(&self) -> i64 {
        self.now()
    }
}

//! [`JournalStore`] for the DynamoDB repository, over the views table the commit path writes to.
//! Both reads are strongly consistent: a journal reader must see every row a committed
//! transaction wrote, or its gap detection would report rows still in flight as lost.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes};

use super::{DynamoRepository, from_sdk_item, s, sdk_error};
use crate::error::Error;
use crate::journal::{
    COUNTER_PARTITION_PREFIX, COUNTER_SORT_KEY, JournalRow, JournalStore, Scope, parse_row_sk,
    read_counter, row_sk,
};
use crate::wire::dynamo::{MAX_VIEW_BATCH_GET, view_row_from_item};

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

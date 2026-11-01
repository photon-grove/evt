//! [`ViewRepository`] over a DynamoDB views table, item-for-item compatible with Go's
//! `dynamo.ViewRepository`.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::time::Duration;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::AsyncSleep;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;

use super::{DynamoRepository, from_sdk_item, s, sdk, sdk_error, to_sdk, to_sdk_item};
use crate::error::Error;
use crate::projection::ViewRow;
use crate::views::{
    DEFAULT_VIEW_SK, ViewPage, ViewRepository, check_view_row, nul_free, resolve_key,
};
use crate::wire::dynamo::{
    self as wire, MAX_VIEW_BATCH_GET, MAX_VIEW_BATCH_WRITE, ViewQuery, encode_view_cursor,
    view_key, view_query_by_entity_type, view_query_by_pk, view_row_from_item, view_row_item,
};

/// How many times a batch call re-sends what DynamoDB left unprocessed before it gives up.
const MAX_BATCH_ATTEMPTS: u32 = 3;

/// Reads and writes view rows in a DynamoDB views table: string partition key `pk`, string sort
/// key `sk`, and a global secondary index named `entityType-index` on `entityType` (the table the
/// commit path writes view operations to, see
/// [`DynamoRepository::with_views_table`](super::DynamoRepository::with_views_table)). An item
/// holds `pk`, `sk`, `entityID` (the owner), `entityType`, `payload`, and an optional `ttl`.
///
/// Reads of the base table are strongly consistent, as in Go: point reads always, and partition
/// listings ([`list_views_by_pk_paged`](ViewRepository::list_views_by_pk_paged) and
/// [`list_views_by_pk`](ViewRepository::list_views_by_pk)) unless
/// [`with_consistent_read(false)`](Self::with_consistent_read) opts them out. A row a commit just
/// wrote is therefore seen by the next read of its key or partition. A strongly consistent read
/// costs twice the read capacity of an eventually consistent one. Entity-type listings read the
/// `entityType-index` global secondary index, which DynamoDB can only read eventually
/// consistently, so they can lag behind a commit.
///
/// Writes go through `BatchWriteItem` like Go's, so a write is not conditional and not part of any
/// commit. Clones share the client.
#[derive(Clone, Debug)]
pub struct DynamoViewRepository {
    client: Client,
    table: String,
    /// Whether partition listings read strongly consistently (default `true`).
    consistent_read: bool,
}

impl DynamoViewRepository {
    /// A view repository over `table`, with strongly consistent partition listings.
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
            consistent_read: true,
        }
    }

    /// Reads partition listings eventually consistently, at half the read capacity, when `false`
    /// (Go `WithConsistentRead`); a listing may then miss a row a commit just wrote. Point reads
    /// stay strongly consistent and entity-type listings stay eventually consistent either way.
    #[must_use]
    pub fn with_consistent_read(mut self, consistent: bool) -> Self {
        self.consistent_read = consistent;
        self
    }

    /// The DynamoDB client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// The views table name.
    pub fn table(&self) -> &str {
        &self.table
    }

    fn backoff(&self, duration: Duration) -> impl Future<Output = ()> + Send + use<> {
        let sleeper = self.client.config().sleep_impl();

        async move {
            if let Some(sleeper) = sleeper {
                sleeper.sleep(duration).await;
            }
        }
    }

    /// Sends one `BatchWriteItem` of at most [`MAX_VIEW_BATCH_WRITE`] requests, re-sending what
    /// DynamoDB reports unprocessed.
    async fn write_batch(&self, writes: Vec<sdk::WriteRequest>) -> Result<(), Error> {
        if writes.is_empty() {
            return Ok(());
        }

        let mut pending = HashMap::from([(self.table.clone(), writes)]);

        for attempt in 1..=MAX_BATCH_ATTEMPTS {
            let output = self
                .client
                .batch_write_item()
                .set_request_items(Some(pending))
                .send()
                .await
                .map_err(sdk_error)?;

            pending = output
                .unprocessed_items
                .unwrap_or_default()
                .into_iter()
                .filter(|(_, writes)| !writes.is_empty())
                .collect();

            if pending.is_empty() {
                return Ok(());
            }

            if attempt == MAX_BATCH_ATTEMPTS {
                return Err(Error::Transient(
                    format!("batch write returned unprocessed items after {attempt} attempts")
                        .into(),
                ));
            }

            self.backoff(Duration::from_millis(u64::from(attempt) * 25))
                .await;
        }

        Ok(())
    }

    async fn get_chunk(
        &self,
        keys: Vec<HashMap<String, sdk::AttributeValue>>,
    ) -> Result<Vec<ViewRow>, Error> {
        let keys = sdk::KeysAndAttributes::builder()
            .set_keys(Some(keys))
            .consistent_read(true)
            .build()
            .map_err(Error::backend)?;
        let mut pending = HashMap::from([(self.table.clone(), keys)]);
        let mut views = Vec::new();

        for attempt in 1..=MAX_BATCH_ATTEMPTS {
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
                .remove(&self.table)
                .unwrap_or_default()
            {
                views.push(view_row_from_item(&from_sdk_item(&item))?);
            }

            pending = output
                .unprocessed_keys
                .unwrap_or_default()
                .into_iter()
                .filter(|(_, keys)| !keys.keys().is_empty())
                .collect();

            if pending.is_empty() {
                return Ok(views);
            }

            if attempt == MAX_BATCH_ATTEMPTS {
                return Err(Error::Transient(
                    format!("batch get returned unprocessed keys after {attempt} attempts").into(),
                ));
            }

            self.backoff(Duration::from_millis(u64::from(attempt) * 25))
                .await;
        }

        Ok(views)
    }

    async fn query_page(&self, query: ViewQuery) -> Result<ViewPage, Error> {
        // Only a base-table query can be consistent; an index query sends no `ConsistentRead`.
        let consistent = query.reads_base_table().then_some(self.consistent_read);
        let output = apply(self.client.query().table_name(&self.table), query)
            .set_consistent_read(consistent)
            .send()
            .await
            .map_err(sdk_error)?;

        let views = output
            .items()
            .iter()
            .map(|item| view_row_from_item(&from_sdk_item(item)))
            .collect::<Result<Vec<_>, _>>()?;

        let next_cursor = match output.last_evaluated_key() {
            Some(key) if !key.is_empty() => Some(encode_view_cursor(&from_sdk_item(key))?),
            _ => None,
        };

        Ok(ViewPage::new(views, next_cursor))
    }
}

fn apply(mut request: QueryFluentBuilder, query: ViewQuery) -> QueryFluentBuilder {
    request = request
        .set_index_name(query.index.map(str::to_owned))
        .key_condition_expression(query.key_condition)
        .set_limit(query.limit);

    for (name, value) in &query.values {
        request = request.expression_attribute_values(name, to_sdk(value));
    }

    request.set_exclusive_start_key(query.start_key.as_ref().map(to_sdk_item))
}

fn write_request(item: wire::Item) -> Result<sdk::WriteRequest, Error> {
    let put = sdk::PutRequest::builder()
        .set_item(Some(to_sdk_item(&item)))
        .build()
        .map_err(Error::backend)?;

    Ok(sdk::WriteRequest::builder().put_request(put).build())
}

impl ViewRepository for DynamoViewRepository {
    async fn get_view_at(&self, pk: &str, sk: &str) -> Result<Option<ViewRow>, Error> {
        let sk = resolve_key(pk, sk)?;
        let output = self
            .client
            .get_item()
            .table_name(&self.table)
            .consistent_read(true)
            .key("pk", s(pk))
            .key("sk", s(sk))
            .send()
            .await
            .map_err(sdk_error)?;

        match output.item {
            Some(item) if !item.is_empty() => Ok(Some(view_row_from_item(&from_sdk_item(&item))?)),
            _ => Ok(None),
        }
    }

    async fn put_view(&self, view: &ViewRow) -> Result<(), Error> {
        check_view_row(view)?;

        self.write_batch(vec![write_request(view_row_item(view)?)?])
            .await
    }

    async fn put_views(&self, views: &[ViewRow]) -> Result<(), Error> {
        for view in views {
            check_view_row(view)?;
        }

        // BatchWriteItem refuses a request that repeats a key, so a repeated `(pk, sk)` keeps its
        // last row, as sequential puts would.
        let mut last: HashMap<(&str, &str), usize> = HashMap::with_capacity(views.len());
        for (index, view) in views.iter().enumerate() {
            let sk = if view.sk.is_empty() {
                DEFAULT_VIEW_SK
            } else {
                &view.sk
            };
            last.insert((&view.pk, sk), index);
        }

        let mut writes = Vec::with_capacity(last.len());
        for (index, view) in views.iter().enumerate() {
            let sk = if view.sk.is_empty() {
                DEFAULT_VIEW_SK
            } else {
                &view.sk
            };
            if last[&(view.pk.as_str(), sk)] == index {
                writes.push(write_request(view_row_item(view)?)?);
            }
        }

        let mut writes = writes.into_iter();

        loop {
            let chunk: Vec<_> = writes.by_ref().take(MAX_VIEW_BATCH_WRITE).collect();
            if chunk.is_empty() {
                break;
            }

            self.write_batch(chunk).await?;
        }

        Ok(())
    }

    async fn delete_view(&self, pk: &str, sk: &str) -> Result<(), Error> {
        let sk = resolve_key(pk, sk)?;

        let delete = sdk::DeleteRequest::builder()
            .set_key(Some(to_sdk_item(&view_key(pk, sk))))
            .build()
            .map_err(Error::backend)?;

        self.write_batch(vec![
            sdk::WriteRequest::builder().delete_request(delete).build(),
        ])
        .await
    }

    async fn batch_get_views_at(&self, pks: &[&str], sk: &str) -> Result<Vec<ViewRow>, Error> {
        let sk = resolve_key("-", sk)?;
        for pk in pks {
            nul_free(pk, "pk")?;
        }

        let mut seen = BTreeSet::new();
        let keys: Vec<_> = pks
            .iter()
            .filter(|pk| !pk.is_empty() && seen.insert(**pk))
            .map(|pk| to_sdk_item(&view_key(pk, sk)))
            .collect();

        let mut views = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(MAX_VIEW_BATCH_GET) {
            views.extend(self.get_chunk(chunk.to_vec()).await?);
        }

        Ok(views)
    }

    async fn list_views_by_pk_paged(
        &self,
        pk: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<ViewPage, Error> {
        nul_free(pk, "pk")?;
        self.query_page(view_query_by_pk(pk, limit, cursor)?).await
    }

    async fn list_views_by_entity_type_paged(
        &self,
        entity_type: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<ViewPage, Error> {
        nul_free(entity_type, "entity type")?;
        self.query_page(view_query_by_entity_type(entity_type, limit, cursor)?)
            .await
    }
}

impl DynamoRepository {
    /// A [`ViewRepository`] over the views table [`with_views_table`](Self::with_views_table)
    /// configured, the table the commit path writes view operations to, with the same client.
    /// Its reads follow [`DynamoViewRepository::new`] (strongly consistent partition listings),
    /// whatever this repository's [`with_consistent_read`](Self::with_consistent_read).
    /// Refused with [`Error::ViewsNotConfigured`] when there is none.
    pub fn view_repository(&self) -> Result<DynamoViewRepository, Error> {
        let table = self.views_table.clone().ok_or(Error::ViewsNotConfigured {
            backend: "dynamodb",
        })?;

        Ok(DynamoViewRepository::new(self.client.clone(), table))
    }
}

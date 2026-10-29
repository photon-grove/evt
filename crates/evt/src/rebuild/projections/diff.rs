//! A diff-mode rebuild (`ProjectionRebuild::diff`): the plan compared with the rows the rebuild
//! owns, so only missing and changed rows are put and owned rows the plan no longer produces are
//! deleted.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;

use super::{Output, RowChanges};
use crate::error::Error;
use crate::projection::{ProjectedView, ViewOperation, ViewOperationKind, ViewRow, check_views};
use crate::rebuild::RebuildFence;
use crate::views::{ViewPage, ViewRepository};

/// The provenance a diff gives the delete of an owned row the plan does not produce. Its
/// projector index is `usize::MAX`, after every registered projector.
pub(super) const STALE_ROWS: &str = "rebuild-stale-rows";

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Where a diff reads the rows it owns: a [`ViewRepository`] over the table the rebuild writes,
/// with its type erased.
pub(super) trait Rows: Send + Sync {
    /// One page of the rows with the entity type (a listing that may lag behind recent writes).
    fn list<'a>(
        &'a self,
        entity_type: &'a str,
        cursor: Option<&'a str>,
    ) -> BoxFuture<'a, Result<ViewPage, Error>>;

    /// The rows at `pks` and `sk` that exist, read by key (strongly consistent).
    fn read<'a>(
        &'a self,
        pks: Vec<&'a str>,
        sk: &'a str,
    ) -> BoxFuture<'a, Result<Vec<ViewRow>, Error>>;
}

impl<V: ViewRepository> Rows for V {
    fn list<'a>(
        &'a self,
        entity_type: &'a str,
        cursor: Option<&'a str>,
    ) -> BoxFuture<'a, Result<ViewPage, Error>> {
        Box::pin(self.list_views_by_entity_type_paged(entity_type, None, cursor))
    }

    fn read<'a>(
        &'a self,
        pks: Vec<&'a str>,
        sk: &'a str,
    ) -> BoxFuture<'a, Result<Vec<ViewRow>, Error>> {
        Box::pin(async move { self.batch_get_views_at(&pks, sk).await })
    }
}

type Key = (String, String);

impl<R: RebuildFence> Output<'_, R> {
    /// Replaces the planned operations with the difference between them and the rows `rows` holds
    /// of the `owned` types:
    ///
    /// - a planned put is kept when the row is missing or differs, and dropped (counted unchanged)
    ///   when a read by key finds it as planned;
    /// - a planned delete is kept when a read by key finds the row; one that finds a row of a type
    ///   the diff does not own fails with [`Error::InvalidConfig`];
    /// - an owned row the plan does not produce is deleted when a read by key still finds it with
    ///   an owned type, and its owner is a stream the run read (`streams`); one owned by a stream
    ///   the run did not read (an unregistered type's) is kept and counted.
    ///
    /// A put of a type the diff does not own is refused with [`Error::InvalidConfig`]: the diff
    /// could neither tell it apart from another writer's row nor delete it once stale. The final
    /// operations are checked as a guarded write checks them, so a row no batch could write fails
    /// here, before any write, rather than in the middle of writing.
    pub(super) async fn diff(
        &mut self,
        rows: &dyn Rows,
        owned: &BTreeSet<String>,
        streams: &BTreeSet<String>,
    ) -> Result<(), Error> {
        let planned = std::mem::take(&mut self.planned);
        if let Some(item) = planned.iter().find(|item| {
            item.op.kind == ViewOperationKind::Put && !owned.contains(&item.op.entity_type)
        }) {
            return Err(Error::InvalidConfig(format!(
                "projector {:?} plans a row of type {:?} at ({:?}, {:?}), which the diff does not \
                 own: add the type to the owned types",
                item.projector, item.op.entity_type, item.op.pk, item.op.sk
            )));
        }

        let mut listed: BTreeMap<Key, ViewRow> = BTreeMap::new();
        for entity_type in owned {
            let mut cursor: Option<String> = None;
            loop {
                let page = rows.list(entity_type, cursor.as_deref()).await?;
                for row in page.views {
                    listed.insert((row.pk.clone(), row.sk.clone()), row);
                }
                self.keep_alive().await?;

                match page.next_cursor {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }
        }
        self.report.existing = listed.len();

        // A listing may lag behind the table, so nothing is skipped or deleted on its word alone:
        // the rows it shows as already planned, the planned deletes, and the rows only it shows are
        // read again by key. A planned row it misses or shows differently is simply written.
        let mut confirm: Vec<Key> = Vec::new();
        for item in &planned {
            let key = (item.op.pk.clone(), item.op.sk.clone());
            let shown = listed.remove(&key);
            let check = match item.op.kind {
                ViewOperationKind::Put => shown.is_some_and(|row| same(&item.op, &row)),
                ViewOperationKind::Delete => true,
            };
            if check {
                confirm.push(key);
            }
        }
        let stale: Vec<Key> = listed.into_keys().collect();
        confirm.extend(stale.iter().cloned());

        let stored = self.read_by_key(rows, &confirm).await?;

        let mut changes: BTreeMap<String, RowChanges> = BTreeMap::new();
        let mut operations = Vec::with_capacity(planned.len());
        for item in planned {
            let key = (item.op.pk.clone(), item.op.sk.clone());
            match item.op.kind {
                ViewOperationKind::Put => {
                    if stored.get(&key).is_some_and(|row| same(&item.op, row)) {
                        self.report.unchanged += 1;
                        continue;
                    }

                    changes.entry(item.op.entity_type.clone()).or_default().puts += 1;
                }
                ViewOperationKind::Delete => {
                    let Some(row) = stored.get(&key) else {
                        continue;
                    };
                    if !owned.contains(&row.entity_type) {
                        return Err(Error::InvalidConfig(format!(
                            "projector {:?} deletes the row at ({:?}, {:?}), which holds a row of \
                             type {:?} the diff does not own",
                            item.projector, key.0, key.1, row.entity_type
                        )));
                    }

                    changes.entry(row.entity_type.clone()).or_default().deletes += 1;
                }
            }
            operations.push(item);
        }

        for key in stale {
            let Some(row) = stored.get(&key) else {
                continue;
            };
            if !owned.contains(&row.entity_type) {
                continue;
            }
            if !streams.contains(&row.entity_id) {
                self.report.kept += 1;
                continue;
            }

            changes.entry(row.entity_type.clone()).or_default().deletes += 1;
            operations.push(ProjectedView::new(
                STALE_ROWS,
                usize::MAX,
                operations.len(),
                ViewOperation::delete(row.pk.clone(), row.sk.clone(), row.entity_id.as_str()),
            ));
        }

        // A row a guarded write refuses (a stale row with no owner, say, from a bad manual write)
        // fails the run here, before the first write and in the dry run alike.
        check_views(&operations)?;

        self.report.puts = changes.values().map(|change| change.puts).sum();
        self.report.deletes = changes.values().map(|change| change.deletes).sum();
        self.report.changes = changes;
        self.planned = operations;

        Ok(())
    }

    /// The rows at `keys` that exist, read by key, batched per sort key, renewing the lease
    /// between batches.
    async fn read_by_key(
        &mut self,
        rows: &dyn Rows,
        keys: &[Key],
    ) -> Result<BTreeMap<Key, ViewRow>, Error> {
        let mut by_sk: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (pk, sk) in keys {
            by_sk.entry(sk.as_str()).or_default().push(pk.as_str());
        }

        let mut stored = BTreeMap::new();
        for (sk, pks) in by_sk {
            for row in rows.read(pks, sk).await? {
                stored.insert((row.pk.clone(), row.sk.clone()), row);
            }
            self.keep_alive().await?;
        }

        Ok(stored)
    }
}

/// Whether a stored row already holds what a put would write: owner, type, payload, and no TTL
/// (a rebuilt row has none).
fn same(op: &ViewOperation, row: &ViewRow) -> bool {
    row.entity_id == op.owner.as_str()
        && row.entity_type == op.entity_type
        && row.payload == op.payload
        && row.ttl == 0
}

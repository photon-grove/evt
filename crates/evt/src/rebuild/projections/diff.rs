//! A diff-mode rebuild (`ProjectionRebuild::diff`): the plan compared with the rows the rebuild
//! owns, so only missing and changed rows are put and owned rows the plan no longer produces are
//! deleted.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;

use super::{Output, RowChanges};
use crate::error::Error;
use crate::projection::{ProjectedView, ViewOperation, ViewOperationKind, ViewRow};
use crate::rebuild::RebuildFence;
use crate::views::ViewRepository;

/// The provenance a diff gives the delete of an owned row the plan does not produce.
pub(super) const STALE_ROWS: &str = "rebuild-stale-rows";

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Where a diff reads the rows it owns: a [`ViewRepository`] over the table the rebuild writes,
/// with its type erased.
pub(super) trait Rows: Send + Sync {
    /// Every row with the entity type (a listing that may lag behind recent writes).
    fn list<'a>(&'a self, entity_type: &'a str) -> BoxFuture<'a, Result<Vec<ViewRow>, Error>>;

    /// The rows at `pks` and `sk` that exist, read by key (strongly consistent).
    fn read<'a>(
        &'a self,
        pks: Vec<&'a str>,
        sk: &'a str,
    ) -> BoxFuture<'a, Result<Vec<ViewRow>, Error>>;
}

impl<V: ViewRepository> Rows for V {
    fn list<'a>(&'a self, entity_type: &'a str) -> BoxFuture<'a, Result<Vec<ViewRow>, Error>> {
        Box::pin(self.list_views_by_entity_type(entity_type))
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
    /// - a planned delete is kept when a read by key finds the row;
    /// - an owned row the plan does not produce is deleted when a read by key still finds it, with
    ///   an owned type.
    ///
    /// A put of a type the diff does not own is refused with [`Error::InvalidConfig`]: the diff
    /// could neither tell it apart from another writer's row nor delete it once stale.
    pub(super) async fn diff(
        &mut self,
        rows: &dyn Rows,
        owned: &BTreeSet<String>,
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

        let mut listed: HashMap<Key, ViewRow> = HashMap::new();
        for entity_type in owned {
            for row in rows.list(entity_type).await? {
                listed.insert((row.pk.clone(), row.sk.clone()), row);
            }
            self.keep_alive().await?;
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

        let stored = read_by_key(rows, &confirm).await?;
        self.keep_alive().await?;

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

            changes.entry(row.entity_type.clone()).or_default().deletes += 1;
            operations.push(ProjectedView::new(
                STALE_ROWS,
                0,
                operations.len(),
                ViewOperation::delete(row.pk.clone(), row.sk.clone(), row.entity_id.as_str()),
            ));
        }

        self.report.puts = changes.values().map(|change| change.puts).sum();
        self.report.deletes = changes.values().map(|change| change.deletes).sum();
        self.report.changes = changes;
        self.planned = operations;

        Ok(())
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

/// The rows at `keys` that exist, read by key, batched per sort key.
async fn read_by_key(rows: &dyn Rows, keys: &[Key]) -> Result<HashMap<Key, ViewRow>, Error> {
    let mut by_sk: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (pk, sk) in keys {
        by_sk.entry(sk.as_str()).or_default().push(pk.as_str());
    }

    let mut stored = HashMap::new();
    for (sk, pks) in by_sk {
        for row in rows.read(pks, sk).await? {
            stored.insert((row.pk.clone(), row.sk.clone()), row);
        }
    }

    Ok(stored)
}

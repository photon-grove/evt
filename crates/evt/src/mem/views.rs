//! [`ViewRepository`] for the in-memory repository, over the same rows commit-wide view operations
//! write ([`MemRepository::view`](super::MemRepository::view)).

use std::ops::Bound;

use super::MemRepository;
use crate::error::Error;
use crate::projection::ViewRow;
use crate::views::{
    DEFAULT_VIEW_SK, ViewPage, ViewRepository, check_view_row, page_limit, resolve_key,
};
use crate::wire::dynamo::{AttributeValue, Item, decode_view_cursor, encode_view_cursor};

impl ViewRepository for MemRepository {
    async fn get_view_at(&self, pk: &str, sk: &str) -> Result<Option<ViewRow>, Error> {
        Ok(self.view(pk, resolve_key(pk, sk)?))
    }

    async fn put_view(&self, view: &ViewRow) -> Result<(), Error> {
        check_view_row(view)?;

        let mut row = view.clone();
        if row.sk.is_empty() {
            row.sk = DEFAULT_VIEW_SK.to_owned();
        }

        self.views_lock()
            .insert((row.pk.clone(), row.sk.clone()), row);

        Ok(())
    }

    async fn delete_view(&self, pk: &str, sk: &str) -> Result<(), Error> {
        let sk = resolve_key(pk, sk)?;

        self.views_lock().remove(&(pk.to_owned(), sk.to_owned()));

        Ok(())
    }

    async fn list_views_by_pk_paged(
        &self,
        pk: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<ViewPage, Error> {
        let after = cursor_key(cursor, |key| text(key, "pk") == Some(pk))?;
        let rows = self.views_lock();

        let start = match after {
            Some(sk) => Bound::Excluded((pk.to_owned(), sk)),
            None => Bound::Included((pk.to_owned(), String::new())),
        };

        let page = rows
            .range((start, Bound::Unbounded))
            .take_while(|((row_pk, _), _)| row_pk == pk)
            .map(|(_, row)| row);

        paged(page, limit, |row| {
            Item::from([
                ("pk".into(), AttributeValue::S(row.pk.clone())),
                ("sk".into(), AttributeValue::S(row.sk.clone())),
            ])
        })
    }

    async fn list_views_by_entity_type_paged(
        &self,
        entity_type: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<ViewPage, Error> {
        let after = cursor_key(cursor, |key| text(key, "entityType") == Some(entity_type))?;
        let rows = self.views_lock();

        // The rows are kept in (pk, sk) order, so a cursor resumes after its (pk, sk).
        let start = match after {
            Some((pk, sk)) => Bound::Excluded((pk, sk)),
            None => Bound::Unbounded,
        };

        let page = rows
            .range((start, Bound::Unbounded))
            .map(|(_, row)| row)
            .filter(|row| row.entity_type == entity_type);

        paged(page, limit, |row| {
            Item::from([
                ("pk".into(), AttributeValue::S(row.pk.clone())),
                ("sk".into(), AttributeValue::S(row.sk.clone())),
                (
                    "entityType".into(),
                    AttributeValue::S(row.entity_type.clone()),
                ),
            ])
        })
    }
}

fn text<'a>(key: &'a Item, name: &str) -> Option<&'a str> {
    match key.get(name) {
        Some(AttributeValue::S(value)) => Some(value),
        _ => None,
    }
}

/// Decodes a cursor and checks it belongs to the listing, returning what it resumes after. The
/// partition listing resumes after an `sk`, the entity-type listing after a `(pk, sk)`.
trait Resume: Sized {
    fn of(key: &Item) -> Option<Self>;
}

impl Resume for String {
    fn of(key: &Item) -> Option<Self> {
        text(key, "sk").map(str::to_owned)
    }
}

impl Resume for (String, String) {
    fn of(key: &Item) -> Option<Self> {
        Some((text(key, "pk")?.to_owned(), text(key, "sk")?.to_owned()))
    }
}

fn cursor_key<T: Resume>(
    cursor: Option<&str>,
    belongs: impl Fn(&Item) -> bool,
) -> Result<Option<T>, Error> {
    let Some(cursor) = cursor.filter(|cursor| !cursor.is_empty()) else {
        return Ok(None);
    };

    let key = decode_view_cursor(cursor)?;
    if !belongs(&key) {
        return Err(Error::InvalidArgument(
            "invalid pagination cursor: it belongs to another listing".into(),
        ));
    }

    Ok(T::of(&key))
}

/// Takes one page off an ordered iterator; the cursor is the last returned row's key, and only
/// when a row follows it.
fn paged<'a>(
    mut rows: impl Iterator<Item = &'a ViewRow>,
    limit: Option<usize>,
    key: impl Fn(&ViewRow) -> Item,
) -> Result<ViewPage, Error> {
    let Some(limit) = page_limit(limit) else {
        return Ok(ViewPage::new(rows.cloned().collect(), None));
    };

    let views: Vec<ViewRow> = rows.by_ref().take(limit).cloned().collect();
    let cursor = match (views.last(), rows.next()) {
        (Some(last), Some(_)) => Some(encode_view_cursor(&key(last))?),
        _ => None,
    };

    Ok(ViewPage::new(views, cursor))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pk: &str, sk: &str, entity_type: &str) -> ViewRow {
        ViewRow::new(pk, sk, "owner", entity_type, "{}")
    }

    #[tokio::test]
    async fn a_page_carries_a_cursor_only_when_a_row_follows() {
        let repo = MemRepository::new();
        for sk in ["a", "b", "c"] {
            repo.put_view(&row("p", sk, "t")).await.unwrap();
        }

        let first = repo
            .list_views_by_pk_paged("p", Some(2), None)
            .await
            .unwrap();
        assert_eq!(first.views.len(), 2);

        let second = repo
            .list_views_by_pk_paged("p", Some(2), first.next_cursor.as_deref())
            .await
            .unwrap();
        assert_eq!(second.views, vec![row("p", "c", "t")]);
        assert_eq!(second.next_cursor, None);

        let exact = repo
            .list_views_by_pk_paged("p", Some(3), None)
            .await
            .unwrap();
        assert_eq!(exact.views.len(), 3);
        assert_eq!(exact.next_cursor, None);
    }

    #[tokio::test]
    async fn a_cursor_is_go_s_json_of_the_last_key() {
        let repo = MemRepository::new();
        for pk in ["p1", "p2"] {
            repo.put_view(&row(pk, "VIEW", "t")).await.unwrap();
        }

        let by_pk = repo
            .list_views_by_pk_paged("p1", Some(1), None)
            .await
            .unwrap();
        assert_eq!(by_pk.next_cursor, None);

        let by_type = repo
            .list_views_by_entity_type_paged("t", Some(1), None)
            .await
            .unwrap();
        // {"entityType":"t","pk":"p1","sk":"VIEW"}, as DynamoDB's index key is.
        assert_eq!(
            by_type.next_cursor.as_deref(),
            Some("eyJlbnRpdHlUeXBlIjoidCIsInBrIjoicDEiLCJzayI6IlZJRVcifQ==")
        );
    }

    #[tokio::test]
    async fn a_cursor_from_another_listing_is_refused() {
        let repo = MemRepository::new();
        for sk in ["a", "b"] {
            repo.put_view(&row("p", sk, "t")).await.unwrap();
        }

        let cursor = repo
            .list_views_by_pk_paged("p", Some(1), None)
            .await
            .unwrap()
            .next_cursor
            .unwrap();

        assert!(matches!(
            repo.list_views_by_pk_paged("other", Some(1), Some(&cursor))
                .await,
            Err(Error::InvalidArgument(_))
        ));
        // A partition cursor carries no entity type.
        assert!(matches!(
            repo.list_views_by_entity_type_paged("t", Some(1), Some(&cursor))
                .await,
            Err(Error::InvalidArgument(_))
        ));
    }

    #[tokio::test]
    async fn clones_share_rows_and_reads_share_nothing() {
        let repo = MemRepository::new();
        let clone = repo.clone();

        repo.put_view(&row("p", "s", "t")).await.unwrap();

        let mut read = clone.get_view_at("p", "s").await.unwrap().unwrap();
        read.payload = b"changed".to_vec();

        assert_eq!(
            repo.get_view_at("p", "s").await.unwrap().unwrap().payload,
            b"{}"
        );
    }
}

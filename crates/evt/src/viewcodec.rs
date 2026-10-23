//! Typed JSON views over a [`ViewRepository`] (P2): the Rust counterpart of Go's `viewcodec.Codec`
//! and `viewcodec.Single`, merged with the `evt.GetJSONView` family.
//!
//! A [`ViewCodec`] binds a repository, an entity type, and a value type `T`. It writes `T` as the
//! row's `payload` with [`json::to_vec`], so the bytes are Go's `json.Marshal` of the same shape
//! (declare struct fields in the Go struct's order, and use `BTreeMap` for maps; see [`json`]), and
//! reads a row back by decoding the payload as `T`.
//!
//! Where Go's decoders differ from serde's, the Rust behavior is the serde one: a JSON `null` does
//! not decode into a struct (Go leaves it at its zero value), and field names match exactly (Go
//! matches case-insensitively). Use `#[serde(default)]` and [`json::null_default`] on fields
//! that older or Go-written rows may leave `null`.

use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::Error;
use crate::json;
use crate::projection::ViewRow;
use crate::views::{ViewPage, ViewRepository};

/// A view row whose payload does not decode into the codec's value type (Go's
/// `JSONViewDecodeError`). It names the row.
#[derive(Debug, thiserror::Error)]
#[error("decode JSON view pk={pk:?} sk={sk:?}: {source}")]
#[non_exhaustive]
pub struct ViewDecodeError {
    /// The row's partition key.
    pub pk: String,
    /// The row's sort key.
    pub sk: String,
    /// The decoder's error.
    #[source]
    pub source: serde_json::Error,
}

/// Why a typed view read or write failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ViewCodecError {
    /// The repository failed.
    #[error(transparent)]
    Repository(#[from] Error),

    /// A row's payload does not decode into the value type.
    #[error(transparent)]
    Decode(#[from] ViewDecodeError),

    /// The value does not encode as JSON.
    #[error("encode JSON view pk={pk:?} sk={sk:?}: {source}")]
    Encode {
        /// The partition key the value was to be written at.
        pk: String,
        /// The sort key the value was to be written at.
        sk: String,
        /// The encoder's error.
        #[source]
        source: serde_json::Error,
    },
}

/// Decodes a row's payload into `T` (Go `evt.DecodeJSONView`).
pub fn decode_json_view<T: DeserializeOwned>(view: &ViewRow) -> Result<T, ViewDecodeError> {
    json::from_slice(&view.payload).map_err(|source| ViewDecodeError {
        pk: view.pk.clone(),
        sk: view.sk.clone(),
        source,
    })
}

type EntityIdOf<T> = dyn Fn(&T) -> String + Send + Sync;

type Decoder<T> = dyn Fn(&[u8]) -> Result<T, serde_json::Error> + Send + Sync;

/// A [`ViewRepository`] bound to a JSON value type `T` and an entity type. The caller supplies
/// `pk`, `sk` and the owner's entity ID per call (Go `viewcodec.Codec`).
pub struct ViewCodec<T, R> {
    repo: R,
    entity_type: String,
    decoder: Option<Arc<Decoder<T>>>,
}

impl<T, R: Clone> Clone for ViewCodec<T, R> {
    fn clone(&self) -> Self {
        Self {
            repo: self.repo.clone(),
            entity_type: self.entity_type.clone(),
            decoder: self.decoder.clone(),
        }
    }
}

impl<T, R> std::fmt::Debug for ViewCodec<T, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ViewCodec")
            .field("entity_type", &self.entity_type)
            .field("decoder", &self.decoder.as_ref().map(|_| "custom"))
            .finish_non_exhaustive()
    }
}

impl<T, R> ViewCodec<T, R>
where
    T: Serialize + DeserializeOwned,
    R: ViewRepository,
{
    /// A codec writing `entity_type` rows. `repo` can be a repository, a reference to one, or an
    /// `Arc` of one.
    pub fn new(repo: R, entity_type: impl Into<String>) -> Self {
        Self {
            repo,
            entity_type: entity_type.into(),
            decoder: None,
        }
    }

    /// Decodes payloads with `decoder` instead of `serde`'s `Deserialize` for `T`: the counterpart
    /// of Go's `NewWithFactory`, for a value that needs constructor-injected dependencies before
    /// it is filled in.
    #[must_use]
    pub fn with_decoder(
        mut self,
        decoder: impl Fn(&[u8]) -> Result<T, serde_json::Error> + Send + Sync + 'static,
    ) -> Self {
        self.decoder = Some(Arc::new(decoder));
        self
    }

    /// The underlying repository, for raw access such as an existence probe that should not pay
    /// the decode.
    pub fn repo(&self) -> &R {
        &self.repo
    }

    /// The entity type this codec writes.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// Decodes a row with the codec's decoder.
    pub fn decode(&self, view: &ViewRow) -> Result<T, ViewDecodeError> {
        match &self.decoder {
            Some(decoder) => decoder(&view.payload).map_err(|source| ViewDecodeError {
                pk: view.pk.clone(),
                sk: view.sk.clone(),
                source,
            }),
            None => decode_json_view(view),
        }
    }

    /// Reads and decodes the single-item view at `pk` (`sk = "VIEW"`); `None` when there is none.
    pub async fn get(&self, pk: &str) -> Result<Option<T>, ViewCodecError> {
        let row = self.repo.get_view(pk).await?;

        Ok(row.map(|row| self.decode(&row)).transpose()?)
    }

    /// Reads and decodes the view at `pk` and `sk`.
    pub async fn get_at(&self, pk: &str, sk: &str) -> Result<Option<T>, ViewCodecError> {
        let row = self.repo.get_view_at(pk, sk).await?;

        Ok(row.map(|row| self.decode(&row)).transpose()?)
    }

    /// Writes `value` at `pk` and `sk`, owned by `entity_id`, under the codec's entity type.
    pub async fn put(
        &self,
        pk: &str,
        sk: &str,
        entity_id: &str,
        value: &T,
    ) -> Result<(), ViewCodecError> {
        self.put_row(
            &ViewRow::new(pk, sk, entity_id, self.entity_type.as_str(), Vec::new()),
            value,
        )
        .await
    }

    /// Writes `value` at `pk` with an empty sort key, which the repository stores as `"VIEW"`.
    pub async fn put_at(&self, pk: &str, entity_id: &str, value: &T) -> Result<(), ViewCodecError> {
        self.put(pk, "", entity_id, value).await
    }

    /// Encodes `value` as the payload of a copy of `template` and writes it (Go `PutJSONView`): the
    /// template's keys, owner, entity type and `ttl` are kept, its payload is replaced.
    pub async fn put_row(&self, template: &ViewRow, value: &T) -> Result<(), ViewCodecError> {
        let payload = json::to_vec(value).map_err(|source| ViewCodecError::Encode {
            pk: template.pk.clone(),
            sk: template.sk.clone(),
            source,
        })?;

        let mut row = template.clone();
        row.payload = payload;

        Ok(self.repo.put_view(&row).await?)
    }

    /// The valid views of a partition, ascending by `sk`. A row that does not decode is skipped
    /// and reported to `on_decode_error`; use [`list_by_pk_strict`](Self::list_by_pk_strict) to
    /// fail instead.
    pub async fn list_by_pk(
        &self,
        pk: &str,
        on_decode_error: impl FnMut(&ViewRow, ViewDecodeError),
    ) -> Result<Vec<T>, ViewCodecError> {
        let rows = self.repo.list_views_by_pk(pk).await?;

        Ok(self.decode_valid(&rows, on_decode_error))
    }

    /// The valid views of the codec's entity type. A row that does not decode is skipped and
    /// reported to `on_decode_error`.
    pub async fn list_all(
        &self,
        on_decode_error: impl FnMut(&ViewRow, ViewDecodeError),
    ) -> Result<Vec<T>, ViewCodecError> {
        let rows = self
            .repo
            .list_views_by_entity_type(&self.entity_type)
            .await?;

        Ok(self.decode_valid(&rows, on_decode_error))
    }

    /// Every view of a partition; the first row that does not decode fails the list.
    pub async fn list_by_pk_strict(&self, pk: &str) -> Result<Vec<T>, ViewCodecError> {
        let rows = self.repo.list_views_by_pk(pk).await?;

        Ok(self.decode_all(&rows)?)
    }

    /// Every view of the codec's entity type; the first row that does not decode fails the list.
    pub async fn list_all_strict(&self) -> Result<Vec<T>, ViewCodecError> {
        let rows = self
            .repo
            .list_views_by_entity_type(&self.entity_type)
            .await?;

        Ok(self.decode_all(&rows)?)
    }

    /// One page of a partition's views, strictly decoded, with the cursor of the next page.
    pub async fn list_by_pk_paged(
        &self,
        pk: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<(Vec<T>, Option<String>), ViewCodecError> {
        let ViewPage { views, next_cursor } =
            self.repo.list_views_by_pk_paged(pk, limit, cursor).await?;

        Ok((self.decode_all(&views)?, next_cursor))
    }

    fn decode_all(&self, rows: &[ViewRow]) -> Result<Vec<T>, ViewDecodeError> {
        rows.iter().map(|row| self.decode(row)).collect()
    }

    fn decode_valid(
        &self,
        rows: &[ViewRow],
        mut on_decode_error: impl FnMut(&ViewRow, ViewDecodeError),
    ) -> Vec<T> {
        let mut values = Vec::with_capacity(rows.len());

        for row in rows {
            match self.decode(row) {
                Ok(value) => values.push(value),
                Err(err) => on_decode_error(row, err),
            }
        }

        values
    }

    /// Binds a function from a key to its `pk`, for the common one-row-per-entity shape (Go
    /// `viewcodec.NewSingle`).
    pub fn single<K>(
        self,
        pk_for: impl Fn(&K) -> String + Send + Sync + 'static,
    ) -> SingleView<K, T, R> {
        SingleView {
            codec: self,
            pk_for: Box::new(pk_for),
            entity_id_of: None,
        }
    }
}

/// A [`ViewCodec`] with a `pk` function: concise `get(id)` and `put(id, value)` for a view with
/// one row per entity (Go `viewcodec.Single`).
pub struct SingleView<K, T, R> {
    codec: ViewCodec<T, R>,
    pk_for: Box<dyn Fn(&K) -> String + Send + Sync>,
    entity_id_of: Option<Box<EntityIdOf<T>>>,
}

impl<K, T, R> std::fmt::Debug for SingleView<K, T, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleView")
            .field("entity_type", &self.codec.entity_type)
            .finish_non_exhaustive()
    }
}

impl<K, T, R> SingleView<K, T, R>
where
    T: Serialize + DeserializeOwned,
    R: ViewRepository,
{
    /// Derives the owner's entity ID of a written value, which [`put`](Self::put) stores as the
    /// row's `entityID`. Without it the owner is empty.
    #[must_use]
    pub fn with_entity_id(
        mut self,
        entity_id_of: impl Fn(&T) -> String + Send + Sync + 'static,
    ) -> Self {
        self.entity_id_of = Some(Box::new(entity_id_of));
        self
    }

    /// The underlying codec, for lookups outside the bound `pk` function (such as a legacy-key
    /// fallback).
    pub fn codec(&self) -> &ViewCodec<T, R> {
        &self.codec
    }

    /// Reads the view keyed by `id`.
    pub async fn get(&self, id: &K) -> Result<Option<T>, ViewCodecError> {
        self.codec.get(&(self.pk_for)(id)).await
    }

    /// Writes `value` at the `pk` of `id`, owned by the entity ID the value derives.
    pub async fn put(&self, id: &K, value: &T) -> Result<(), ViewCodecError> {
        let entity_id = self
            .entity_id_of
            .as_ref()
            .map(|entity_id_of| entity_id_of(value))
            .unwrap_or_default();

        self.codec
            .put_at(&(self.pk_for)(id), &entity_id, value)
            .await
    }

    /// The valid views of the codec's entity type; see [`ViewCodec::list_all`].
    pub async fn list_all(
        &self,
        on_decode_error: impl FnMut(&ViewRow, ViewDecodeError),
    ) -> Result<Vec<T>, ViewCodecError> {
        self.codec.list_all(on_decode_error).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::Deserialize;

    use super::*;
    use crate::MemRepository;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Profile {
        name: String,
        bio: Option<String>,
        tags: Vec<String>,
        labels: BTreeMap<String, String>,
        count: i64,
    }

    fn profile(name: &str) -> Profile {
        Profile {
            name: name.into(),
            bio: None,
            tags: vec!["b".into(), "a".into()],
            labels: BTreeMap::from([("tier".into(), "gold".into())]),
            count: 3,
        }
    }

    #[tokio::test]
    async fn a_typed_value_round_trips_through_the_repository() {
        let repo = MemRepository::new();
        let codec = ViewCodec::<Profile, _>::new(&repo, "profile");

        assert_eq!(codec.get("profile#ada").await.unwrap(), None);

        codec
            .put_at("profile#ada", "ada", &profile("Ada"))
            .await
            .unwrap();
        assert_eq!(
            codec.get("profile#ada").await.unwrap(),
            Some(profile("Ada"))
        );

        let row = repo.view("profile#ada", "VIEW").unwrap();
        assert_eq!(row.entity_id, "ada");
        assert_eq!(row.entity_type, "profile");
        assert_eq!(
            row.payload,
            br#"{"name":"Ada","bio":null,"tags":["b","a"],"labels":{"tier":"gold"},"count":3}"#
        );
    }

    #[tokio::test]
    async fn the_payload_is_encoded_the_way_go_encodes_it() {
        let repo = MemRepository::new();
        let codec = ViewCodec::<Profile, _>::new(&repo, "profile");

        let mut value = profile("Ada <&> \u{2028}");
        value.bio = Some("<b>".into());
        codec.put("p", "s", "o", &value).await.unwrap();

        assert_eq!(
            repo.view("p", "s").unwrap().payload,
            br#"{"name":"Ada \u003c\u0026\u003e \u2028","bio":"\u003cb\u003e","tags":["b","a"],"labels":{"tier":"gold"},"count":3}"#
        );
    }

    #[tokio::test]
    async fn put_row_keeps_the_template_and_its_ttl() {
        let repo = MemRepository::new();
        let codec = ViewCodec::<Profile, _>::new(repo.clone(), "profile");

        let template = ViewRow::new("p", "s", "owner", "other-type", "ignored").with_ttl(99);
        codec.put_row(&template, &profile("Ada")).await.unwrap();

        let row = repo.view("p", "s").unwrap();
        assert_eq!((row.entity_type.as_str(), row.ttl), ("other-type", 99));
        assert_eq!(codec.get_at("p", "s").await.unwrap(), Some(profile("Ada")));
        // The template is not modified.
        assert_eq!(template.payload, b"ignored");
    }

    #[tokio::test]
    async fn invalid_payloads_fail_a_strict_read_and_are_skipped_by_a_lenient_list() {
        let repo = MemRepository::new();
        let codec = ViewCodec::<Profile, _>::new(&repo, "profile");

        codec.put("p", "a", "o", &profile("a")).await.unwrap();
        repo.put_view(&ViewRow::new("p", "b", "o", "profile", "not json"))
            .await
            .unwrap();
        repo.put_view(&ViewRow::new("p", "c", "o", "profile", r#"{"name":5}"#))
            .await
            .unwrap();
        codec.put("p", "d", "o", &profile("d")).await.unwrap();

        match codec.get_at("p", "b").await {
            Err(ViewCodecError::Decode(err)) => {
                assert_eq!((err.pk.as_str(), err.sk.as_str()), ("p", "b"));
                assert!(
                    err.to_string()
                        .starts_with(r#"decode JSON view pk="p" sk="b": "#)
                );
            }
            other => panic!("want a decode error, got {other:?}"),
        }

        let mut skipped = Vec::new();
        let listed = codec
            .list_by_pk("p", |row, err| skipped.push((row.sk.clone(), err.sk)))
            .await
            .unwrap();
        assert_eq!(listed, vec![profile("a"), profile("d")]);
        assert_eq!(
            skipped,
            vec![
                ("b".to_owned(), "b".to_owned()),
                ("c".to_owned(), "c".to_owned())
            ]
        );

        let all = codec.list_all(|_, _| {}).await.unwrap();
        assert_eq!(all, listed);

        assert!(matches!(
            codec.list_by_pk_strict("p").await,
            Err(ViewCodecError::Decode(_))
        ));
        assert!(matches!(
            codec.list_all_strict().await,
            Err(ViewCodecError::Decode(_))
        ));
    }

    #[tokio::test]
    async fn lists_are_bounded_to_the_entity_type() {
        let repo = MemRepository::new();
        let profiles = ViewCodec::<Profile, _>::new(&repo, "profile");
        let others = ViewCodec::<Profile, _>::new(&repo, "other");

        profiles
            .put("p1", "VIEW", "o", &profile("a"))
            .await
            .unwrap();
        others.put("p2", "VIEW", "o", &profile("b")).await.unwrap();

        assert_eq!(
            profiles.list_all_strict().await.unwrap(),
            vec![profile("a")]
        );

        let (page, cursor) = profiles
            .list_by_pk_paged("p1", Some(1), None)
            .await
            .unwrap();
        assert_eq!((page, cursor), (vec![profile("a")], None));
    }

    #[tokio::test]
    async fn a_decoder_builds_the_value_like_a_factory() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Entity {
            #[serde(skip)]
            injected: u32,
            name: String,
        }

        let repo = MemRepository::new();
        let codec = ViewCodec::<Entity, _>::new(&repo, "entity").with_decoder(|payload| {
            let mut entity: Entity = serde_json::from_slice(payload)?;
            entity.injected = 7;

            Ok(entity)
        });

        codec
            .put_at(
                "e",
                "o",
                &Entity {
                    injected: 1,
                    name: "x".into(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            codec.get("e").await.unwrap(),
            Some(Entity {
                injected: 7,
                name: "x".into()
            })
        );
    }

    #[tokio::test]
    async fn a_single_view_derives_the_pk_and_the_owner() {
        let repo = std::sync::Arc::new(MemRepository::new());
        let single = ViewCodec::<Profile, _>::new(repo.clone(), "profile")
            .single(|id: &String| format!("profile#{id}"))
            .with_entity_id(|value: &Profile| value.name.to_lowercase());

        assert_eq!(single.get(&"ada".to_owned()).await.unwrap(), None);

        single
            .put(&"ada".to_owned(), &profile("Ada"))
            .await
            .unwrap();
        assert_eq!(
            single.get(&"ada".to_owned()).await.unwrap(),
            Some(profile("Ada"))
        );

        let row = repo.view("profile#ada", "VIEW").unwrap();
        assert_eq!(row.entity_id, "ada");
        assert_eq!(
            single.list_all(|_, _| {}).await.unwrap(),
            vec![profile("Ada")]
        );
        assert_eq!(single.codec().entity_type(), "profile");
    }

    #[test]
    fn decode_json_view_reports_the_row() {
        let row = ViewRow::new("p", "s", "o", "t", "[1]");
        let err = decode_json_view::<Profile>(&row).unwrap_err();

        assert_eq!((err.pk.as_str(), err.sk.as_str()), ("p", "s"));
    }
}

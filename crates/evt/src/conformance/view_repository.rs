//! The view repository cases (P1, P10): point reads and writes, listing by partition and by entity
//! type, pagination cursors, batch writes and reads, and the rows the commit path wrote. Every
//! case namespaces its keys and entity type, so a durable table can be shared across runs.

use std::collections::BTreeSet;

use super::*;
use crate::projection::{ProjectedView, ViewOperation, ViewRow};
use crate::views::{ViewPage, ViewRepository};

/// Runs the [`ViewRepository`] cases against a repository whose table (or map) is empty of the
/// cases' own keys. Like [`outcome_cases`], it panics after running every case if any failed.
///
/// DynamoDB lists by entity type through a global secondary index, so a backend whose index lags
/// behind its table must be given time to catch up before the listing cases run; the local
/// emulator is immediate.
pub async fn view_repository_cases<V: ViewRepository>(repo: &V) {
    let mut failures = Vec::new();

    let mut record = |name: &str, result: CaseResult| match result {
        Ok(()) => println!("ok   {name}"),
        Err(Failure(message)) => {
            println!("FAIL {name}");
            failures.push(format!("{name}: {message}"));
        }
    };

    record(
        "a view is put, read, overwritten and deleted",
        Box::pin(view_round_trip(repo)).await,
    );
    record(
        "an empty sort key is stored as VIEW, and a ttl round-trips",
        Box::pin(view_default_sk_and_ttl(repo)).await,
    );
    record(
        "rows of a partition list ascending by sort key bytes, and only that partition's",
        Box::pin(view_list_by_pk(repo)).await,
    );
    record(
        "a partition pages with a cursor until it is exhausted",
        Box::pin(view_page_by_pk(repo)).await,
    );
    record(
        "an entity type lists and pages across partitions",
        Box::pin(view_by_entity_type(repo)).await,
    );
    record(
        "a malformed cursor is refused on every listing",
        Box::pin(view_bad_cursor(repo)).await,
    );
    record(
        "put_views writes every row, in chunks, and nothing from an empty slice",
        Box::pin(view_put_many(repo)).await,
    );
    record(
        "batch_get_views de-duplicates, skips empty and missing keys, and chunks",
        Box::pin(view_batch_get(repo)).await,
    );
    record(
        "a row with an empty pk or a non-UTF-8 payload is refused before any write",
        Box::pin(view_refusals(repo)).await,
    );

    if !failures.is_empty() {
        panic!(
            "{} view repository case(s) failed:\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }
}

/// Runs the cases that tie the commit path to the view repository: a row a commit wrote through
/// `writer` is read, listed and by entity type through `reader`, at the same table and
/// attributes, owner included. `reader` must read the table `writer` writes view operations to.
pub async fn committed_view_cases<R, V>(writer: &R, reader: &V)
where
    R: Repository,
    V: ViewRepository,
{
    let result = Box::pin(committed_rows(writer, reader)).await;

    match result {
        Ok(()) => println!("ok   rows written by a commit are read by the view repository"),
        Err(Failure(message)) => panic!("committed view row case failed: {message}"),
    }
}

fn row(pk: &str, sk: &str, entity_type: &str, payload: &str) -> ViewRow {
    ViewRow::new(pk, sk, format!("owner-{pk}"), entity_type, payload)
}

fn keys(rows: &[ViewRow]) -> Vec<(String, String)> {
    rows.iter()
        .map(|row| (row.pk.clone(), row.sk.clone()))
        .collect()
}

async fn view_round_trip<V: ViewRepository>(repo: &V) -> CaseResult {
    let ns = namespace();
    let pk = format!("{ns}-a");
    let first = row(&pk, "VIEW", &format!("{ns}-t"), r#"{"v":1}"#);

    ensure_eq!(repo.get_view(&pk).await?, None, "an absent view");

    repo.put_view(&first).await?;
    ensure_eq!(repo.get_view(&pk).await?, Some(first.clone()), "the row");
    ensure_eq!(
        repo.get_view_at(&pk, "VIEW").await?,
        Some(first.clone()),
        "the row at its sort key"
    );

    let second = ViewRow::new(
        pk.as_str(),
        "VIEW",
        "owner-2",
        format!("{ns}-u"),
        r#"{"v":2}"#,
    );
    repo.put_view(&second).await?;
    ensure_eq!(
        repo.get_view(&pk).await?,
        Some(second),
        "a put replaces the whole row"
    );

    repo.delete_view(&pk, "").await?;
    ensure_eq!(
        repo.get_view(&pk).await?,
        None,
        "an empty sort key deletes VIEW"
    );
    repo.delete_view(&pk, "VIEW").await?;
    repo.delete_view(&format!("{ns}-never"), "x").await?;

    Ok(())
}

async fn view_default_sk_and_ttl<V: ViewRepository>(repo: &V) -> CaseResult {
    let ns = namespace();
    let pk = format!("{ns}-a");

    let mut put = row(&pk, "", &format!("{ns}-t"), "{}");
    put.ttl = 4_102_444_800;
    repo.put_view(&put).await?;

    let mut want = put.clone();
    want.sk = "VIEW".into();
    ensure_eq!(repo.get_view(&pk).await?, Some(want), "stored at VIEW");

    // Replacing a row without a ttl removes the ttl.
    repo.put_view(&row(&pk, "VIEW", &format!("{ns}-t"), "{}"))
        .await?;
    let stored = repo.get_view(&pk).await?;
    ensure_eq!(stored.map(|row| row.ttl), Some(0), "the ttl is gone");

    Ok(())
}

async fn view_list_by_pk<V: ViewRepository>(repo: &V) -> CaseResult {
    let ns = namespace();
    let (pk, other) = (format!("{ns}-a"), format!("{ns}-b"));
    let entity_type = format!("{ns}-t");

    ensure_eq!(
        repo.list_views_by_pk(&pk).await?,
        Vec::<ViewRow>::new(),
        "an empty partition"
    );

    // UTF-8 byte order: "a" < "z" < "é" (C3 A9) < U+10000 (F0 90 80 80).
    let order = ["a", "z", "é", "\u{10000}"];
    for sk in ["é", "a", "\u{10000}", "z"] {
        repo.put_view(&row(&pk, sk, &entity_type, "{}")).await?;
    }
    repo.put_view(&row(&other, "m", &entity_type, "{}")).await?;

    let listed = repo.list_views_by_pk(&pk).await?;
    ensure_eq!(
        keys(&listed),
        order
            .iter()
            .map(|sk| (pk.clone(), (*sk).to_owned()))
            .collect::<Vec<_>>(),
        "ascending by sort key bytes"
    );

    Ok(())
}

async fn view_page_by_pk<V: ViewRepository>(repo: &V) -> CaseResult {
    let ns = namespace();
    let pk = format!("{ns}-a");

    let all: Vec<ViewRow> = (0..5)
        .map(|index| row(&pk, &format!("s{index}"), &format!("{ns}-t"), "{}"))
        .collect();
    repo.put_views(&all).await?;

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;

    loop {
        let ViewPage { views, next_cursor } = repo
            .list_views_by_pk_paged(&pk, Some(2), cursor.as_deref())
            .await?;

        ensure!(
            views.len() <= 2,
            "a page of {} rows exceeds the limit",
            views.len()
        );
        seen.extend(views);
        pages += 1;
        ensure!(pages <= 6, "the pages never end");

        match next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    ensure_eq!(seen, all, "the pages, in order, are the whole partition");
    ensure!(
        pages >= 3,
        "five rows at two per page take at least three pages"
    );

    let unbounded = repo.list_views_by_pk_paged(&pk, None, None).await?;
    ensure_eq!(unbounded.views, all, "an unbounded page");
    ensure_eq!(unbounded.next_cursor, None, "nothing follows");

    let zero = repo.list_views_by_pk_paged(&pk, Some(0), None).await?;
    ensure_eq!(zero.views, all, "a zero limit is no limit");

    Ok(())
}

async fn view_by_entity_type<V: ViewRepository>(repo: &V) -> CaseResult {
    let ns = namespace();
    let entity_type = format!("{ns}-t");

    let mut all = Vec::new();
    for partition in 0..2 {
        for index in 0..3 {
            all.push(row(
                &format!("{ns}-p{partition}"),
                &format!("s{index}"),
                &entity_type,
                "{}",
            ));
        }
    }
    all.push(row(&format!("{ns}-other"), "s", &format!("{ns}-u"), "{}"));
    repo.put_views(&all).await?;

    let want: BTreeSet<_> = keys(&all[..6]).into_iter().collect();

    ensure_eq!(
        repo.list_views_by_entity_type(&entity_type)
            .await?
            .into_iter()
            .map(|row| (row.pk, row.sk))
            .collect::<BTreeSet<_>>(),
        want,
        "every row of the type, and no other"
    );
    ensure_eq!(
        repo.list_views_by_entity_type(&format!("{ns}-none"))
            .await?,
        Vec::<ViewRow>::new(),
        "an entity type with no rows"
    );

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;

    loop {
        let page = repo
            .list_views_by_entity_type_paged(&entity_type, Some(2), cursor.as_deref())
            .await?;

        ensure!(page.views.len() <= 2, "a page exceeds the limit");
        seen.extend(page.views.into_iter().map(|row| (row.pk, row.sk)));
        pages += 1;
        ensure!(pages <= 8, "the pages never end");

        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    ensure_eq!(seen.len(), 6, "no row is repeated or skipped across pages");
    ensure_eq!(
        seen.into_iter().collect::<BTreeSet<_>>(),
        want,
        "the pages cover the type"
    );

    Ok(())
}

async fn view_bad_cursor<V: ViewRepository>(repo: &V) -> CaseResult {
    let ns = namespace();

    for cursor in ["not base64!", "bm90IGpzb24=", "eyJwayI6MX0=", "e30="] {
        expect_err(
            repo.list_views_by_pk_paged(&ns, Some(1), Some(cursor))
                .await,
            &format!("pk listing with cursor {cursor:?}"),
            |err| matches!(err, Error::InvalidArgument(_)),
        )?;
        expect_err(
            repo.list_views_by_entity_type_paged(&ns, Some(1), Some(cursor))
                .await,
            &format!("entity type listing with cursor {cursor:?}"),
            |err| matches!(err, Error::InvalidArgument(_)),
        )?;
    }

    // An empty cursor is the first page.
    repo.list_views_by_pk_paged(&ns, Some(1), Some("")).await?;

    // A cursor continues the listing that made it.
    let (pk, other, entity_type) = (format!("{ns}-a"), format!("{ns}-b"), format!("{ns}-t"));
    repo.put_views(&[
        row(&pk, "1", &entity_type, "{}"),
        row(&pk, "2", &entity_type, "{}"),
    ])
    .await?;

    let by_pk = repo.list_views_by_pk_paged(&pk, Some(1), None).await?;
    let by_pk = by_pk
        .next_cursor
        .ok_or_else(|| Failure("a partition cursor".into()))?;
    let by_type = repo
        .list_views_by_entity_type_paged(&entity_type, Some(1), None)
        .await?;
    let by_type = by_type
        .next_cursor
        .ok_or_else(|| Failure("an entity type cursor".into()))?;

    let refused = |err: &Error| matches!(err, Error::InvalidArgument(_));
    expect_err(
        repo.list_views_by_pk_paged(&other, Some(1), Some(&by_pk))
            .await,
        "a partition cursor on another partition",
        refused,
    )?;
    expect_err(
        repo.list_views_by_pk_paged(&other, Some(1), Some(&by_type))
            .await,
        "an entity type cursor on another partition",
        refused,
    )?;
    expect_err(
        repo.list_views_by_entity_type_paged(&entity_type, Some(1), Some(&by_pk))
            .await,
        "a partition cursor on an entity type",
        refused,
    )?;
    expect_err(
        repo.list_views_by_entity_type_paged(&format!("{ns}-u"), Some(1), Some(&by_type))
            .await,
        "an entity type cursor on another entity type",
        refused,
    )?;

    Ok(())
}

async fn view_put_many<V: ViewRepository>(repo: &V) -> CaseResult {
    let ns = namespace();
    let pk = format!("{ns}-a");

    repo.put_views(&[]).await?;

    // 60 rows are three BatchWriteItem calls (25, 25, 10) on DynamoDB.
    let all: Vec<ViewRow> = (0..60)
        .map(|index| row(&pk, &format!("s{index:02}"), &format!("{ns}-t"), "{}"))
        .collect();
    repo.put_views(&all).await?;

    ensure_eq!(
        repo.list_views_by_pk(&pk).await?,
        all,
        "every row is stored"
    );

    // A key repeated within one batch keeps its last row.
    let repeated = format!("{ns}-repeat");
    repo.put_views(&[
        row(&repeated, "VIEW", &format!("{ns}-t"), r#"{"n":1}"#),
        row(&repeated, "", &format!("{ns}-t"), r#"{"n":2}"#),
        row(&repeated, "VIEW", &format!("{ns}-t"), r#"{"n":3}"#),
    ])
    .await?;
    let stored = repo.get_view(&repeated).await?;
    ensure_eq!(
        stored.map(|row| row.payload),
        Some(br#"{"n":3}"#.to_vec()),
        "the last of a repeated key"
    );

    // The batch is a plain put: a later one replaces rows.
    let replaced: Vec<ViewRow> = all
        .iter()
        .take(3)
        .map(|row| ViewRow {
            payload: br#"{"again":true}"#.to_vec(),
            ..row.clone()
        })
        .collect();
    repo.put_views(&replaced).await?;
    let listed = repo.list_views_by_pk(&pk).await?;
    ensure_eq!(listed[..3].to_vec(), replaced, "replaced rows");
    ensure_eq!(listed.len(), 60, "no row was added");

    Ok(())
}

async fn view_batch_get<V: ViewRepository>(repo: &V) -> CaseResult {
    let ns = namespace();
    let entity_type = format!("{ns}-t");

    // 130 distinct partitions take two BatchGetItem calls (100, 30).
    let all: Vec<ViewRow> = (0..130)
        .map(|index| row(&format!("{ns}-p{index:03}"), "VIEW", &entity_type, "{}"))
        .collect();
    repo.put_views(&all).await?;

    let mut pks: Vec<&str> = all.iter().map(|row| row.pk.as_str()).collect();
    pks.push(all[0].pk.as_str());
    pks.push("");
    pks.push("missing-partition");

    let mut got = repo.batch_get_views(&pks).await?;
    got.sort_by(|a, b| a.pk.cmp(&b.pk));
    ensure_eq!(got, all, "each existing row once");

    ensure_eq!(
        repo.batch_get_views(&[]).await?,
        Vec::<ViewRow>::new(),
        "no keys"
    );

    let custom = row(&all[0].pk, "other", &entity_type, "{}");
    repo.put_view(&custom).await?;
    ensure_eq!(
        repo.batch_get_views_at(&[all[0].pk.as_str(), all[1].pk.as_str()], "other")
            .await?,
        vec![custom],
        "a custom sort key"
    );

    Ok(())
}

async fn view_refusals<V: ViewRepository>(repo: &V) -> CaseResult {
    let ns = namespace();
    let pk = format!("{ns}-a");

    expect_err(
        repo.put_view(&row("", "s", &ns, "{}")).await,
        "an empty pk",
        |err| matches!(err, Error::InvalidArgument(_)),
    )?;

    let bad = ViewRow::new(pk.as_str(), "s", "o", ns.as_str(), vec![0xff, 0xfe]);
    expect_err(repo.put_view(&bad).await, "a non-UTF-8 payload", |err| {
        matches!(err, Error::InvalidRecord(_))
    })?;

    expect_err(
        repo.get_view_at("", "s").await,
        "a read with an empty pk",
        |err| matches!(err, Error::InvalidArgument(_)),
    )?;
    expect_err(
        repo.delete_view("", "s").await,
        "a delete with an empty pk",
        |err| matches!(err, Error::InvalidArgument(_)),
    )?;

    // An empty sort key reads the single-item row, as it writes it.
    repo.put_view(&row(&pk, "", &ns, "{}")).await?;
    ensure!(
        repo.get_view_at(&pk, "").await?.is_some(),
        "an empty sort key reads VIEW"
    );
    ensure_eq!(
        repo.batch_get_views_at(&[&pk], "").await?.len(),
        1,
        "a batch read with an empty sort key reads VIEW"
    );
    repo.delete_view(&pk, "VIEW").await?;

    // A bad row anywhere in a batch refuses the whole batch before any write.
    let good = row(&pk, "good", &ns, "{}");
    expect_err(
        repo.put_views(&[good, bad]).await,
        "a batch with a non-UTF-8 payload",
        |err| matches!(err, Error::InvalidRecord(_)),
    )?;
    ensure_eq!(
        repo.list_views_by_pk(&pk).await?,
        Vec::<ViewRow>::new(),
        "nothing was written"
    );

    Ok(())
}

async fn committed_rows<R: Repository, V: ViewRepository>(writer: &R, reader: &V) -> CaseResult {
    let ns = namespace();
    let stream = Stream::new(&ns, "owner", "owner");
    let (pk, entity_type) = (format!("{ns}-committed"), format!("{ns}-kind"));
    let payload = r#"{"claimed":true}"#;

    let put = |sk: &str| {
        ProjectedView::new(
            "p",
            0,
            0,
            ViewOperation::put(
                pk.as_str(),
                sk,
                stream.id.clone(),
                entity_type.as_str(),
                payload,
            ),
        )
    };

    writer
        .commit(write(&stream, 1..=1).with_views([put("one"), {
            let mut view = put("two");
            view.operation = 1;
            view
        }]))
        .await?;

    let want = ViewRow::new(
        pk.as_str(),
        "one",
        stream.id.to_string(),
        entity_type.as_str(),
        payload,
    );
    ensure_eq!(
        reader.get_view_at(&pk, "one").await?,
        Some(want.clone()),
        "the committed row, owner as entityID"
    );
    ensure_eq!(
        keys(&reader.list_views_by_pk(&pk).await?),
        vec![
            (pk.clone(), "one".to_owned()),
            (pk.clone(), "two".to_owned())
        ],
        "the committed partition"
    );
    ensure_eq!(
        reader.list_views_by_entity_type(&entity_type).await?.len(),
        2,
        "the committed rows by entity type"
    );

    // A delete on the commit path is seen by the reader.
    let delete = ProjectedView::new(
        "p",
        0,
        0,
        ViewOperation::delete(pk.as_str(), "one", stream.id.clone()),
    );
    writer
        .commit(write(&stream, 2..=2).with_views([delete]))
        .await?;
    ensure_eq!(
        reader.get_view_at(&pk, "one").await?,
        None,
        "the committed delete"
    );

    Ok(())
}

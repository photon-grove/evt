//! Journal wake-ups (feature `journal`): a views-table stream record that says a journal scope has
//! new rows.
//!
//! The journal's rows (`journal#<scope>` / 20-digit position) and counters
//! (`journal-counter#<scope>` / `COUNTER`) are view rows in the views table, written in the commit
//! (see the storage format spec, "Journal"). A consumer turns on the table's stream and filters it
//! to those keys with [`JournalFilter`]; the records it then receives are **wake-ups**, not
//! deliveries. Stream records are ordered per item, not per scope, so a consumer reads the scope
//! with [`photon_evt::journal::page`] (or [`page_at_epoch`](photon_evt::journal::page_at_epoch))
//! from its own cursor: the page is ordered and reports gaps, and a redelivered or coalesced
//! wake-up only causes a read that finds nothing new.
//!
//! Wake-ups decode from the record's keys alone, so they work with every stream view type,
//! `KEYS_ONLY` included. They also name no table: the same helpers work unchanged on a stream of a
//! dedicated journal table, should one hold the rows.

use photon_evt::BoxError;
use photon_evt::journal::{
    COUNTER_PARTITION_PREFIX, COUNTER_SORT_KEY, ROW_PARTITION_PREFIX, Scope, parse_row_sk,
    read_counter,
};
use photon_evt::wire::dynamo::{AttributeValue, view_row_from_item};

use crate::batch::BatchOutcome;
use crate::error::FilterError;
use crate::filter::{StringRule, encode_pattern};
use crate::record::{EventName, StreamEvent, StreamRecord};

/// A scope with new rows, from one stream record (or, coalesced, several).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct JournalWakeup {
    /// The scope.
    pub scope: Scope,
    /// The highest position the record shows: the row's position, or the counter's value when the
    /// record has a new image. `None` for a counter record without one (`KEYS_ONLY`), or whose
    /// image does not decode. The scope may hold more rows than this by the time the consumer reads
    /// it.
    pub position: Option<u64>,
    /// The sequence number of the (first) record that woke the scope.
    pub sequence_number: String,
}

/// Decodes the journal wake-up a record carries, or `None` if it carries none.
///
/// A wake-up is an `INSERT` or a `MODIFY` of a journal row (`journal#<scope>` at a 20-digit
/// position) or a journal counter (`journal-counter#<scope>` at `COUNTER`). Everything else is
/// `None`: other view rows, removals (a compaction deletes rows; that is not news), and keys in
/// those partitions that the journal never writes (a scope that is not valid, any other sort key).
/// Decoding never fails, so no record of the views table can stop a shard: a corrupt journal is
/// reported where it is read, by the page the consumer reads (a gap, or an invalid row). A counter
/// whose image does not decode still wakes its scope, with no position.
pub fn decode_wakeup(record: &StreamRecord) -> Option<JournalWakeup> {
    if !matches!(record.event_name, EventName::Insert | EventName::Modify) {
        return None;
    }

    let pk = record.pk()?;
    let sk = match record.key("sk") {
        Some(AttributeValue::S(sk)) => sk.as_str(),
        _ => return None,
    };

    let (scope, position) = if let Some(name) = pk.strip_prefix(ROW_PARTITION_PREFIX) {
        (Scope::new(name).ok()?, Some(parse_row_sk(sk)?))
    } else if let Some(name) = pk.strip_prefix(COUNTER_PARTITION_PREFIX)
        && sk == COUNTER_SORT_KEY
    {
        let scope = Scope::new(name).ok()?;
        let position = record.change.new_image.as_ref().and_then(|image| {
            view_row_from_item(image)
                .and_then(|row| read_counter(&scope, &row))
                .ok()
        });

        (scope, position)
    } else {
        return None;
    };

    Some(JournalWakeup {
        scope,
        position,
        sequence_number: record.sequence_number().to_owned(),
    })
}

/// Merges wake-ups by scope: one per scope, in the order the scopes first appear, with the
/// highest position seen and the first record's sequence number.
pub fn coalesce_wakeups(wakeups: impl IntoIterator<Item = JournalWakeup>) -> Vec<JournalWakeup> {
    coalesce_indexed(wakeups.into_iter().map(|wakeup| (0, wakeup)))
        .into_iter()
        .map(|(_, wakeup)| wakeup)
        .collect()
}

/// [`coalesce_wakeups`], keeping the batch index of each scope's first record.
fn coalesce_indexed(
    wakeups: impl IntoIterator<Item = (usize, JournalWakeup)>,
) -> Vec<(usize, JournalWakeup)> {
    let mut merged: Vec<(usize, JournalWakeup)> = Vec::new();
    let mut at_scope: std::collections::HashMap<Scope, usize> = std::collections::HashMap::new();

    for (index, wakeup) in wakeups {
        match at_scope.get(&wakeup.scope) {
            Some(&at) => {
                let entry: &mut JournalWakeup = &mut merged[at].1;
                entry.position = entry.position.max(wakeup.position);
            }
            None => {
                at_scope.insert(wakeup.scope.clone(), merged.len());
                merged.push((index, wakeup));
            }
        }
    }

    merged
}

/// Wakes `handler` once per scope the batch's matching records name, coalesced
/// ([`coalesce_wakeups`]), in the order the scopes first appear, stopping at the first failure.
///
/// The handler reads the scope from its own cursor (typically
/// [`page_at_epoch`](photon_evt::journal::page_at_epoch) until it is caught up) and stores the new
/// cursor. Records `filter` does not match, and records that carry no wake-up
/// ([`decode_wakeup`]), are acknowledged without one.
///
/// A handler error for a scope reports that scope's first record: the scopes woken before it are
/// done, and the failed scope and every later one wake again on the retry. A read that finds
/// nothing new is the only cost of a repeated wake-up. A wake-up whose retries run out is lost
/// (see the crate README, "Journal wake-ups"): the scope is read again on its next write.
pub async fn process_wakeups<F, E>(
    event: &StreamEvent,
    filter: &JournalFilter,
    mut handler: F,
) -> BatchOutcome
where
    F: AsyncFnMut(JournalWakeup) -> Result<(), E>,
    E: Into<BoxError>,
{
    let mut outcome = BatchOutcome::default();
    let decoded = event
        .records
        .iter()
        .enumerate()
        .filter(|(_, record)| filter.matches(record))
        .filter_map(|(index, record)| Some((index, decode_wakeup(record)?)));

    for (index, wakeup) in coalesce_indexed(decoded) {
        let sequence_number = event.records[index].sequence_number().to_owned();

        if let Err(err) = handler(wakeup).await {
            outcome.failed(index, &sequence_number, err.into());

            return outcome;
        }

        outcome.handled += 1;
    }

    outcome.completed = event.records.len();

    outcome
}

/// Selects journal wake-ups in a views-table stream: the records of journal rows, or of journal
/// counters, optionally of some scopes only.
///
/// A record matches when it is an `INSERT` or a `MODIFY` whose key `pk` is in the selected
/// partitions. The pattern reads only `Keys`, so a `KEYS_ONLY` stream is enough.
///
/// - [`JournalFilter::counters`] wakes once per scope per commit (the commit puts each touched
///   scope's counter once), which is the fewest invocations. Records carry the counter's value
///   when the stream writes new images.
/// - [`JournalFilter::rows`] wakes once per row, and each record names the row's position.
///
/// ```
/// use photon_evt::journal::Scope;
/// use photon_evt_lambda::JournalFilter;
///
/// assert_eq!(
///     JournalFilter::counters().pattern().unwrap(),
///     r#"{"eventName":["INSERT","MODIFY"],"dynamodb":{"Keys":{"pk":{"S":[{"prefix":"journal-counter#"}]}}}}"#,
/// );
///
/// let filter = JournalFilter::rows()
///     .scope(&Scope::new("world:7").unwrap())
///     .scope_prefix("world:7/");
/// assert_eq!(
///     filter.pattern().unwrap(),
///     r#"{"eventName":["INSERT","MODIFY"],"dynamodb":{"Keys":{"pk":{"S":["journal#world:7",{"prefix":"journal#world:7/"}]}}}}"#,
/// );
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalFilter {
    partition_prefix: &'static str,
    pk: StringRule,
}

impl JournalFilter {
    /// Wake-ups from journal rows (`journal#<scope>`).
    pub fn rows() -> Self {
        Self::of(ROW_PARTITION_PREFIX)
    }

    /// Wake-ups from journal counters (`journal-counter#<scope>`).
    pub fn counters() -> Self {
        Self::of(COUNTER_PARTITION_PREFIX)
    }

    fn of(partition_prefix: &'static str) -> Self {
        Self {
            partition_prefix,
            pk: StringRule::default(),
        }
    }

    /// Also matches this scope.
    pub fn scope(mut self, scope: &Scope) -> Self {
        self.pk
            .values
            .insert(format!("{}{scope}", self.partition_prefix));
        self
    }

    /// Also matches every scope that starts with `prefix`, such as `world:7/`.
    pub fn scope_prefix(mut self, prefix: impl AsRef<str>) -> Self {
        self.pk
            .prefixes
            .insert(format!("{}{}", self.partition_prefix, prefix.as_ref()));
        self
    }

    fn rule(&self) -> StringRule {
        if self.pk.is_any() {
            let mut every = StringRule::default();
            every.prefixes.insert(self.partition_prefix.to_owned());

            return every;
        }

        self.pk.clone()
    }

    /// The event source mapping filter pattern (`Filters[].Pattern`). Deterministic text.
    pub fn pattern(&self) -> Result<String, FilterError> {
        encode_pattern(
            &["INSERT", "MODIFY"],
            vec![("pk", &self.rule())],
            Vec::new(),
        )
    }

    /// Whether the record matches, by the rule the pattern states.
    pub fn matches(&self, record: &StreamRecord) -> bool {
        matches!(record.event_name, EventName::Insert | EventName::Modify)
            && self.rule().matches_attribute(&record.change.keys, "pk")
    }
}

#[cfg(test)]
mod tests {
    use photon_evt::journal::{counter_view, row_sk, row_view};
    use photon_evt::wire::dynamo::{Item, view_row_item};

    use super::*;
    use crate::filter::tests::{event, lambda_matches};

    fn scope(name: &str) -> Scope {
        Scope::new(name).unwrap()
    }

    fn keys(pk: &str, sk: &str) -> Item {
        Item::from([
            ("pk".into(), AttributeValue::S(pk.into())),
            ("sk".into(), AttributeValue::S(sk.into())),
        ])
    }

    fn keys_only(name: EventName, pk: &str, sk: &str, sequence: &str) -> StreamRecord {
        StreamRecord::new(name, keys(pk, sk), sequence)
    }

    #[test]
    fn rows_and_counters_wake_their_scope() {
        let row = keys_only(EventName::Insert, "journal#world:7", &row_sk(4), "1");
        assert_eq!(
            decode_wakeup(&row),
            Some(JournalWakeup {
                scope: scope("world:7"),
                position: Some(4),
                sequence_number: "1".into(),
            })
        );

        let counter = keys_only(EventName::Modify, "journal-counter#world:7", "COUNTER", "2");
        assert_eq!(decode_wakeup(&counter).unwrap().position, None);

        let image = view_row_item(&counter_view(&scope("world:7"), 12)).unwrap();
        let counter = counter.with_new_image(image);
        assert_eq!(decode_wakeup(&counter).unwrap().position, Some(12));

        let full_row = row.clone().with_new_image(
            view_row_item(
                &row_view(&scope("world:7"), 4, &event("ledger", "ledger:added")).unwrap(),
            )
            .unwrap(),
        );
        assert_eq!(decode_wakeup(&full_row).unwrap().position, Some(4));
    }

    #[test]
    fn other_rows_and_removals_are_not_wakeups() {
        for record in [
            keys_only(EventName::Remove, "journal#world:7", &row_sk(4), "1"),
            keys_only(EventName::Insert, "account#acct-1", "VIEW", "1"),
            keys_only(EventName::Insert, "journalist#x", "VIEW", "1"),
            StreamRecord::new(EventName::Insert, Item::new(), "1"),
        ] {
            assert_eq!(decode_wakeup(&record), None, "{record:?}");
        }
    }

    #[test]
    fn keys_the_journal_never_writes_are_not_wakeups() {
        for record in [
            keys_only(EventName::Insert, "journal#world:7", "4", "1"),
            keys_only(EventName::Insert, "journal#world:7", "VIEW", "1"),
            keys_only(EventName::Insert, "journal#", &row_sk(1), "1"),
            keys_only(EventName::Insert, "journal-counter#world:7", "VIEW", "1"),
            keys_only(EventName::Insert, "journal-counter#", "COUNTER", "1"),
        ] {
            assert_eq!(decode_wakeup(&record), None, "{record:?}");
        }

        // A counter image that does not decode still wakes the scope; the page read reports it.
        let foreign = keys_only(EventName::Modify, "journal-counter#world:7", "COUNTER", "1")
            .with_new_image(view_row_item(&counter_view(&scope("world:8"), 1)).unwrap());
        let wakeup = decode_wakeup(&foreign).unwrap();
        assert_eq!(wakeup.scope, scope("world:7"));
        assert_eq!(wakeup.position, None);
    }

    #[test]
    fn wakeups_coalesce_by_scope_in_first_appearance_order() {
        let wake = |name: &str, position: Option<u64>, sequence: &str| JournalWakeup {
            scope: scope(name),
            position,
            sequence_number: sequence.into(),
        };

        let merged = coalesce_wakeups([
            wake("b", Some(3), "1"),
            wake("a", None, "2"),
            wake("b", Some(5), "3"),
            wake("b", Some(4), "4"),
            wake("a", Some(1), "5"),
        ]);

        assert_eq!(
            merged,
            vec![wake("b", Some(5), "1"), wake("a", Some(1), "2")]
        );
    }

    #[tokio::test]
    async fn a_batch_wakes_each_scope_once_and_reports_the_first_record_not_done() {
        let event = StreamEvent {
            records: vec![
                keys_only(EventName::Insert, "journal#a", &row_sk(1), "1"),
                keys_only(EventName::Insert, "account#acct-1", "VIEW", "2"),
                keys_only(EventName::Insert, "journal#b", &row_sk(1), "3"),
                keys_only(EventName::Insert, "journal#a", &row_sk(2), "4"),
                keys_only(EventName::Insert, "journal#c", &row_sk(9), "5"),
            ],
        };

        let mut woken = Vec::new();
        let outcome = process_wakeups(
            &event,
            &JournalFilter::rows(),
            async |wakeup: JournalWakeup| {
                woken.push((wakeup.scope.to_string(), wakeup.position));
                Ok::<_, BoxError>(())
            },
        )
        .await;

        assert!(outcome.is_success());
        assert_eq!(outcome.completed, 5);
        assert_eq!(
            woken,
            [
                ("a".to_owned(), Some(2)),
                ("b".to_owned(), Some(1)),
                ("c".to_owned(), Some(9))
            ]
        );

        let outcome = process_wakeups(
            &event,
            &JournalFilter::rows(),
            async |wakeup: JournalWakeup| {
                if wakeup.scope.as_str() == "b" {
                    return Err("rebuild in progress");
                }

                Ok(())
            },
        )
        .await;
        assert_eq!(outcome.handled, 1);
        assert_eq!(outcome.completed, 2);
        assert_eq!(outcome.response(), crate::BatchResponse::failed_from("3"));

        // A stray key in the journal's partitions is acknowledged, never a poison record.
        let mut stray = event.clone();
        stray.records[3] = keys_only(EventName::Insert, "journal#a", "VIEW", "4");
        let mut woken = Vec::new();
        let outcome = process_wakeups(
            &stray,
            &JournalFilter::rows(),
            async |wakeup: JournalWakeup| {
                woken.push((wakeup.scope.to_string(), wakeup.position));
                Ok::<_, BoxError>(())
            },
        )
        .await;
        assert_eq!(
            woken,
            [
                ("a".to_owned(), Some(1)),
                ("b".to_owned(), Some(1)),
                ("c".to_owned(), Some(9))
            ]
        );
        assert!(outcome.is_success());
    }

    #[tokio::test]
    async fn counter_wakeups_carry_the_counter_and_a_failed_scope_wakes_again_from_its_first_record()
     {
        let counter = |name: &str, position: u64, sequence: &str| {
            let image = view_row_item(&counter_view(&scope(name), position)).unwrap();
            let keys = Item::from([
                ("pk".into(), image["pk"].clone()),
                ("sk".into(), image["sk"].clone()),
            ]);

            StreamRecord::new(EventName::Modify, keys, sequence).with_new_image(image)
        };
        let event = StreamEvent {
            records: vec![
                counter("a", 3, "1"),
                counter("b", 7, "2"),
                counter("a", 4, "3"),
                counter("c", 1, "4"),
                counter("b", 8, "5"),
            ],
        };

        let mut woken = Vec::new();
        let outcome = process_wakeups(
            &event,
            &JournalFilter::counters(),
            async |wakeup: JournalWakeup| {
                woken.push((wakeup.scope.to_string(), wakeup.position));

                if wakeup.scope.as_str() == "b" {
                    return Err("rebuild in progress");
                }

                Ok(())
            },
        )
        .await;

        assert_eq!(
            woken,
            [("a".to_owned(), Some(4)), ("b".to_owned(), Some(8))],
            "c is not attempted"
        );
        assert_eq!(outcome.response(), crate::BatchResponse::failed_from("2"));
        assert_eq!(outcome.completed, 1);

        // The rows filter ignores counters entirely.
        let rows = process_wakeups(&event, &JournalFilter::rows(), async |_: JournalWakeup| {
            Err::<(), BoxError>("never called".into())
        })
        .await;
        assert!(rows.is_success());
        assert_eq!(rows.handled, 0);
    }

    #[test]
    fn the_handler_test_agrees_with_the_pattern() {
        let filters = [
            JournalFilter::rows(),
            JournalFilter::counters(),
            JournalFilter::rows().scope(&scope("world:7")),
            JournalFilter::counters()
                .scope_prefix("world:7/")
                .scope(&scope("world:8")),
        ];
        let records = [
            keys_only(EventName::Insert, "journal#world:7", &row_sk(1), "1"),
            keys_only(
                EventName::Modify,
                "journal#world:7/place:3",
                &row_sk(1),
                "1",
            ),
            keys_only(EventName::Remove, "journal#world:7", &row_sk(1), "1"),
            keys_only(EventName::Modify, "journal-counter#world:7", "COUNTER", "1"),
            keys_only(
                EventName::Insert,
                "journal-counter#world:7/place:3",
                "COUNTER",
                "1",
            ),
            keys_only(EventName::Insert, "journal-counter#world:8", "COUNTER", "1"),
            keys_only(EventName::Insert, "account#acct-1", "VIEW", "1"),
        ];

        for filter in &filters {
            let pattern = filter.pattern().unwrap();

            for record in &records {
                assert_eq!(
                    filter.matches(record),
                    lambda_matches(&pattern, record),
                    "{pattern} on {record:?}"
                );
            }
        }

        let hits = |filter: &JournalFilter| {
            records
                .iter()
                .filter(|record| filter.matches(record))
                .count()
        };
        assert_eq!(hits(&filters[0]), 2);
        assert_eq!(hits(&filters[1]), 3);
        assert_eq!(hits(&filters[2]), 1);
        assert_eq!(hits(&filters[3]), 2);
    }
}

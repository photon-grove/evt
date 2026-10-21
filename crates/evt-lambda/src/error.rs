//! Errors.

/// A record that is meant for this consumer and cannot be read.
///
/// A decode error is never skipped: the batch helpers report the record as failed, so the event
/// source mapping retries it and, once its retries run out, sends it to the on-failure destination.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// An event item was inserted, but the record has no new image: the table's stream view type
    /// is `KEYS_ONLY` or `OLD_IMAGE`. Event decode needs `NEW_IMAGE` or `NEW_AND_OLD_IMAGES`.
    #[error(
        "stream record {sequence_number} inserts an event but has no NewImage; the stream view type \
         must be NEW_IMAGE or NEW_AND_OLD_IMAGES (it is {view_type})"
    )]
    MissingNewImage {
        /// The record's sequence number.
        sequence_number: String,
        /// The view type the record reports, or `unset`.
        view_type: String,
    },

    /// The event item does not decode.
    #[error("stream record {sequence_number}: {source}")]
    Record {
        /// The record's sequence number.
        sequence_number: String,
        /// Why.
        #[source]
        source: Box<photon_evt::Error>,
    },
}

impl DecodeError {
    /// The sequence number of the record that failed.
    pub fn sequence_number(&self) -> &str {
        match self {
            Self::MissingNewImage {
                sequence_number, ..
            }
            | Self::Record {
                sequence_number, ..
            } => sequence_number,
        }
    }
}

/// A filter that cannot be turned into an event source mapping filter.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FilterError {
    /// An empty value or prefix, which would match nothing or everything.
    #[error("a filter {0} cannot be empty")]
    Empty(&'static str),

    /// The pattern is longer than Lambda accepts.
    #[error("the filter pattern is {len} bytes; Lambda accepts at most {max}")]
    TooLong {
        /// The pattern's length.
        len: usize,
        /// [`MAX_PATTERN_BYTES`](crate::MAX_PATTERN_BYTES).
        max: usize,
    },
}

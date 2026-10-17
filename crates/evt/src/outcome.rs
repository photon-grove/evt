//! Durable command outcomes and rejections (ADR 0004, C3 and C4).
//!
//! A store with outcomes enabled records every command on a reserved `command-outcome#<id>` row,
//! written once, in its final state, inside the commit transaction. A repeat of the command (same
//! command ID, same identity digest) returns the stored outcome without running the handler; a
//! repeat with another digest is [`Error::IdempotencyKeyReuse`]; a domain error becomes a durable
//! rejected outcome that a retried command replays verbatim.
//!
//! See `docs/spec/storage-format.md` ("Command Outcomes", "Rejections") for the stored shape. The
//! pieces here are:
//!
//! - [`Outcome`], the stored row, and [`CommandOutcome`], what an execution returns;
//! - [`Rejection`], [`RejectionCode`], and [`ToRejection`], the persisted domain refusal;
//! - [`DigestInput`], [`default_digest`], and [`OutcomeConfig`], the command identity digest;
//! - [`CommandIdentity`] and [`OutcomeCommand`], how a command describes itself to the digest.

use std::borrow::Cow;
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{BoxError, Error};
use crate::event::SerializedEvent;
use crate::ids::{CommandId, EntityId, EventId};
use crate::json;
use crate::metadata::Metadata;
use crate::repository::Repository;
use crate::sha256::sha256_hex;

/// The longest command ID a store with outcomes accepts, in UTF-8 bytes. The outcome partition key
/// is the 16-byte `command-outcome#` prefix plus the ID.
pub const MAX_COMMAND_ID_BYTES: usize = 512;

/// The largest stored response, and separately the largest encoded rejection, in bytes.
pub const MAX_OUTCOME_BYTES: usize = 65536;

/// The tag that pins the canonical identity encoding. A change to the encoding bumps it.
const DIGEST_TAG: &str = "evt-cmd-digest-v1";

/// The final status of a recorded command.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OutcomeStatus {
    /// The engine accepted the command. It may have written events, or none (an accepted no-op).
    Accepted,
    /// The command was refused by a domain rejection. Terminal for the command's digest.
    Rejected,
}

impl OutcomeStatus {
    /// The stored `status` text: `accepted` or `rejected`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
        }
    }

    /// Parses the stored `status` text.
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "accepted" => Some(Self::Accepted),
            "rejected" => Some(Self::Rejected),
            _ => None,
        }
    }
}

impl fmt::Display for OutcomeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A free-form, machine-readable reason for a rejection. The engine provides
/// [`NOT_FOUND`](RejectionCode::NOT_FOUND), [`BAD_COMMAND`](RejectionCode::BAD_COMMAND), and
/// [`STALE_FENCE`](RejectionCode::STALE_FENCE); applications add their own.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct RejectionCode(Cow<'static, str>);

impl RejectionCode {
    /// A requested entity or record does not exist.
    pub const NOT_FOUND: Self = Self(Cow::Borrowed("not-found"));

    /// The command itself was malformed or invalid.
    pub const BAD_COMMAND: Self = Self(Cow::Borrowed("bad-command"));

    /// A caller-stamped read fence went stale before the commit.
    pub const STALE_FENCE: Self = Self(Cow::Borrowed("stale-fence"));

    /// Creates an application-defined code.
    pub fn new(code: impl Into<String>) -> Self {
        Self(Cow::Owned(code.into()))
    }

    /// The code text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Reports whether the code is empty. A rejection needs a non-empty code.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'de> Deserialize<'de> for RejectionCode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::new(
            Option::<String>::deserialize(deserializer)?.unwrap_or_default(),
        ))
    }
}

impl fmt::Display for RejectionCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for RejectionCode {
    fn from(code: &str) -> Self {
        Self::new(code)
    }
}

impl From<String> for RejectionCode {
    fn from(code: String) -> Self {
        Self::new(code)
    }
}

impl PartialEq<str> for RejectionCode {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for RejectionCode {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

/// The standard, persisted domain rejection. Its JSON encoding is the stored form: keys in the
/// order `code`, `message`, `details`, with `details` `null` when absent.
///
/// `details` is a JSON object of application details or `None`. Numbers inside it are not
/// normalized across languages, so keep them integral for a replay that is byte-identical in Go.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rejection {
    /// The machine-readable reason.
    #[serde(deserialize_with = "json::null_default")]
    pub code: RejectionCode,
    /// A human-readable explanation.
    #[serde(default, deserialize_with = "json::null_default")]
    pub message: String,
    /// Application details, or `None` (stored as `null`).
    #[serde(default)]
    pub details: Option<serde_json::Value>,
}

impl Default for RejectionCode {
    fn default() -> Self {
        Self::new("")
    }
}

impl Rejection {
    /// Creates a rejection without details.
    pub fn new(code: impl Into<RejectionCode>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }

    /// Attaches application details. They must be a JSON object; `null` clears them. A rejection
    /// with another kind of value is refused when it is stored, with [`Error::InvalidArgument`].
    #[must_use]
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = (!details.is_null()).then_some(details);
        self
    }

    /// The engine's [`NOT_FOUND`](RejectionCode::NOT_FOUND) rejection.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(RejectionCode::NOT_FOUND, message)
    }

    /// The engine's [`BAD_COMMAND`](RejectionCode::BAD_COMMAND) rejection.
    pub fn bad_command(message: impl Into<String>) -> Self {
        Self::new(RejectionCode::BAD_COMMAND, message)
    }

    /// The engine's [`STALE_FENCE`](RejectionCode::STALE_FENCE) rejection, recorded when a
    /// caller-stamped read fence went stale before the commit.
    pub fn stale_fence() -> Self {
        Self::new(
            RejectionCode::STALE_FENCE,
            "a caller-stamped fence went stale",
        )
    }

    /// Encodes the rejection as its stored JSON, byte for byte as Go's `json.Marshal` writes it.
    pub fn encode(&self) -> Result<String, Error> {
        if let Some(details) = &self.details
            && !details.is_object()
        {
            return Err(Error::InvalidArgument(
                "rejection details must be a JSON object or absent".into(),
            ));
        }

        Ok(json::to_string(self)?)
    }

    /// The rejection as it reads back from storage (encoded and decoded). A first execution
    /// returns this form so it equals a replay on every backend.
    pub fn canonical(&self) -> Result<Self, Error> {
        Self::decode(&self.encode()?)
    }

    /// Parses the stored JSON form of a rejection.
    pub fn decode(text: &str) -> Result<Self, Error> {
        let mut rejection: Rejection = serde_json::from_str(text)
            .map_err(|err| Error::InvalidRecord(format!("decode rejection: {err}")))?;
        // A stored `null` and an absent `details` are the same thing.
        rejection.details = rejection.details.filter(|details| !details.is_null());

        // Details are a JSON object or absent; anything else is a corrupt row, as in Go.
        if rejection
            .details
            .as_ref()
            .is_some_and(|details| !details.is_object())
        {
            return Err(Error::InvalidRecord(
                "decode rejection: details must be a JSON object or null".into(),
            ));
        }

        Ok(rejection)
    }
}

/// Maps an aggregate's `Error` onto the standard [`Rejection`]. Implement it for the type: the
/// outcome-aware methods require it. `Some` is a domain rejection (not found, invalid,
/// conflict-by-rule), which is persisted as a rejected outcome and replayed verbatim by a retry;
/// `None` is an internal failure the handler could not decide (a dependency was down, a bug), which
/// is returned as [`Error::Handler`] and writes no outcome, so a retry after the fix starts clean.
/// Failures outside the handler (encoding, storage, replay, projectors) are [`Error`]s already and
/// never reach this trait.
///
/// ```
/// use photon_evt::{Rejection, RejectionCode, ToRejection};
///
/// #[derive(Debug, thiserror::Error)]
/// enum AccountError {
///     #[error("handle {0} is taken")]
///     HandleTaken(String),
/// }
///
/// impl ToRejection for AccountError {
///     fn to_rejection(&self) -> Option<Rejection> {
///         match self {
///             Self::HandleTaken(handle) => Some(
///                 Rejection::new("username-taken", self.to_string())
///                     .with_details(serde_json::json!({ "handle": handle })),
///             ),
///         }
///     }
/// }
/// ```
pub trait ToRejection {
    /// The rejection that records this error, or `None` for an internal failure that must not be
    /// persisted.
    fn to_rejection(&self) -> Option<Rejection>;
}

impl ToRejection for Rejection {
    fn to_rejection(&self) -> Option<Rejection> {
        Some(self.clone())
    }
}

impl ToRejection for Infallible {
    fn to_rejection(&self) -> Option<Rejection> {
        match *self {}
    }
}

/// The durable record of one command: the row written once, in its final state, on the reserved
/// `command-outcome#<id>` stream inside the commit transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Outcome {
    /// The command ID the row is keyed by.
    pub command_id: CommandId,
    /// Accepted or rejected, final at write time.
    pub status: OutcomeStatus,
    /// The identity digest the command was recorded under.
    pub digest: String,
    /// The stored response bytes, `None` when the command returned none.
    pub response: Option<Vec<u8>>,
    /// The events the commit wrote, ordered by stream ID bytewise, then sequence. Empty for a
    /// rejection and for an accepted no-op.
    pub event_ids: Vec<EventId>,
    /// The rejection, set exactly when the status is [`Rejected`](OutcomeStatus::Rejected).
    pub rejection: Option<Rejection>,
}

impl Outcome {
    /// Checks an outcome decoded from storage: a digest, a rejection with a code exactly when the
    /// status is rejected, no event IDs on a rejection, and well-formed event IDs
    /// (`{entityId}:{sequence}` with a positive, canonical sequence). A malformed row is
    /// [`Error::InvalidRecord`], never replayed as a result. Every backend runs it on the rows it
    /// reads, with the same rules as Go's `ValidateStoredOutcome`.
    pub fn validate_stored(&self) -> Result<(), Error> {
        let corrupt = |reason: String| {
            Err(Error::InvalidRecord(format!(
                "stored outcome for command {} is corrupt: {reason}",
                self.command_id
            )))
        };

        if self.digest.is_empty() {
            return corrupt("the digest is missing".into());
        }

        match self.status {
            OutcomeStatus::Accepted if self.rejection.is_some() => {
                return corrupt("an accepted outcome carries a rejection".into());
            }
            OutcomeStatus::Rejected => match &self.rejection {
                Some(rejection) if !rejection.code.as_str().is_empty() => {
                    if !self.event_ids.is_empty() {
                        return corrupt("a rejected outcome lists events".into());
                    }
                }
                _ => return corrupt("a rejected outcome has no rejection code".into()),
            },
            OutcomeStatus::Accepted => {}
        }

        for id in &self.event_ids {
            let canonical = id
                .as_str()
                .rsplit_once(':')
                .filter(|(entity, _)| !entity.is_empty())
                .and_then(|(_, sequence)| {
                    sequence
                        .parse::<i64>()
                        .ok()
                        .filter(|parsed| *parsed >= 1 && parsed.to_string() == sequence)
                });
            if canonical.is_none() {
                return corrupt(format!(
                    "event ID {id:?} is not {{entityId}}:{{sequence}} with a positive canonical sequence"
                ));
            }
        }

        Ok(())
    }

    /// An accepted outcome with no response. The commit fills the event IDs it writes.
    pub fn accepted(command_id: impl Into<CommandId>, digest: impl Into<String>) -> Self {
        Self {
            command_id: command_id.into(),
            status: OutcomeStatus::Accepted,
            digest: digest.into(),
            response: None,
            event_ids: Vec::new(),
            rejection: None,
        }
    }

    /// A rejected outcome.
    pub fn rejected(
        command_id: impl Into<CommandId>,
        digest: impl Into<String>,
        rejection: Rejection,
    ) -> Self {
        Self {
            command_id: command_id.into(),
            status: OutcomeStatus::Rejected,
            digest: digest.into(),
            response: None,
            event_ids: Vec::new(),
            rejection: Some(rejection),
        }
    }

    /// Stores response bytes with the outcome. An empty response is stored as none.
    #[must_use]
    pub fn with_response(mut self, response: impl Into<Vec<u8>>) -> Self {
        let response = response.into();
        self.response = (!response.is_empty()).then_some(response);
        self
    }

    /// Reports whether the command was accepted.
    pub fn is_accepted(&self) -> bool {
        self.status == OutcomeStatus::Accepted
    }

    /// Reports whether the command was rejected.
    pub fn is_rejected(&self) -> bool {
        self.status == OutcomeStatus::Rejected
    }

    /// Reports whether the command was accepted without writing any event.
    pub fn is_noop(&self) -> bool {
        self.is_accepted() && self.event_ids.is_empty()
    }

    /// The outcome as the commit stores it: the response normalized, the event IDs replaced by
    /// the ones the commit writes.
    pub(crate) fn stored(&self, event_ids: Vec<EventId>) -> Self {
        let mut stored = self.clone();
        stored.response = stored.response.filter(|response| !response.is_empty());
        stored.event_ids = event_ids;

        stored
    }
}

/// What executing a command on a store with outcomes returns: the recorded [`Outcome`], plus what
/// this call did.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommandOutcome<E = ()> {
    /// The recorded outcome: the row this call wrote, or the stored row it replayed.
    pub outcome: Outcome,
    /// The events this call committed, in the order the commit persists them. Empty when the
    /// outcome was replayed, and for a rejection or a no-op.
    pub events: Vec<SerializedEvent>,
    /// The typed events the handler decided, when the call ran a handler that returned them.
    pub typed_events: Vec<E>,
    /// The outcome was read back from a stored row (a duplicate command, a lost race resolved by
    /// the winner's row, or ambiguous-write recovery) rather than produced by this call.
    pub replayed: bool,
}

impl<E> CommandOutcome<E> {
    pub(crate) fn fresh(
        outcome: Outcome,
        events: Vec<SerializedEvent>,
        typed_events: Vec<E>,
    ) -> Self {
        Self {
            outcome,
            events,
            typed_events,
            replayed: false,
        }
    }

    pub(crate) fn replay(outcome: Outcome) -> Self {
        Self {
            outcome,
            events: Vec::new(),
            typed_events: Vec::new(),
            replayed: true,
        }
    }

    /// Reports whether the command was accepted.
    pub fn is_accepted(&self) -> bool {
        self.outcome.is_accepted()
    }

    /// Reports whether the command was rejected.
    pub fn is_rejected(&self) -> bool {
        self.outcome.is_rejected()
    }

    /// Reports whether the command was accepted without writing any event.
    pub fn is_noop(&self) -> bool {
        self.outcome.is_noop()
    }

    /// The stored response bytes, if any.
    pub fn response(&self) -> Option<&[u8]> {
        self.outcome.response.as_deref()
    }

    /// The rejection, if the command was rejected.
    pub fn rejection(&self) -> Option<&Rejection> {
        self.outcome.rejection.as_ref()
    }
}

/// The identity of a command: what it is, never what the executed commit wrote.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DigestInput {
    /// The command's type string.
    pub command_type: String,
    /// The stream IDs the command declares it will write. They are deduplicated and sorted by byte
    /// order when digested. Fenced (read-only) streams are not part of it.
    pub streams: Vec<EntityId>,
    /// The canonical JSON of the command payload; empty when the command has none.
    pub payload: Vec<u8>,
    /// The command metadata's `principalId`, empty when unset.
    pub principal: String,
}

/// Computes a command's identity digest before the command executes. A custom digester stores its
/// own opaque string; recovery compares digests for equality only.
pub type Digester = Arc<dyn Fn(&DigestInput) -> Result<String, BoxError> + Send + Sync>;

/// Returns the canonical bytes the default digest hashes:
///
/// ```text
/// "evt-cmd-digest-v1" || 0x00
/// || lp(command type)
/// || uint32-le(stream count) || lp(stream ID) ...
/// || lp(canonical payload JSON)
/// || lp(principal)
/// ```
///
/// where `lp(x)` is the little-endian `uint32` length of `x` followed by `x`.
pub fn canonical_digest_bytes(input: &DigestInput) -> Vec<u8> {
    let mut streams: Vec<&EntityId> = input.streams.iter().collect();
    streams.sort();
    streams.dedup();

    let mut bytes = Vec::with_capacity(
        DIGEST_TAG.len()
            + 1
            + input.command_type.len()
            + input.payload.len()
            + input.principal.len()
            + 20
            + streams
                .iter()
                .map(|id| id.as_str().len() + 4)
                .sum::<usize>(),
    );

    bytes.extend_from_slice(DIGEST_TAG.as_bytes());
    bytes.push(0);
    length_prefixed(&mut bytes, input.command_type.as_bytes());

    bytes.extend_from_slice(&(streams.len() as u32).to_le_bytes());
    for stream in streams {
        length_prefixed(&mut bytes, stream.as_str().as_bytes());
    }

    length_prefixed(&mut bytes, &input.payload);
    length_prefixed(&mut bytes, input.principal.as_bytes());

    bytes
}

fn length_prefixed(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}

/// The default [`Digester`]: SHA-256 over [`canonical_digest_bytes`], as lowercase hex.
pub fn default_digest(input: &DigestInput) -> Result<String, BoxError> {
    Ok(sha256_hex(&canonical_digest_bytes(input)))
}

/// Encodes a value as canonical payload JSON for a command's identity digest: the same encoder as
/// every JSON document in the storage format, so Go and Rust digest equal payloads equally.
/// Payloads that must digest identically in both languages use integers, not floats.
pub fn canonical_json<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, BoxError> {
    Ok(json::to_vec(value)?)
}

/// How a command describes itself to the identity digest. Implement it for an aggregate's
/// `Command` type to use [`Store::execute_with_outcome`](crate::Store::execute_with_outcome).
///
/// ```
/// use photon_evt::{BoxError, OutcomeCommand, canonical_json};
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Deposit {
///     cents: i64,
/// }
///
/// impl OutcomeCommand for Deposit {
///     fn command_type(&self) -> String {
///         "account:deposit".into()
///     }
///
///     fn canonical_payload(&self) -> Result<Option<Vec<u8>>, BoxError> {
///         Ok(Some(canonical_json(self)?))
///     }
/// }
/// ```
pub trait OutcomeCommand {
    /// The command's type string.
    fn command_type(&self) -> String;

    /// The canonical JSON of the command's payload, or `None` when it has none (digested as the
    /// zero-length byte string). Use [`canonical_json`] to encode a `Serialize` payload.
    fn canonical_payload(&self) -> Result<Option<Vec<u8>>, BoxError> {
        Ok(None)
    }
}

/// Describes a command to a store with outcomes when there is no single command value to derive
/// its identity from, such as a multi-stream transaction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommandIdentity {
    command_type: String,
    streams: Vec<EntityId>,
    payload: Vec<u8>,
}

impl CommandIdentity {
    /// Starts an identity with the command's type string.
    pub fn new(command_type: impl Into<String>) -> Self {
        Self {
            command_type: command_type.into(),
            streams: Vec::new(),
            payload: Vec::new(),
        }
    }

    /// Declares the streams the command will write events to. Fenced read-only streams are not
    /// declared. An outcome-only command declares none.
    #[must_use]
    pub fn with_streams<I, S>(mut self, streams: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<EntityId>,
    {
        self.streams = streams.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the payload, encoded canonically into the digest.
    pub fn with_payload<T: Serialize + ?Sized>(mut self, payload: &T) -> Result<Self, Error> {
        self.payload = json::to_vec(payload)?;

        Ok(self)
    }

    /// Sets the payload from already-canonical JSON bytes.
    #[must_use]
    pub fn with_payload_json(mut self, payload: impl Into<Vec<u8>>) -> Self {
        self.payload = payload.into();
        self
    }

    /// Builds the digest input for this identity and the command metadata's principal.
    pub fn digest_input(&self, metadata: &Metadata) -> DigestInput {
        DigestInput {
            command_type: self.command_type.clone(),
            streams: self.streams.clone(),
            payload: self.payload.clone(),
            principal: principal(metadata),
        }
    }
}

/// The metadata's principal as the digest sees it: `""` when unset.
pub(crate) fn principal(metadata: &Metadata) -> String {
    metadata
        .principal_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default()
}

/// Enables command outcomes on a [`Store`](crate::Store) or a
/// [`Transaction`](crate::Transaction).
#[derive(Clone)]
pub struct OutcomeConfig {
    digester: Digester,
    recovery_wait: Duration,
}

impl Default for OutcomeConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for OutcomeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutcomeConfig").finish_non_exhaustive()
    }
}

impl OutcomeConfig {
    /// Outcomes with the default identity digest ([`default_digest`]).
    pub fn new() -> Self {
        Self {
            digester: Arc::new(default_digest),
            recovery_wait: Duration::from_secs(10),
        }
    }

    /// Bounds how long ambiguous-write recovery polls the outcome row for a commit whose result is
    /// unknown, 10 seconds by default. If the row has not appeared when the window ends the command
    /// fails with the retryable [`Error::AmbiguousWrite`].
    #[must_use]
    pub fn with_recovery_wait(mut self, wait: Duration) -> Self {
        self.recovery_wait = wait;
        self
    }

    pub(crate) fn recovery_wait(&self) -> Duration {
        self.recovery_wait
    }

    /// Replaces the identity digest. The digester computes an opaque string from the command's
    /// identity before it executes; recovery compares the strings for equality only.
    #[must_use]
    pub fn with_digester(
        mut self,
        digester: impl Fn(&DigestInput) -> Result<String, BoxError> + Send + Sync + 'static,
    ) -> Self {
        self.digester = Arc::new(digester);
        self
    }

    pub(crate) fn digest(&self, input: &DigestInput) -> Result<String, Error> {
        let digest = (self.digester)(input)
            .map_err(|err| Error::InvalidArgument(format!("the command digester failed: {err}")))?;

        if digest.is_empty() {
            return Err(Error::InvalidArgument(
                "the command digester returned an empty digest".into(),
            ));
        }

        Ok(digest)
    }
}

/// Checks a command ID before any read or write: required when outcomes are enabled
/// ([`Error::InvalidArgument`]) and at most [`MAX_COMMAND_ID_BYTES`] UTF-8 bytes
/// ([`Error::CommandIdTooLong`]). Command IDs, principals, and digests are Rust `String`s, which
/// are UTF-8 by construction, so unlike Go there is no invalid-encoding case to refuse.
pub fn check_command_id(command_id: &CommandId) -> Result<(), Error> {
    if command_id.is_empty() {
        return Err(Error::InvalidArgument(
            "a command ID is required when command outcomes are enabled".into(),
        ));
    }

    if command_id.as_str().len() > MAX_COMMAND_ID_BYTES {
        return Err(Error::CommandIdTooLong {
            length: command_id.as_str().len(),
        });
    }

    Ok(())
}

/// The command ID of the metadata, checked with [`check_command_id`]. An empty ID is absent.
pub(crate) fn require_command_id(metadata: &Metadata) -> Result<CommandId, Error> {
    let command_id = metadata
        .command_id
        .clone()
        .filter(|command_id| !command_id.is_empty())
        .ok_or_else(|| {
            Error::InvalidArgument(
                "a command ID is required when command outcomes are enabled".into(),
            )
        })?;

    check_command_id(&command_id)?;

    Ok(command_id)
}

/// Validates an outcome before any write, in the order the spec pins: the command ID first, then
/// the response bound, then the rejection bound.
pub fn check_outcome(outcome: &Outcome) -> Result<(), Error> {
    check_command_id(&outcome.command_id)?;

    if outcome.digest.is_empty() {
        return Err(Error::InvalidArgument(
            "an outcome needs an identity digest".into(),
        ));
    }

    match outcome.status {
        OutcomeStatus::Accepted => {
            if outcome.rejection.is_some() {
                return Err(Error::InvalidArgument(
                    "an accepted outcome carries no rejection".into(),
                ));
            }
        }
        OutcomeStatus::Rejected => {
            if outcome
                .rejection
                .as_ref()
                .is_none_or(|rejection| rejection.code.is_empty())
            {
                return Err(Error::InvalidArgument(
                    "a rejected outcome needs a rejection with a code".into(),
                ));
            }
        }
    }

    let response = outcome.response.as_ref().map_or(0, Vec::len);
    if response > MAX_OUTCOME_BYTES {
        return Err(Error::OutcomeTooLarge {
            what: "response",
            size: response,
        });
    }

    if let Some(rejection) = &outcome.rejection {
        let size = rejection.encode()?.len();
        if size > MAX_OUTCOME_BYTES {
            return Err(Error::OutcomeTooLarge {
                what: "rejection",
                size,
            });
        }
    }

    Ok(())
}

/// Encodes event IDs as the stored JSON array.
pub fn encode_event_ids(ids: &[EventId]) -> Result<String, Error> {
    Ok(json::to_string(ids)?)
}

/// Parses the stored JSON array of event IDs.
pub fn decode_event_ids(text: &str) -> Result<Vec<EventId>, Error> {
    Ok(serde_json::from_str::<Option<Vec<EventId>>>(text)
        .map_err(|err| Error::InvalidRecord(format!("decode event IDs: {err}")))?
        .unwrap_or_default())
}

/// How a commit that carried an outcome ended, once races and ambiguity are resolved.
pub(crate) enum Settled {
    /// This call's commit applied.
    Committed,
    /// Another commit already recorded the outcome (a lost race or an applied ambiguous write).
    Replayed(Outcome),
}

/// What polling for an outcome row found.
#[derive(Debug)]
pub(crate) enum Polled {
    /// The row exists: the commit landed (or another writer's did).
    Found(Box<Outcome>),
    /// The window passed without a row.
    Absent,
    /// The row could not be read, so nothing can be said.
    Unreadable,
}

/// Polls `read` for an outcome row with exponential backoff (50 ms, doubling to 1 s) until it
/// appears or `window` has been slept through. `sleep` is the repository's timer
/// ([`Repository::backoff`]), so the core stays runtime-agnostic; the elapsed time is the sum of
/// the delays, which keeps the wait deterministic.
pub(crate) async fn poll_for_row<R, RF, S, SF>(
    mut read: R,
    mut sleep: S,
    window: Duration,
) -> Polled
where
    R: FnMut() -> RF,
    RF: Future<Output = Result<Option<Outcome>, Error>>,
    S: FnMut(Duration) -> SF,
    SF: Future<Output = ()>,
{
    let mut delay = Duration::from_millis(50);
    let mut waited = Duration::ZERO;

    loop {
        match read().await {
            Ok(Some(outcome)) => return Polled::Found(Box::new(outcome)),
            Ok(None) => {}
            Err(_) => return Polled::Unreadable,
        }

        if waited >= window {
            return Polled::Absent;
        }

        let step = delay.min(window - waited);
        sleep(step).await;
        waited += step;
        delay = (delay * 2).min(Duration::from_secs(1));
    }
}

/// Compares a stored outcome with the digest this attempt computed: the same digest returns the
/// stored outcome, another is [`Error::IdempotencyKeyReuse`].
pub(crate) fn same_command(stored: Outcome, digest: &str) -> Result<Outcome, Error> {
    if stored.digest == digest {
        return Ok(stored);
    }

    Err(Error::IdempotencyKeyReuse {
        command_id: stored.command_id.clone(),
        prior_outcome: Box::new(stored),
    })
}

/// The pre-check every attempt starts with: reads the outcome row by command ID (strongly
/// consistent) and returns the stored outcome for the same digest, `None` when absent, and
/// [`Error::IdempotencyKeyReuse`] for another digest.
pub(crate) async fn pre_check<R: Repository>(
    repository: &R,
    command_id: &CommandId,
    digest: &str,
) -> Result<Option<Outcome>, Error> {
    match repository.outcome(command_id).await? {
        Some(stored) => same_command(stored, digest).map(Some),
        None => Ok(None),
    }
}

/// Resolves the result of a commit that carried an outcome:
///
/// - a lost outcome put ([`Error::OutcomeConflict`]) resolves through the winner's row;
/// - an indeterminate failure ([`Error::Indeterminate`], which backends return only for a commit
///   whose result is genuinely unknown) is recovered by polling the row for the recovery
///   `window` (bounded backoff via [`Repository::backoff`]): present with the same digest, the
///   commit won; another digest, [`Error::IdempotencyKeyReuse`]; still absent when the window ends
///   (or unreadable), a retryable [`Error::AmbiguousWrite`], never "not applied", because a
///   transaction can land after any single read. The handler is never re-run.
pub(crate) async fn settle<R: Repository>(
    repository: &R,
    command_id: &CommandId,
    digest: &str,
    window: Duration,
    result: Result<(), Error>,
) -> Result<Settled, Error> {
    match result {
        Ok(()) => Ok(Settled::Committed),
        Err(conflict @ Error::OutcomeConflict { .. }) => {
            match repository.outcome(command_id).await? {
                Some(stored) => Ok(Settled::Replayed(same_command(stored, digest)?)),
                // The row that won the race is gone again; nothing can be said about the command.
                None => Err(conflict),
            }
        }
        Err(Error::Indeterminate(source)) => {
            let polled = poll_for_row(
                || repository.outcome(command_id),
                |step| repository.backoff(step),
                window,
            )
            .await;

            match polled {
                Polled::Found(stored) => Ok(Settled::Replayed(same_command(*stored, digest)?)),
                Polled::Absent | Polled::Unreadable => Err(Error::AmbiguousWrite {
                    command_id: command_id.clone(),
                    source,
                }),
            }
        }
        // An ordinary conflict may be the loser of a race for the same command ID: the winner's
        // row exists by now. Re-read once, so the retry's pre-check is not needed to find it.
        // Fence conflicts are not ordinary conflicts here; a failed read keeps the conflict.
        Err(conflict @ (Error::Conflict { .. } | Error::SequenceGap { .. })) => {
            match repository.outcome(command_id).await {
                Ok(Some(stored)) => Ok(Settled::Replayed(same_command(stored, digest)?)),
                _ => Err(conflict),
            }
        }
        Err(other) => Err(other),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    #[tokio::test]
    async fn polling_finds_a_row_that_appears_late() {
        let reads = Cell::new(0);
        let slept = Cell::new(Duration::ZERO);

        let polled = poll_for_row(
            || {
                reads.set(reads.get() + 1);
                let row = (reads.get() >= 4).then(|| Outcome::accepted("cmd", "digest"));
                async move { Ok(row) }
            },
            |step| {
                slept.set(slept.get() + step);
                async {}
            },
            Duration::from_secs(10),
        )
        .await;

        assert!(matches!(polled, Polled::Found(_)), "{polled:?}");
        assert_eq!(reads.get(), 4);
        // 50 + 100 + 200 ms of backoff before the fourth read finds the row.
        assert_eq!(slept.get(), Duration::from_millis(350));
    }

    #[tokio::test]
    async fn polling_expires_at_the_window_and_never_overshoots_it() {
        let slept = Cell::new(Duration::ZERO);

        let polled = poll_for_row(
            || async { Ok(None) },
            |step| {
                slept.set(slept.get() + step);
                async {}
            },
            Duration::from_secs(10),
        )
        .await;

        assert!(matches!(polled, Polled::Absent), "{polled:?}");
        assert_eq!(slept.get(), Duration::from_secs(10));
    }

    #[tokio::test]
    async fn an_unreadable_row_ends_polling_without_a_verdict() {
        let polled = poll_for_row(
            || async { Err(Error::backend("table unavailable")) },
            |_| async {},
            Duration::from_secs(10),
        )
        .await;

        assert!(matches!(polled, Polled::Unreadable), "{polled:?}");
    }

    #[test]
    fn a_stored_rejection_must_be_one_object_with_object_details() {
        for stored in [
            r#"{"code":"c","message":"m","details":["x"]}"#,
            r#"{"code":"c","message":"m","details":"x"}"#,
            r#"{"code":"c","message":"m","details":1}"#,
            r#"{"code":"c","message":"m","details":null} {}"#,
            r#"{"code":"c","message":"m","details":null}x"#,
        ] {
            assert!(
                matches!(Rejection::decode(stored), Err(Error::InvalidRecord(_))),
                "{stored}"
            );
        }

        assert!(Rejection::decode(r#"{"code":"c","message":"m","details":{"a":1}} "#).is_ok());
    }

    #[test]
    fn stored_outcomes_are_validated_like_go() {
        let accepted = Outcome::accepted("cmd", "digest");
        assert!(accepted.validate_stored().is_ok());

        let mut with_events = accepted.clone();
        with_events.event_ids = vec!["a:1".into(), "b:12".into()];
        assert!(with_events.validate_stored().is_ok());

        for bad in ["a", "a:0", "a:01", ":1", "a:-1", "a:x", "a:+1"] {
            let mut outcome = accepted.clone();
            outcome.event_ids = vec![bad.into()];

            assert!(
                matches!(outcome.validate_stored(), Err(Error::InvalidRecord(_))),
                "{bad:?}"
            );
        }

        let mut no_digest = accepted.clone();
        no_digest.digest.clear();
        assert!(no_digest.validate_stored().is_err());

        let mut rejected = Outcome::rejected("cmd", "digest", Rejection::new("c", "m"));
        assert!(rejected.validate_stored().is_ok());
        rejected.event_ids = vec!["a:1".into()];
        assert!(rejected.validate_stored().is_err());

        let mut coded = Outcome::accepted("cmd", "digest");
        coded.rejection = Some(Rejection::new("c", "m"));
        assert!(coded.validate_stored().is_err());
    }
    use super::*;

    fn input(command_type: &str, streams: &[&str], payload: &str, principal: &str) -> DigestInput {
        DigestInput {
            command_type: command_type.into(),
            streams: streams.iter().map(|id| EntityId::new(*id)).collect(),
            payload: payload.as_bytes().to_vec(),
            principal: principal.into(),
        }
    }

    fn digest(input: &DigestInput) -> String {
        default_digest(input).unwrap()
    }

    #[test]
    fn the_default_digest_reproduces_the_spec_examples() {
        let cases = [
            (
                "single stream",
                input("account:open", &["acct-1"], r#"{"owner":"Ada"}"#, "user:42"),
                "08f63bf06ef7167f79dda7f5de9307e1ffb626de015ecbc1c1482d1e67e5abd6",
            ),
            (
                "multi stream, declaration order does not matter",
                input(
                    "claim-username",
                    &["profile#u-1", "claim#ada"],
                    r#"{"handle":"ada"}"#,
                    "",
                ),
                "21c04bf818ab67db244dc184ae98197928309a4b3a3533acba69dacf992b6d79",
            ),
            (
                "multi stream with a principal",
                input(
                    "claim-username",
                    &["claim#ada", "profile#u-1"],
                    r#"{"handle":"ada"}"#,
                    "user:42",
                ),
                "56d01f5b9788b60e12a686e40807c28d9dfde802d82faf46888adda0acee7050",
            ),
            (
                "absent payload",
                input("account:ping", &["acct-1"], "", "user:42"),
                "d24c7d5ce955a76c936a83e6eca2c3bc277d936c88a76460ed2d221e285a215a",
            ),
            (
                "stale fence",
                input(
                    "account:rename",
                    &["acct-1"],
                    r#"{"name":"Grace"}"#,
                    "user:42",
                ),
                "ba4a6a859dcf103a00261515d7e309b31e8f07ca21732a4b7872b7fdebb92c1b",
            ),
        ];

        for (name, input, want) in cases {
            assert_eq!(digest(&input), want, "{name}");
        }
    }

    #[test]
    fn the_canonical_bytes_match_the_spec_layout() {
        let bytes = canonical_digest_bytes(&input(
            "account:open",
            &["acct-1"],
            r#"{"owner":"Ada"}"#,
            "user:42",
        ));

        assert_eq!(bytes.len(), 78, "the spec's worked example is 78 bytes");
        assert!(bytes.starts_with(b"evt-cmd-digest-v1\0"));
        assert_eq!(&bytes[18..22], &[0x0c, 0, 0, 0], "lp(command type)");
    }

    #[test]
    fn streams_are_deduplicated_and_sorted_by_bytes() {
        assert_eq!(
            canonical_digest_bytes(&input("t", &["b", "a", "b"], "", "")),
            canonical_digest_bytes(&input("t", &["a", "b"], "", ""))
        );
        // Byte order, not locale order: uppercase sorts before lowercase.
        assert_eq!(
            canonical_digest_bytes(&input("t", &["a", "B"], "", "")),
            canonical_digest_bytes(&input("t", &["B", "a"], "", ""))
        );
    }

    #[test]
    fn a_null_payload_differs_from_no_payload() {
        assert_ne!(
            digest(&input("t", &[], "null", "")),
            digest(&input("t", &[], "", ""))
        );
    }

    #[test]
    fn an_identity_encodes_its_payload_canonically() {
        #[derive(Serialize)]
        struct Open {
            owner: &'static str,
        }

        let metadata = Metadata::default().with_principal_id("user:42");
        let identity = CommandIdentity::new("account:open")
            .with_streams(["acct-1"])
            .with_payload(&Open { owner: "Ada" })
            .unwrap();

        assert_eq!(
            digest(&identity.digest_input(&metadata)),
            "08f63bf06ef7167f79dda7f5de9307e1ffb626de015ecbc1c1482d1e67e5abd6"
        );

        let none = CommandIdentity::new("account:ping").with_streams(["acct-1"]);
        assert!(none.digest_input(&metadata).payload.is_empty());
        assert_eq!(
            digest(&none.digest_input(&metadata)),
            "d24c7d5ce955a76c936a83e6eca2c3bc277d936c88a76460ed2d221e285a215a"
        );
    }

    #[test]
    fn a_custom_digester_must_return_something() {
        let config = OutcomeConfig::new().with_digester(|_| Ok(String::new()));

        assert!(matches!(
            config.digest(&DigestInput::default()),
            Err(Error::InvalidArgument(_))
        ));

        let config = OutcomeConfig::new().with_digester(|input| Ok(input.command_type.clone()));
        assert_eq!(config.digest(&input("t", &[], "", "")).unwrap(), "t");

        let config = OutcomeConfig::new().with_digester(|_| Err("no digest".into()));
        assert!(matches!(
            config.digest(&DigestInput::default()),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn rejections_encode_as_the_spec_pins() {
        let rejection = Rejection::new("username-taken", "handle ada is taken")
            .with_details(serde_json::json!({ "handle": "ada" }));
        assert_eq!(
            rejection.encode().unwrap(),
            r#"{"code":"username-taken","message":"handle ada is taken","details":{"handle":"ada"}}"#
        );

        assert_eq!(
            Rejection::stale_fence().encode().unwrap(),
            r#"{"code":"stale-fence","message":"a caller-stamped fence went stale","details":null}"#
        );

        assert_eq!(
            Rejection::new("c", "a<b>&c").encode().unwrap(),
            r#"{"code":"c","message":"a\u003cb\u003e\u0026c","details":null}"#,
            "the shared JSON escape rules apply"
        );

        // Go sorts map keys; so does serde_json's default map.
        let nested = Rejection::new("c", "m").with_details(serde_json::json!({ "b": 1, "a": 2 }));
        assert_eq!(
            nested.encode().unwrap(),
            r#"{"code":"c","message":"m","details":{"a":2,"b":1}}"#
        );
    }

    #[test]
    fn rejection_details_must_be_an_object() {
        let mut rejection = Rejection::new("c", "m");
        rejection.details = Some(serde_json::json!([1, 2]));

        assert!(matches!(rejection.encode(), Err(Error::InvalidArgument(_))));
        assert!(
            Rejection::new("c", "m")
                .with_details(serde_json::Value::Null)
                .details
                .is_none(),
            "null clears the details"
        );
    }

    #[test]
    fn a_stored_rejection_decodes_and_re_encodes_verbatim() {
        let stored =
            r#"{"code":"limit","message":"too many","details":{"limit":9007199254740993,"n":2}}"#;

        let rejection = Rejection::decode(stored).unwrap();
        assert_eq!(rejection.code, "limit");
        assert_eq!(rejection.encode().unwrap(), stored);

        let bare = Rejection::decode(r#"{"code":"c","message":"m","details":null}"#).unwrap();
        assert_eq!(bare.details, None);
        assert!(matches!(
            Rejection::decode("not json"),
            Err(Error::InvalidRecord(_))
        ));
    }

    fn accepted(command_id: &str) -> Outcome {
        Outcome::accepted(command_id, "d")
    }

    #[test]
    fn the_command_id_is_checked_before_the_response_bound() {
        let long = "a".repeat(MAX_COMMAND_ID_BYTES + 1);
        let oversized = vec![0u8; MAX_OUTCOME_BYTES + 1];

        let mut outcome = accepted(&long);
        outcome.response = Some(oversized.clone());
        assert!(matches!(
            check_outcome(&outcome),
            Err(Error::CommandIdTooLong { length: 513 })
        ));

        // The limit is bytes, not characters: 257 two-byte characters are 514 bytes.
        let multibyte = accepted(&"é".repeat(MAX_COMMAND_ID_BYTES / 2 + 1));
        assert!(matches!(
            check_outcome(&multibyte),
            Err(Error::CommandIdTooLong { length: 514 })
        ));

        // The exact limits pass.
        let mut exact = accepted(&"a".repeat(MAX_COMMAND_ID_BYTES));
        exact.response = Some(vec![0u8; MAX_OUTCOME_BYTES]);
        check_outcome(&exact).unwrap();

        let mut too_big = accepted("cmd");
        too_big.response = Some(oversized);
        assert!(matches!(
            check_outcome(&too_big),
            Err(Error::OutcomeTooLarge {
                what: "response",
                size: 65537
            })
        ));

        let rejected = Outcome::rejected(
            "cmd",
            "d",
            Rejection::new("c", "x".repeat(MAX_OUTCOME_BYTES)),
        );
        assert!(matches!(
            check_outcome(&rejected),
            Err(Error::OutcomeTooLarge {
                what: "rejection",
                ..
            })
        ));

        assert!(matches!(
            check_outcome(&accepted("")),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn an_outcome_must_be_consistent() {
        let mut accepted_with_rejection = accepted("cmd");
        accepted_with_rejection.rejection = Some(Rejection::new("c", "m"));
        assert!(matches!(
            check_outcome(&accepted_with_rejection),
            Err(Error::InvalidArgument(_))
        ));

        let mut rejected_without = accepted("cmd");
        rejected_without.status = OutcomeStatus::Rejected;
        assert!(matches!(
            check_outcome(&rejected_without),
            Err(Error::InvalidArgument(_))
        ));

        let no_code = Outcome::rejected("cmd", "d", Rejection::new("", "m"));
        assert!(matches!(
            check_outcome(&no_code),
            Err(Error::InvalidArgument(_))
        ));

        assert!(matches!(
            check_outcome(&Outcome::accepted("cmd", "")),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn event_ids_round_trip_through_their_stored_json() {
        let ids = vec![EventId::new("claim#ada:1"), EventId::new("a<b:2")];

        let text = encode_event_ids(&ids).unwrap();
        assert_eq!(text, r#"["claim#ada:1","a\u003cb:2"]"#);
        assert_eq!(decode_event_ids(&text).unwrap(), ids);
        assert_eq!(decode_event_ids("null").unwrap(), Vec::<EventId>::new());
        assert!(decode_event_ids("{").is_err());
    }

    #[test]
    fn the_outcome_helpers_classify() {
        assert!(accepted("c").is_noop());

        let mut written = accepted("c");
        written.event_ids = vec![EventId::new("a:1")];
        assert!(written.is_accepted() && !written.is_noop());

        let rejected = Outcome::rejected("c", "d", Rejection::not_found("gone"));
        assert!(rejected.is_rejected() && !rejected.is_noop());
        assert_eq!(rejected.rejection.unwrap().code, RejectionCode::NOT_FOUND);

        assert_eq!(accepted("c").with_response(Vec::new()).response, None);
        assert_eq!(
            accepted("c").with_response(b"x".to_vec()).response,
            Some(b"x".to_vec())
        );
    }

    #[test]
    fn engine_codes_have_their_spec_text() {
        assert_eq!(RejectionCode::NOT_FOUND, "not-found");
        assert_eq!(RejectionCode::BAD_COMMAND, "bad-command");
        assert_eq!(RejectionCode::STALE_FENCE, "stale-fence");
    }
}

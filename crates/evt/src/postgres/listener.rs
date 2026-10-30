//! Journal wake-ups on PostgreSQL (ADR 0008, storage format: Journal on PostgreSQL): a journaled
//! commit through the views table runs `pg_notify` on the journal channel once per scope it wrote,
//! and a [`JournalListener`] turns those notifications into [`Wakeup`]s.
//!
//! A wake-up is not a delivery: the consumer pages the scope from its own cursor, as with a
//! DynamoDB stream record. Notifications sent while the listener is disconnected are lost, so the
//! listener reports every connection ([`Wakeup::Connected`]) and a quiet interval
//! ([`Wakeup::Poll`]), and the consumer pages every scope it follows then.

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use tokio::time::Instant;

use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{AsyncMessage, Client, Config, Connection, Socket};

use crate::error::Error;
use crate::journal::Scope;
use crate::wire::postgres::{MAX_EVENTS_TABLE_NAME_LEN, is_valid_identifier, journal_channel};

/// How long [`JournalListener::next`] waits for a notification before it returns
/// [`Wakeup::Poll`], by default.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How long a connection attempt, and the health check each [`Wakeup::Poll`] runs, may take, by
/// default.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Why a consumer should read: a scope that has new rows, or every scope it follows.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Wakeup {
    /// A commit wrote rows in this scope. Page it from your cursor.
    Scope(Scope),
    /// The listener (re)connected: notifications sent before may be lost. Page every scope.
    Connected,
    /// Nothing arrived for the poll interval: page every scope, as a backstop.
    Poll,
}

/// Listens on a PostgreSQL event log's journal channel over one dedicated connection (not a pooled
/// one: a pooled connection is shared, and `LISTEN` belongs to the session). It reconnects after
/// the connection drops, and drives the connection only while [`next`](Self::next) runs, so keep
/// calling it: PostgreSQL keeps notifications until every listening session has read them.
pub struct JournalListener<T>
where
    T: MakeTlsConnect<Socket>,
{
    config: Config,
    tls: T,
    channel: String,
    poll_interval: Duration,
    connect_timeout: Duration,
    /// When the next [`Wakeup::Poll`] is due: a fixed cadence from the last `Poll` or `Connected`,
    /// so steady traffic on some scopes never starves the backstop for the others.
    next_poll: Instant,
    /// Whether the connection's health check is due: set by each `Poll`, cleared once a check
    /// completes, so a call cancelled mid-check checks again on the next one.
    check_due: bool,
    connected: Option<(Client, Connection<Socket, T::Stream>)>,
    pending: VecDeque<Wakeup>,
}

impl<T> std::fmt::Debug for JournalListener<T>
where
    T: MakeTlsConnect<Socket>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalListener")
            .field("channel", &self.channel)
            .field("poll_interval", &self.poll_interval)
            .field("connected", &self.connected.is_some())
            .finish_non_exhaustive()
    }
}

impl<T> JournalListener<T>
where
    T: MakeTlsConnect<Socket> + Clone,
{
    /// A listener for the event log `events_table` (as configured on its repository), connecting
    /// with `config` and `tls`. It connects on the first [`next`](Self::next).
    ///
    /// # Errors
    ///
    /// [`Error::InvalidConfig`] for an events table name the repository would refuse.
    pub fn new(config: Config, tls: T, events_table: &str) -> Result<Self, Error> {
        if !is_valid_identifier(events_table, MAX_EVENTS_TABLE_NAME_LEN) {
            return Err(Error::InvalidConfig(format!(
                "postgres: invalid events table name {events_table:?}"
            )));
        }

        Ok(Self {
            config,
            tls,
            channel: journal_channel(events_table),
            poll_interval: DEFAULT_POLL_INTERVAL,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            next_poll: Instant::now() + DEFAULT_POLL_INTERVAL,
            check_due: false,
            connected: None,
            pending: VecDeque::new(),
        })
    }

    /// Returns [`Wakeup::Poll`] every `interval` (from the last `Poll` or `Connected`), however
    /// many notifications arrive in between.
    #[must_use]
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self.next_poll = Instant::now() + interval;
        self
    }

    /// Bounds a connection attempt (with `LISTEN`) and the health check each [`Wakeup::Poll`]
    /// runs. A health check that takes longer replaces the connection.
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// The channel it listens on (`{events}__evt_journal`).
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// The next wake-up. When it is not connected it connects and returns [`Wakeup::Connected`].
    /// A `Poll` is returned the moment it is due, and the call after it first checks the
    /// connection with `SELECT 1`; when the connection ends or fails that check (a half-open
    /// connection, after a failover) it reconnects within that call. The connection is driven only
    /// while this runs, so keep calling it. Dropping a call (say, in a `select!`) is safe: a
    /// notification it had not reported yet is covered by a later `Poll`, and an interrupted check
    /// runs again.
    ///
    /// # Errors
    ///
    /// Connecting or `LISTEN` failing (the server is down or refusing): a backend error. Retry
    /// after a backoff; the next call connects again.
    pub async fn next(&mut self) -> Result<Wakeup, Error> {
        loop {
            if let Some(wakeup) = self.pending.pop_front() {
                return Ok(wakeup);
            }

            let Some((client, connection)) = self.connected.as_mut() else {
                self.connect().await?;
                self.check_due = false;
                self.next_poll = Instant::now() + self.poll_interval;

                return Ok(Wakeup::Connected);
            };

            // The health check the last `Poll` asked for: a half-open connection (after a
            // failover) is replaced rather than polled forever.
            if self.check_due {
                let mut received = Vec::new();
                let alive = tokio::time::timeout(
                    self.connect_timeout,
                    drive(connection, client.batch_execute("SELECT 1"), &mut received),
                )
                .await
                .is_ok_and(|result| result.is_ok());
                self.check_due = false;

                for message in received {
                    self.receive(message);
                }

                if !alive {
                    self.connected = None;
                }

                continue;
            }

            let message =
                tokio::time::timeout_at(self.next_poll, poll_fn(|cx| connection.poll_message(cx)))
                    .await;

            match message {
                Ok(Some(Ok(message))) => self.receive(message),

                // The connection ended: reconnect and report it.
                Ok(Some(Err(_)) | None) => self.connected = None,

                // The backstop is due: report it at once, and check the connection on the next
                // call.
                Err(_) => {
                    self.next_poll = Instant::now() + self.poll_interval;
                    self.check_due = true;

                    return Ok(Wakeup::Poll);
                }
            }
        }
    }

    /// Opens the connection and runs `LISTEN` on it, within the connect timeout, keeping any
    /// notification that arrives meanwhile.
    async fn connect(&mut self) -> Result<(), Error> {
        let timed_out = || Error::backend("connecting to PostgreSQL timed out");
        let deadline = Instant::now() + self.connect_timeout;
        let (client, mut connection) =
            tokio::time::timeout_at(deadline, self.config.connect(self.tls.clone()))
                .await
                .map_err(|_| timed_out())?
                .map_err(Error::backend)?;
        let mut received = Vec::new();
        let sql = format!("LISTEN {}", self.channel);
        tokio::time::timeout_at(
            deadline,
            drive(&mut connection, client.batch_execute(&sql), &mut received),
        )
        .await
        .map_err(|_| timed_out())?
        .map_err(Error::backend)?;

        for message in received {
            self.receive(message);
        }

        self.connected = Some((client, connection));

        Ok(())
    }

    /// Queues the wake-up a message carries: a notification on the channel, with a valid scope.
    fn receive(&mut self, message: AsyncMessage) {
        if let AsyncMessage::Notification(notification) = message
            && notification.channel() == self.channel
            && let Ok(scope) = Scope::new(notification.payload())
        {
            self.pending.push_back(Wakeup::Scope(scope));
        }
    }
}

/// Runs `request` on `connection`'s client to completion while driving the connection, keeping
/// every message that arrives meanwhile in `received`.
async fn drive<S, T, F>(
    connection: &mut Connection<S, T>,
    request: F,
    received: &mut Vec<AsyncMessage>,
) -> Result<(), tokio_postgres::Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: Future<Output = Result<(), tokio_postgres::Error>>,
{
    let mut request = pin!(request);

    poll_fn(|cx| {
        if let Poll::Ready(result) = request.as_mut().poll(cx) {
            return Poll::Ready(result);
        }

        loop {
            match connection.poll_message(cx) {
                Poll::Ready(Some(Ok(message))) => received.push(message),
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(None) | Poll::Pending => return Poll::Pending,
            }
        }
    })
    .await
}

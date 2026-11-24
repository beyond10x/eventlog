//! PostgreSQL instruments that need no hook in the provider: a counting proxy for round trips,
//! parses and executes, the transaction id for write transactions, `EXPLAIN (ANALYZE, BUFFERS)`
//! for buffers, and the statistics views for tuples read.
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_postgres::{Client, Config, NoTls, config::Host, types::ToSql};

use crate::{Failure, Outcome};

/// A direct client, beside the store, for instruments and fixture checks.
pub async fn client(url: &str) -> Outcome<Client> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

/// The next transaction id. Only a transaction that writes is given one, so the difference
/// between two reads, less the second read's own, is the write transactions in between.
pub async fn next_xid(client: &Client) -> Outcome<u64> {
    let text: String = client
        .query_one("SELECT pg_current_xact_id()::text", &[])
        .await?
        .get(0);
    text.parse()
        .map_err(|_| Failure(format!("transaction id {text} is not a number")))
}

pub async fn count_rows(client: &Client, table: &str) -> Outcome<u64> {
    let count: i64 = client
        .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
        .await?
        .get(0);
    u64::try_from(count).map_err(|_| Failure("negative row count".into()))
}

/// Rows removed by filters in a plan node and every node under it.
fn filtered(node: &Value) -> u64 {
    let own = node
        .get("Rows Removed by Filter")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    own + node
        .get("Plans")
        .and_then(Value::as_array)
        .map_or(0, |plans| plans.iter().map(filtered).sum())
}

/// Buffers a statement touched (shared hits and reads) and rows its filters removed.
pub struct Plan {
    pub buffers: u64,
    pub rows_filtered: u64,
}

/// The JSON document of `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) <query>`.
pub async fn explain_document(
    client: &Client,
    query: &str,
    parameters: &[&(dyn ToSql + Sync)],
) -> Outcome<Value> {
    let row = client
        .query_one(
            &format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"),
            parameters,
        )
        .await?;
    Ok(match row.try_get::<_, Value>(0) {
        Ok(value) => value,
        Err(_) => serde_json::from_str(&row.try_get::<_, String>(0)?)?,
    })
}

pub async fn explain(
    client: &Client,
    query: &str,
    parameters: &[&(dyn ToSql + Sync)],
) -> Outcome<Plan> {
    let document = explain_document(client, query, parameters).await?;
    let plan = document
        .get(0)
        .and_then(|entry| entry.get("Plan"))
        .ok_or_else(|| Failure("EXPLAIN returned no plan".into()))?;
    Ok(Plan {
        buffers: blocks(plan),
        rows_filtered: filtered(plan),
    })
}

/// Shared blocks (hits and reads) a plan node and everything under it touched.
pub fn blocks(node: &Value) -> u64 {
    let number = |key: &str| node.get(key).and_then(Value::as_u64).unwrap_or(0);
    number("Shared Hit Blocks") + number("Shared Read Blocks")
}

/// The tuples read from `table` and its indexes since the statistics began: `seq_tup_read` plus
/// every index's `idx_tup_read`. A backend adds its pending counts to the shared statistics as it
/// exits, so the number is complete only once the backends that did the work are gone
/// ([`await_backends_gone`]). The reading runs on a session of its own that never touches `table`.
pub async fn tuples_read(url: &str, table: &str) -> Outcome<u64> {
    let reader = client(url).await?;
    let total: i64 = reader
        .query_one(
            "SELECT (COALESCE((SELECT SUM(seq_tup_read) FROM pg_stat_user_tables
                               WHERE relname = $1), 0)
                   + COALESCE((SELECT SUM(idx_tup_read) FROM pg_stat_user_indexes
                               WHERE relname = $1), 0))::bigint",
            &[&table],
        )
        .await?
        .get(0);
    u64::try_from(total).map_err(|_| Failure("negative tuple count".into()))
}

/// Wait until no backend with `application_name` is left (at most ten seconds), then give the
/// server a moment more: a backend's statistics are flushed as it exits.
pub async fn await_backends_gone(observer: &Client, application: &str) -> Outcome<()> {
    for _ in 0..200 {
        let left: i64 = observer
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
                &[&application],
            )
            .await?
            .get(0);
        if left == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Err(Failure(format!(
        "backends of application {application} did not exit within ten seconds"
    )))
}

/// `url` with `application_name` set, so the backends of one store can be told from the rest.
pub fn with_application(url: &str, application: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}application_name={application}")
}

/// A loopback TCP proxy in front of the server that counts what the client sends: round trips
/// (each `Sync` closes an extended-protocol exchange, each `Query` is a simple-protocol one),
/// `Parse` messages, and the `Execute` messages of each named statement.
pub struct Proxy {
    pub url: String,
    seen: Arc<Shared>,
}

/// What every connection through one proxy has sent since the last `take`.
#[derive(Default)]
struct Shared {
    round_trips: AtomicU64,
    parses: AtomicU64,
    connections: AtomicU64,
    /// `Execute` messages by connection and prepared statement name.
    executes: Mutex<HashMap<(u64, Vec<u8>), u64>>,
}

impl Proxy {
    pub async fn start(target: &str) -> Outcome<Self> {
        let config: Config = target
            .parse()
            .map_err(|_| Failure("unusable PostgreSQL URL".into()))?;
        let host = match config.get_hosts().first() {
            Some(Host::Tcp(host)) => host.clone(),
            _ => return Err(Failure("the KPI proxy needs a TCP PostgreSQL host".into())),
        };
        let port = config.get_ports().first().copied().unwrap_or(5432);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local = listener.local_addr()?;
        let seen = Arc::new(Shared::default());
        let shared = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let upstream = format!("{host}:{port}");
                let shared = Arc::clone(&shared);
                let connection = shared.connections.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    if let Ok(server) = TcpStream::connect(upstream).await {
                        let _ = relay(client, server, shared, connection).await;
                    }
                });
            }
        });
        let mut parts = vec![
            "host=127.0.0.1".to_owned(),
            format!("port={}", local.port()),
        ];
        if let Some(user) = config.get_user() {
            parts.push(format!("user={}", quote(user)));
        }
        if let Some(password) = config.get_password() {
            parts.push(format!(
                "password={}",
                quote(&String::from_utf8_lossy(password))
            ));
        }
        if let Some(database) = config.get_dbname() {
            parts.push(format!("dbname={}", quote(database)));
        }
        Ok(Self {
            url: parts.join(" "),
            seen,
        })
    }

    /// Round trips since the last call.
    pub fn take(&self) -> u64 {
        self.seen.round_trips.swap(0, Ordering::Relaxed)
    }

    /// `Parse` messages since the last call.
    pub fn take_parses(&self) -> u64 {
        self.seen.parses.swap(0, Ordering::Relaxed)
    }

    /// The most `Execute` messages any one prepared statement of one connection received since
    /// the last call, 0 when none was prepared by name. A statement run through the unnamed
    /// statement is parsed again each time, so it is not a repeated statement and is not counted.
    pub fn take_busiest_statement(&self) -> u64 {
        let mut executes = self
            .seen
            .executes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let busiest = executes.values().copied().max().unwrap_or(0);
        executes.clear();
        busiest
    }
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

async fn relay(
    client: TcpStream,
    server: TcpStream,
    shared: Arc<Shared>,
    connection: u64,
) -> std::io::Result<()> {
    let (mut client_read, mut client_write) = client.into_split();
    let (mut server_read, mut server_write) = server.into_split();
    let back =
        tokio::spawn(async move { tokio::io::copy(&mut server_read, &mut client_write).await });
    let mut framing = Framing::default();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = client_read.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let seen = framing.feed(&buffer[..read]);
        shared
            .round_trips
            .fetch_add(seen.exchanges, Ordering::Relaxed);
        shared.parses.fetch_add(seen.parses, Ordering::Relaxed);
        if !seen.executed.is_empty() {
            let mut executes = shared
                .executes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for statement in seen.executed {
                *executes.entry((connection, statement)).or_default() += 1;
            }
        }
        server_write.write_all(&buffer[..read]).await?;
    }
    let _ = server_write.shutdown().await;
    let _ = back.await;
    Ok(())
}

/// What one `feed` saw complete.
#[derive(Debug, Default, PartialEq, Eq)]
struct Seen {
    /// `Sync` and `Query` messages.
    exchanges: u64,
    /// `Parse` messages.
    parses: u64,
    /// The statement name of each `Execute` of a named prepared statement, in order.
    executed: Vec<Vec<u8>>,
}

/// How much of a `Bind` message is kept to read its two names (portal, then statement).
const BIND_NAMES: usize = 128;

/// The client-to-server message stream, cut into messages. The first message is the untyped
/// startup packet; every later one is a type byte and a big-endian length that counts itself.
#[derive(Default)]
struct Framing {
    started: bool,
    header: Vec<u8>,
    remaining: usize,
    /// The message being skipped is a `Bind`, whose first bytes are kept in `names`.
    binding: bool,
    names: Vec<u8>,
    /// The statement the last `Bind` named; the next `Execute` runs its portal.
    statement: Vec<u8>,
}

impl Framing {
    /// Consume `bytes` and return what completed in them.
    fn feed(&mut self, mut bytes: &[u8]) -> Seen {
        let mut seen = Seen::default();
        while !bytes.is_empty() {
            if self.remaining > 0 {
                let skip = self.remaining.min(bytes.len());
                if self.binding {
                    let room = BIND_NAMES.saturating_sub(self.names.len());
                    self.names.extend_from_slice(&bytes[..skip.min(room)]);
                }
                self.remaining -= skip;
                bytes = &bytes[skip..];
                if self.remaining == 0 {
                    self.end_of_message();
                }
                continue;
            }
            let header_length = if self.started { 5 } else { 4 };
            let need = header_length - self.header.len();
            let take = need.min(bytes.len());
            self.header.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.header.len() < header_length {
                break;
            }
            let length_bytes: [u8; 4] = self.header[header_length - 4..]
                .try_into()
                .unwrap_or([0; 4]);
            let length = usize::try_from(u32::from_be_bytes(length_bytes)).unwrap_or(0);
            if self.started {
                match self.header[0] {
                    b'S' | b'Q' => seen.exchanges += 1,
                    b'P' => seen.parses += 1,
                    b'E' if !self.statement.is_empty() => {
                        seen.executed.push(self.statement.clone());
                    }
                    _ => {}
                }
                self.binding = self.header[0] == b'B';
                self.names.clear();
            } else {
                self.started = true;
            }
            self.remaining = length.saturating_sub(4);
            self.header.clear();
            if self.remaining == 0 {
                self.end_of_message();
            }
        }
        seen
    }

    fn end_of_message(&mut self) {
        if self.binding {
            self.binding = false;
            let mut names = self.names.splitn(3, |byte| *byte == 0);
            names.next();
            self.statement = names.next().unwrap_or_default().to_vec();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Framing, Seen};

    fn message(kind: u8, body: &[u8]) -> Vec<u8> {
        let mut bytes = vec![kind];
        bytes.extend_from_slice(&(u32::try_from(body.len() + 4).unwrap()).to_be_bytes());
        bytes.extend_from_slice(body);
        bytes
    }

    fn stream() -> Vec<u8> {
        let mut stream = 12_u32.to_be_bytes().to_vec();
        stream.extend_from_slice(&[0, 3, 0, 0, b'u', 0, 0, 0]);
        for (kind, body) in [
            (b'P', &b"select 1"[..]),
            (b'B', b"\0\0\0\0\0\0\0\0"),
            (b'E', b"\0\0\0\0\0"),
            (b'S', b""),
            (b'Q', b"select 'S'"),
            (b'P', b"SSSS"),
            (b'S', b""),
            (b'P', b"s1\0select 2\0"),
            (b'B', b"\0s1\0\0\0\0\0\0\0"),
            (b'E', b"\0\0\0\0\0"),
            (b'B', b"\0s1\0\0\0\0\0\0\0"),
            (b'E', b"\0\0\0\0\0"),
            (b'B', b"portal\0s2\0\0\0\0\0\0\0"),
            (b'E', b"portal\0\0\0\0\0"),
            (b'B', b"\0s1\0\0\0\0\0\0\0"),
            (b'E', b"\0\0\0\0\0"),
            (b'S', b""),
        ] {
            stream.extend(message(kind, body));
        }
        stream
    }

    fn total(parts: impl IntoIterator<Item = Seen>) -> Seen {
        let mut sum = Seen::default();
        for part in parts {
            sum.exchanges += part.exchanges;
            sum.parses += part.parses;
            sum.executed.extend(part.executed);
        }
        sum
    }

    fn expected() -> Seen {
        Seen {
            exchanges: 4,
            parses: 3,
            executed: vec![
                b"s1".to_vec(),
                b"s1".to_vec(),
                b"s2".to_vec(),
                b"s1".to_vec(),
            ],
        }
    }

    #[test]
    fn framing_counts_syncs_queries_parses_and_named_executes_across_arbitrary_splits() {
        let stream = stream();
        for split in 1..stream.len() {
            let mut framing = Framing::default();
            let counted = total([
                framing.feed(&stream[..split]),
                framing.feed(&stream[split..]),
            ]);
            assert_eq!(
                counted,
                expected(),
                "three Syncs and a Query, three Parses, four named Executes (an unnamed \
                 statement's is not counted), split at {split}"
            );
        }
        let mut framing = Framing::default();
        let one_byte_at_a_time = total(stream.iter().map(|byte| framing.feed(&[*byte])));
        assert_eq!(
            one_byte_at_a_time,
            expected(),
            "message bodies are never read as types"
        );
    }

    #[test]
    fn a_statement_name_longer_than_the_kept_prefix_is_not_a_panic() {
        let mut stream = 8_u32.to_be_bytes().to_vec();
        stream.extend_from_slice(&[0, 3, 0, 0]);
        let mut body = vec![0];
        body.extend(std::iter::repeat_n(b'n', 300));
        body.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0]);
        stream.extend(message(b'B', &body));
        stream.extend(message(b'E', b"\0\0\0\0\0"));
        let mut framing = Framing::default();
        assert_eq!(framing.feed(&stream).executed.len(), 1);
    }
}

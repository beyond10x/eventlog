//! PostgreSQL instruments that need no hook in the provider: a counting proxy for round trips,
//! the transaction id for write transactions, and `EXPLAIN (ANALYZE, BUFFERS)` for buffers.
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
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

pub async fn explain(
    client: &Client,
    query: &str,
    parameters: &[&(dyn ToSql + Sync)],
) -> Outcome<Plan> {
    let row = client
        .query_one(
            &format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"),
            parameters,
        )
        .await?;
    let document: Value = match row.try_get::<_, Value>(0) {
        Ok(value) => value,
        Err(_) => serde_json::from_str(&row.try_get::<_, String>(0)?)?,
    };
    let plan = document
        .get(0)
        .and_then(|entry| entry.get("Plan"))
        .ok_or_else(|| Failure("EXPLAIN returned no plan".into()))?;
    let number = |node: &Value, key: &str| node.get(key).and_then(Value::as_u64).unwrap_or(0);
    Ok(Plan {
        buffers: number(plan, "Shared Hit Blocks") + number(plan, "Shared Read Blocks"),
        rows_filtered: filtered(plan),
    })
}

/// A loopback TCP proxy in front of the server that counts the client's round trips: each
/// `Sync` closes an extended-protocol exchange, each `Query` is a simple-protocol one.
pub struct Proxy {
    pub url: String,
    round_trips: Arc<AtomicU64>,
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
        let round_trips = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&round_trips);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let upstream = format!("{host}:{port}");
                let counter = Arc::clone(&counter);
                tokio::spawn(async move {
                    if let Ok(server) = TcpStream::connect(upstream).await {
                        let _ = relay(client, server, counter).await;
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
            round_trips,
        })
    }

    /// Round trips since the last call.
    pub fn take(&self) -> u64 {
        self.round_trips.swap(0, Ordering::Relaxed)
    }
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

async fn relay(
    client: TcpStream,
    server: TcpStream,
    counter: Arc<AtomicU64>,
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
        let exchanges = framing.feed(&buffer[..read]);
        counter.fetch_add(exchanges, Ordering::Relaxed);
        server_write.write_all(&buffer[..read]).await?;
    }
    let _ = server_write.shutdown().await;
    let _ = back.await;
    Ok(())
}

/// The client-to-server message stream, cut into messages. The first message is the untyped
/// startup packet; every later one is a type byte and a big-endian length that counts itself.
#[derive(Default)]
struct Framing {
    started: bool,
    header: Vec<u8>,
    remaining: usize,
}

impl Framing {
    /// Consume `bytes` and return how many round-trip-closing messages completed in them.
    fn feed(&mut self, mut bytes: &[u8]) -> u64 {
        let mut exchanges = 0;
        while !bytes.is_empty() {
            if self.remaining > 0 {
                let skip = self.remaining.min(bytes.len());
                self.remaining -= skip;
                bytes = &bytes[skip..];
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
                if matches!(self.header[0], b'S' | b'Q') {
                    exchanges += 1;
                }
            } else {
                self.started = true;
            }
            self.remaining = length.saturating_sub(4);
            self.header.clear();
        }
        exchanges
    }
}

#[cfg(test)]
mod tests {
    use super::Framing;

    fn message(kind: u8, body: &[u8]) -> Vec<u8> {
        let mut bytes = vec![kind];
        bytes.extend_from_slice(&(u32::try_from(body.len() + 4).unwrap()).to_be_bytes());
        bytes.extend_from_slice(body);
        bytes
    }

    #[test]
    fn framing_counts_syncs_and_queries_across_arbitrary_splits() {
        let mut stream = 12_u32.to_be_bytes().to_vec();
        stream.extend_from_slice(&[0, 3, 0, 0, b'u', 0, 0, 0]);
        for (kind, body) in [
            (b'P', &b"select 1"[..]),
            (b'B', b""),
            (b'E', b"\0\0\0\0\0"),
            (b'S', b""),
            (b'Q', b"select 'S'"),
            (b'P', b"SSSS"),
            (b'S', b""),
        ] {
            stream.extend(message(kind, body));
        }
        for split in 1..stream.len() {
            let mut framing = Framing::default();
            let counted = framing.feed(&stream[..split]) + framing.feed(&stream[split..]);
            assert_eq!(counted, 3, "two Syncs and one Query, split at {split}");
        }
        let mut framing = Framing::default();
        let one_byte_at_a_time: u64 = stream.iter().map(|byte| framing.feed(&[*byte])).sum();
        assert_eq!(
            one_byte_at_a_time, 3,
            "message bodies are never read as types"
        );
    }
}

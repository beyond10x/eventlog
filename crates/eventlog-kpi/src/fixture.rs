//! The stores and data every scenario starts from, one provider at a time, in a scratch directory.
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use eventlog_conformance::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicEventStore, BoxFuture, EventLogError, EventStore, Expected, NewEvent,
    NoGuard, ProjectionSpec, ProjectionStore, Projector, RecordedEvent, StreamAppend, StreamId,
    TenantId, UNAVAILABLE,
};
use eventlog_file::FileEventStore;
use eventlog_postgres::PostgresEventStore;
use eventlog_sqlite::SqliteEventStore;
use eventlog_tree::TreeEventStore;
use serde_json::json;

use crate::{Outcome, ensure};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Provider {
    Sqlite,
    File,
    Tree,
    Postgres,
}

impl Provider {
    pub const ALL: [Self; 4] = [Self::Sqlite, Self::File, Self::Tree, Self::Postgres];

    pub fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::File => "file",
            Self::Tree => "tree",
            Self::Postgres => "postgres",
        }
    }
}

/// An open store of any provider.
#[allow(
    clippy::large_enum_variant,
    reason = "a few stores per run, opened one after another"
)]
pub enum Store {
    Sqlite(SqliteEventStore),
    File(FileEventStore),
    Tree(TreeEventStore),
    Postgres(PostgresEventStore),
}

impl Store {
    pub fn atomic(&self) -> &dyn AtomicEventStore {
        match self {
            Self::Sqlite(store) => store,
            Self::File(store) => store,
            Self::Tree(store) => store,
            Self::Postgres(store) => store,
        }
    }

    pub fn events(&self) -> &dyn EventStore {
        self.atomic()
    }

    /// Release the store: PostgreSQL drains its pool, the others let go of their files.
    pub async fn close(self) {
        if let Self::Postgres(store) = self {
            let _ = store.shutdown().await;
        }
    }

    /// Release the store and, on PostgreSQL, drop the owner's tables.
    pub async fn discard(self) {
        if let Self::Postgres(store) = &self {
            let _ = store.drop_tables().await;
        }
        self.close().await;
    }
}

static PLACES: AtomicU64 = AtomicU64::new(0);

/// Where one provider's store lives, so a scenario can open it again.
pub struct Place {
    pub provider: Provider,
    pub root: PathBuf,
    pub prefix: String,
    url: Option<String>,
}

/// A PostgreSQL table prefix no other run uses: lowercase letters and underscores only.
fn unique_prefix(serial: u64) -> String {
    let stamp = time::OffsetDateTime::now_utc().unix_timestamp_nanos();
    let letters = |value: u128| -> String {
        value
            .to_string()
            .bytes()
            .map(|digit| char::from(b'a' + digit - b'0'))
            .collect()
    };
    let stamp = letters(u128::try_from(stamp).unwrap_or(0) % 1_000_000_000_000);
    format!("kpi_{stamp}_{}", letters(u128::from(serial)))
}

impl Place {
    pub fn new(provider: Provider, scratch: &Path, url: Option<&str>) -> Outcome<Self> {
        let serial = PLACES.fetch_add(1, Ordering::Relaxed);
        let root = scratch.join(format!("{}-{serial}", provider.name()));
        std::fs::create_dir_all(&root)?;
        ensure(
            provider != Provider::Postgres || url.is_some(),
            "a PostgreSQL place needs EVENTLOG_TEST_POSTGRES_URL",
        )?;
        Ok(Self {
            provider,
            root,
            prefix: unique_prefix(serial),
            url: url.map(str::to_owned),
        })
    }

    pub fn sqlite_path(&self) -> PathBuf {
        self.root.join("store.db")
    }

    pub fn file_root(&self) -> PathBuf {
        self.root.join("file")
    }

    pub async fn open(&self) -> Outcome<Store> {
        self.open_at(self.url.as_deref()).await
    }

    /// Open, with PostgreSQL reached at `url` (the counting proxy) rather than the server.
    pub async fn open_at(&self, url: Option<&str>) -> Outcome<Store> {
        Ok(match self.provider {
            Provider::Sqlite => Store::Sqlite(
                SqliteEventStore::open(&self.sqlite_path().to_string_lossy(), "kpi").await?,
            ),
            Provider::File => Store::File(FileEventStore::open(self.file_root()).await?),
            Provider::Tree => Store::Tree(TreeEventStore::open(self.root.join("tree")).await?),
            Provider::Postgres => {
                let url = url.ok_or_else(|| crate::Failure("no PostgreSQL URL".into()))?;
                Store::Postgres(PostgresEventStore::connect(url, &self.prefix).await?)
            }
        })
    }

    /// An `Arc` handle for the repository API, which holds the store as a trait object.
    pub async fn open_shared(&self) -> Outcome<Arc<dyn EventStore>> {
        Ok(match self.open().await? {
            Store::Sqlite(store) => Arc::new(store),
            Store::File(store) => Arc::new(store),
            Store::Tree(store) => Arc::new(store),
            Store::Postgres(store) => Arc::new(store),
        })
    }
}

/// Blob bindings by digest, as the group-with-blobs port takes them.
pub type Blobs = Vec<(String, Vec<u8>)>;

pub fn tenant() -> Outcome<TenantId> {
    Ok(TenantId::new("owner")?)
}

pub fn stream(tenant: &TenantId, kind: &str, id: &str) -> Outcome<StreamId> {
    Ok(StreamId::new(tenant.clone(), kind, id)?)
}

/// `count` events in one append, as the review's 128-event fixture was written.
pub async fn seed(
    store: &dyn EventStore,
    stream: &StreamId,
    count: usize,
    key: &str,
) -> Outcome<()> {
    let events: Vec<NewEvent> = (0..count)
        .map(|index| event("added", i64::try_from(index).unwrap_or(i64::MAX)))
        .collect();
    let result = store
        .append(
            stream,
            Expected::Any,
            &events,
            &meta(key, &json!({ "key": key, "count": count })),
        )
        .await?;
    ensure(
        result.events.len() == count,
        "the seed append wrote every event",
    )
}

/// One event in one append, to a fresh stream: the single-event write.
pub async fn append_one(store: &dyn EventStore, stream: &StreamId, key: &str) -> Outcome<()> {
    store
        .append(
            stream,
            Expected::NoStream,
            &[event("added", 1)],
            &meta(key, &json!({ "key": key })),
        )
        .await?;
    Ok(())
}

/// A group of `streams` single-event appends and `blobs` blobs of `blob_bytes` each.
pub fn blob_group(
    tenant: &TenantId,
    key: &str,
    streams: usize,
    blobs: usize,
    blob_bytes: usize,
) -> Outcome<(AppendGroup, Blobs)> {
    let appends = (0..streams)
        .map(|index| {
            Ok(StreamAppend {
                stream: stream(tenant, "grouped", &format!("{key}-{index}"))?,
                expected: Expected::NoStream,
                events: vec![event("grouped", 1)],
            })
        })
        .collect::<Outcome<Vec<_>>>()?;
    let group = AppendGroup {
        tenant: tenant.clone(),
        meta: meta(key, &json!({ "group": key })),
        appends,
    };
    let content = (0..blobs)
        .map(|index| {
            let byte = u8::try_from(index % 251).unwrap_or(0);
            (format!("{key}-blob-{index}"), vec![byte; blob_bytes])
        })
        .collect();
    Ok((group, content))
}

/// A guarded group with its blobs, as a consumer holding a trait object writes it: in one
/// barrier where the provider has one, otherwise each blob and then the group (review P1).
/// Returns whether the provider's single barrier took it.
pub async fn guarded_blob_group(
    store: &dyn AtomicEventStore,
    group: &AppendGroup,
    blobs: &[(String, Vec<u8>)],
) -> Outcome<bool> {
    match store
        .append_group_guarded_with_blobs(group, Arc::new(NoGuard), blobs)
        .await
    {
        Ok(_) => Ok(true),
        Err(EventLogError::Invalid(message)) if message == UNAVAILABLE => {
            for (digest, bytes) in blobs {
                store.put_blob(&group.tenant, digest, bytes).await?;
            }
            store.append_group_guarded(group, Arc::new(NoGuard)).await?;
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

/// A read model of numbered rows; the last ten of each seeded run have kind `item`.
pub struct Rows;

pub const ROWS: ProjectionSpec = ProjectionSpec {
    name: "kpi_rows",
    indexed: &["kind"],
};

pub const SEEDED_ROWS: u64 = 1_001;

impl Projector for Rows {
    fn name(&self) -> &'static str {
        "kpi_rows"
    }

    fn projections(&self) -> &'static [ProjectionSpec] {
        std::slice::from_ref(&ROWS)
    }

    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            if event.name != "rows.seeded" {
                return Ok(());
            }
            let number = |field: &str| event.data.get(field).and_then(serde_json::Value::as_u64);
            let (Some(start), Some(count), Some(total)) =
                (number("start"), number("count"), number("total"))
            else {
                return Err(EventLogError::Invalid("rows.seeded needs numbers".into()));
            };
            for index in start..start + count {
                let kind = if index + 10 >= total { "item" } else { "other" };
                store
                    .upsert(
                        &ROWS,
                        &event.tenant,
                        &format!("key-{index:05}"),
                        &json!({ "kind": kind, "n": index }),
                    )
                    .await?;
            }
            Ok(())
        })
    }
}

/// A store holding [`SEEDED_ROWS`] rows of [`ROWS`], projected inline from eight events.
pub async fn projected(place: &Place) -> Outcome<Store> {
    let store = place.open().await?;
    let projector: Arc<dyn Projector> = Arc::new(Rows);
    store
        .events()
        .create_projections(Arc::clone(&projector))
        .await?;
    store.events().register_inline(projector).await?;
    let tenant = tenant()?;
    let chunk = 128;
    let events = (0..SEEDED_ROWS.div_ceil(chunk))
        .map(|index| {
            let start = index * chunk;
            let count = chunk.min(SEEDED_ROWS - start);
            NewEvent::new(
                "rows.seeded",
                1,
                json!({ "start": start, "count": count, "total": SEEDED_ROWS }),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    store
        .events()
        .append(
            &stream(&tenant, "rows", "all")?,
            Expected::NoStream,
            &events,
            &meta("rows", &json!({ "rows": SEEDED_ROWS })),
        )
        .await?;
    Ok(store)
}

//! Strict cold history inspection; WAL-mode sources are refused without opening SQLite.
use eventlog_core::{
    BoxFuture, HistoryInspection, InspectHistory, InspectionError, InspectionLimits, TenantId,
};
use std::path::{Path, PathBuf};

/// Selects a source for inspection without provisioning or granting writer authority.
///
/// Linux rollback-mode sources only. Inspection retains at most 64 read-only
/// source descriptors for the process lifetime, including refused observations.
/// Concurrent inspections and descriptor exhaustion return `SourceBusy`.
/// Keeping descriptors alive avoids releasing another SQLite connection's POSIX locks.
#[derive(Clone, Debug)]
pub struct SqliteHistoryInspector {
    path: PathBuf,
    prefix: String,
}

impl SqliteHistoryInspector {
    /// Construction performs no I/O; each observation admits the source independently.
    pub fn new(path: impl AsRef<Path>, prefix: impl Into<String>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
            prefix: prefix.into(),
        }
    }
}

impl InspectHistory for SqliteHistoryInspector {
    fn inspect_history<'a>(
        &'a self,
        tenant: &'a TenantId,
        limits: InspectionLimits,
    ) -> BoxFuture<'a, Result<HistoryInspection, InspectionError>> {
        let path = self.path.clone();
        let prefix = self.prefix.clone();
        let tenant = tenant.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || inspect(&path, &prefix, tenant, limits))
                .await
                .map_err(|_| InspectionError::SourceBusy)?
        })
    }
}

#[cfg(not(target_os = "linux"))]
fn inspect(
    _: &Path,
    _: &str,
    _: TenantId,
    _: InspectionLimits,
) -> Result<HistoryInspection, InspectionError> {
    Err(InspectionError::UnsupportedSource)
}

#[cfg(target_os = "linux")]
use linux::inspect;

#[cfg(target_os = "linux")]
mod linux {
    use super::{HistoryInspection, InspectionError, InspectionLimits, Path, PathBuf, TenantId};
    use nix::{
        fcntl::{FcntlArg, fcntl},
        libc,
    };
    use rusqlite::{Connection, OpenFlags, OptionalExtension as _};
    use std::{
        collections::BTreeMap,
        fmt::Write as _,
        fs::{self, File, Metadata, OpenOptions},
        os::unix::{
            ffi::OsStrExt as _,
            fs::{FileExt as _, MetadataExt as _, OpenOptionsExt as _},
        },
        sync::{Mutex, MutexGuard},
        time::Duration,
    };

    const DESCRIPTOR_LIMIT: usize = 64;
    // No opened source FD leaves this process-lifetime registry, even on a
    // refused or raced admission: closing any FD for the inode would release
    // this process's *other* POSIX SQLite locks. Static values are not dropped.
    static DESCRIPTORS: Mutex<Vec<File>> = Mutex::new(Vec::new());

    /// Serializes this process's inspectors and holds one OFD lock, not a writer.
    struct Source {
        descriptors: MutexGuard<'static, Vec<File>>,
        index: usize,
        locked: bool,
        path: PathBuf,
        metadata: Metadata,
    }

    impl Source {
        fn open(path: &Path) -> Result<Self, InspectionError> {
            let metadata = fs::symlink_metadata(path).map_err(io_error)?;
            if !metadata.is_file() {
                return Err(InspectionError::UnsupportedSource);
            }
            let mut descriptors = DESCRIPTORS
                .try_lock()
                .map_err(|_| InspectionError::SourceBusy)?;
            let existing = descriptors.iter().position(|file| {
                file.metadata()
                    .is_ok_and(|held| (held.dev(), held.ino()) == (metadata.dev(), metadata.ino()))
            });
            let index = if let Some(index) = existing {
                index
            } else {
                if descriptors.len() >= DESCRIPTOR_LIMIT {
                    return Err(InspectionError::SourceBusy);
                }
                descriptors
                    .try_reserve(1)
                    .map_err(|_| InspectionError::SourceBusy)?;
                let file = OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                    .open(path)
                    .map_err(io_error)?;
                // Retain before any later operation can fail or discover a race.
                descriptors.push(file);
                descriptors.len() - 1
            };
            let mut source = Self {
                descriptors,
                index,
                locked: false,
                path: path.to_owned(),
                metadata,
            };
            native_lock(source.file(), libc::F_RDLCK)?;
            source.locked = true;
            let opened = source.file().metadata().map_err(io_error)?;
            if !opened.is_file()
                || (opened.dev(), opened.ino()) != (source.metadata.dev(), source.metadata.ino())
            {
                return Err(InspectionError::SourceChanged);
            }
            source.metadata = opened;
            source.verify()?;
            Ok(source)
        }

        fn file(&self) -> &File {
            &self.descriptors[self.index]
        }

        fn unlock(&mut self) -> Result<(), InspectionError> {
            if self.locked {
                native_lock(self.file(), libc::F_UNLCK)?;
                self.locked = false;
            }
            Ok(())
        }

        fn verify(&self) -> Result<(), InspectionError> {
            let metadata =
                fs::symlink_metadata(&self.path).map_err(|_| InspectionError::SourceChanged)?;
            if !metadata.is_file()
                || (
                    metadata.dev(),
                    metadata.ino(),
                    metadata.len(),
                    metadata.mtime(),
                    metadata.mtime_nsec(),
                ) != (
                    self.metadata.dev(),
                    self.metadata.ino(),
                    self.metadata.len(),
                    self.metadata.mtime(),
                    self.metadata.mtime_nsec(),
                )
            {
                return Err(InspectionError::SourceChanged);
            }
            Ok(())
        }

        fn admit(&self, limit: u64) -> Result<(), InspectionError> {
            for suffix in ["-wal", "-shm", "-journal"] {
                let mut name = self.path.as_os_str().to_owned();
                name.push(suffix);
                match fs::symlink_metadata(PathBuf::from(name)) {
                    Ok(_) => return Err(InspectionError::RecoveryRequired),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(io_error(error)),
                }
            }
            let mut header = [0_u8; 100];
            self.file()
                .read_exact_at(&mut header, 0)
                .map_err(|_| InspectionError::CorruptSource)?;
            if &header[..16] != b"SQLite format 3\0" {
                return Err(InspectionError::UnsupportedSource);
            }
            if header[18..20] != [1, 1] {
                return Err(InspectionError::RecoveryRequired);
            }
            if self.metadata.len() > limit {
                return Err(InspectionError::LimitExceeded);
            }
            Ok(())
        }
    }

    impl Drop for Source {
        fn drop(&mut self) {
            // An unlock failure may retain a read lock; closing the descriptor
            // would instead silently release an unrelated native writer lock.
            let _ = self.unlock();
        }
    }

    fn native_lock(file: &File, kind: libc::c_int) -> Result<(), InspectionError> {
        let lock = libc::flock {
            l_type: i16::try_from(kind).map_err(|_| InspectionError::UnsupportedSource)?,
            l_whence: i16::try_from(libc::SEEK_SET)
                .map_err(|_| InspectionError::UnsupportedSource)?,
            l_start: 0,
            l_len: 0,
            l_pid: 0,
        };
        fcntl(file, FcntlArg::F_OFD_SETLK(&lock))
            .map(|_| ())
            .map_err(|error| match error {
                nix::errno::Errno::EAGAIN | nix::errno::Errno::EACCES => {
                    InspectionError::SourceBusy
                }
                _ => InspectionError::UnsupportedSource,
            })
    }

    pub(super) fn inspect(
        path: &Path,
        prefix: &str,
        tenant: TenantId,
        limits: InspectionLimits,
    ) -> Result<HistoryInspection, InspectionError> {
        super::super::validate_prefix(prefix).map_err(|_| InspectionError::UnsupportedSource)?;
        let mut source = Source::open(path)?;
        source.admit(limits.source_bytes)?;
        let canonical = fs::canonicalize(path).map_err(io_error)?;
        // Percent-encode every URI-sensitive byte; caller text cannot select a VFS,
        // immutable mode, or another database. The OFD guard precedes SQLite opening.
        let mut uri = String::from("file:");
        for byte in canonical.as_os_str().as_bytes() {
            if byte.is_ascii_alphanumeric() || b"/-._~".contains(byte) {
                uri.push(char::from(*byte));
            } else {
                write!(uri, "%{byte:02X}").map_err(|_| InspectionError::UnsupportedSource)?;
            }
        }
        uri.push_str("?mode=ro&readonly_shm=1");
        source.verify()?;
        let connection = Connection::open_with_flags(
            uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .map_err(sql_error)?;
        source.verify()?;
        connection.busy_timeout(Duration::ZERO).map_err(sql_error)?;
        let result = (|| {
            connection
                .execute_batch("BEGIN DEFERRED")
                .map_err(sql_error)?;
            admit_schema(&connection, prefix)?;
            read_history(&connection, prefix, tenant, limits)
        })();
        drop(connection);
        source.verify()?;
        source.unlock()?;
        result
    }

    fn io_error(error: std::io::Error) -> InspectionError {
        let kind = error.kind();
        // Discard diagnostics at the boundary, including source paths.
        drop(error);
        if kind == std::io::ErrorKind::NotFound {
            InspectionError::MissingSource
        } else {
            InspectionError::SourceBusy
        }
    }

    fn sql_error(error: rusqlite::Error) -> InspectionError {
        let code = error.sqlite_error_code();
        // SQLite diagnostics may contain retained values; only the code crosses this API.
        drop(error);
        match code {
            Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked) => {
                InspectionError::SourceBusy
            }
            Some(rusqlite::ErrorCode::CannotOpen) => InspectionError::RecoveryRequired,
            _ => InspectionError::CorruptSource,
        }
    }

    fn compact(sql: &str) -> String {
        sql.chars()
            .filter(|c| !c.is_ascii_whitespace())
            .flat_map(char::to_lowercase)
            .collect()
    }

    // Frozen published 0.2.1 shape, not DDL to execute. No evolved blob schema is required.
    fn admit_schema(connection: &Connection, prefix: &str) -> Result<(), InspectionError> {
        let expected = [
            (
                format!("{prefix}_events"),
                format!(
                    "CREATE TABLE {prefix}_events (
                global_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                committed_xid INTEGER NOT NULL DEFAULT 0,
                tenant_id TEXT NOT NULL, stream_type TEXT NOT NULL, stream_id TEXT NOT NULL,
                version INTEGER NOT NULL, event_id TEXT NOT NULL, event_name TEXT NOT NULL,
                event_schema_version INTEGER NOT NULL, occurred_at TEXT NOT NULL,
                recorded_at TEXT NOT NULL, subject TEXT NOT NULL, actor TEXT NOT NULL,
                request_id TEXT NOT NULL, trace_id TEXT NOT NULL, causation_id TEXT,
                causation_depth INTEGER NOT NULL DEFAULT 0, redacted_at TEXT, data TEXT NOT NULL,
                UNIQUE (tenant_id, stream_type, stream_id, version))"
                ),
            ),
            (
                format!("{prefix}_identity"),
                format!(
                    "CREATE TABLE {prefix}_identity (
                tenant_id TEXT NOT NULL PRIMARY KEY, stream_identity TEXT NOT NULL)"
                ),
            ),
        ];
        for (name, expected_sql) in expected {
            let row: Option<(String, Option<String>)> = connection
                .query_row(
                    "SELECT type,sql FROM sqlite_master WHERE name=?1",
                    [&name],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(sql_error)?;
            let Some((kind, Some(sql))) = row else {
                return Err(InspectionError::MissingSource);
            };
            if kind != "table" || compact(&sql) != compact(&expected_sql) {
                return Err(InspectionError::UnsupportedSource);
            }
            // `rusqlite` 0.40 reads SQLite integers as `i64`; `u64` has no `FromSql`.
            let triggers: i64 = connection
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='trigger' AND tbl_name=?1",
                    [&name],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if triggers != 0 {
                return Err(InspectionError::UnsupportedSource);
            }
        }
        Ok(())
    }

    fn read_history(
        connection: &Connection,
        prefix: &str,
        tenant: TenantId,
        limits: InspectionLimits,
    ) -> Result<HistoryInspection, InspectionError> {
        let stream_identity: Option<String> = connection
            .query_row(
                &format!("SELECT stream_identity FROM {prefix}_identity WHERE tenant_id=?1"),
                [tenant.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        if stream_identity.as_ref().is_some_and(String::is_empty) {
            return Err(InspectionError::CorruptSource);
        }
        let mut statement = connection
            .prepare(&format!(
                "SELECT {} FROM {prefix}_events WHERE tenant_id=?1 ORDER BY global_seq",
                super::super::COLUMNS
            ))
            .map_err(sql_error)?;
        let mut rows = statement.query([tenant.as_str()]).map_err(sql_error)?;
        let mut events = Vec::new();
        let mut versions = BTreeMap::new();
        let mut ids = std::collections::BTreeSet::new();
        let mut envelope_bytes = 0_u64;
        let mut position = 0;
        while let Some(row) = rows.next().map_err(sql_error)? {
            if !matches!(
                row.get_ref(16).map_err(sql_error)?,
                rusqlite::types::ValueRef::Null
            ) {
                return Err(InspectionError::RedactedHistory);
            }
            if u64::try_from(events.len()).map_err(|_| InspectionError::LimitExceeded)?
                >= limits.events
            {
                return Err(InspectionError::LimitExceeded);
            }
            // Borrowed SQLite values expose lengths before read_event makes owned Strings.
            let mut row_bytes = 0_u64;
            for column in 0..18 {
                if let rusqlite::types::ValueRef::Text(bytes)
                | rusqlite::types::ValueRef::Blob(bytes) =
                    row.get_ref(column).map_err(sql_error)?
                {
                    row_bytes = row_bytes
                        .checked_add(
                            u64::try_from(bytes.len())
                                .map_err(|_| InspectionError::LimitExceeded)?,
                        )
                        .ok_or(InspectionError::LimitExceeded)?;
                }
            }
            if row_bytes > limits.source_bytes {
                return Err(InspectionError::LimitExceeded);
            }
            let event = super::super::read_event(row)
                .map_err(sql_error)?
                .map_err(|_| InspectionError::CorruptSource)?;
            event.validate_for_inspection()?;
            let stream = event.stream().map_err(|_| InspectionError::CorruptSource)?;
            let previous = versions.entry(stream).or_insert(0_u64);
            if event.global_seq <= position
                || event.version
                    != previous
                        .checked_add(1)
                        .ok_or(InspectionError::CorruptSource)?
                || !ids.insert(event.event_id.clone())
            {
                return Err(InspectionError::CorruptSource);
            }
            position = event.global_seq;
            *previous = event.version;
            let length = u64::try_from(
                serde_json::to_vec(&event)
                    .map_err(|_| InspectionError::CorruptSource)?
                    .len(),
            )
            .map_err(|_| InspectionError::LimitExceeded)?;
            envelope_bytes = envelope_bytes
                .checked_add(length)
                .ok_or(InspectionError::LimitExceeded)?;
            if envelope_bytes > limits.envelope_bytes {
                return Err(InspectionError::LimitExceeded);
            }
            events.push(event);
        }
        Ok(HistoryInspection {
            tenant,
            stream_identity,
            events,
        })
    }

    #[cfg(test)]
    mod tests {
        use super::{Connection, Duration, InspectionError, Source, fs};
        static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

        fn source_fixture(path: &std::path::Path) {
            Connection::open(path)
                .unwrap()
                .execute_batch("CREATE TABLE fixture(value INTEGER)")
                .unwrap();
        }

        fn writer_refused(path: &std::path::Path) {
            let writer = Connection::open(path).unwrap();
            writer.busy_timeout(Duration::ZERO).unwrap();
            for command in ["BEGIN IMMEDIATE", "PRAGMA journal_mode=WAL"] {
                let error = writer.execute_batch(command).unwrap_err();
                assert_eq!(
                    error.sqlite_error_code(),
                    Some(rusqlite::ErrorCode::DatabaseBusy)
                );
            }
        }

        #[test]
        fn inspection_ofd_blocks_native_writers_in_process() {
            let _test = TEST_LOCK.lock().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("fixture.sqlite3");
            source_fixture(&path);
            let before = fs::read(&path).unwrap();
            let source = Source::open(&path).unwrap();
            writer_refused(&path);
            assert_eq!(fs::read(&path).unwrap(), before);
            assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
            // A second read-only connection closing does not release the OFD lock.
            let reader =
                Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                    .unwrap();
            reader
                .query_row("SELECT count(*) FROM fixture", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap();
            drop(reader);
            writer_refused(&path);
            drop(source);
            Connection::open(&path)
                .unwrap()
                .execute_batch("BEGIN IMMEDIATE; ROLLBACK")
                .unwrap();
        }

        #[test]
        fn inspection_ofd_blocks_native_writer_process() {
            const CHILD: &str = "EVENTLOG_INSPECTION_OFD_CHILD";
            if let Some(path) = std::env::var_os(CHILD) {
                writer_refused(std::path::Path::new(&path));
                return;
            }
            let _test = TEST_LOCK.lock().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("fixture.sqlite3");
            source_fixture(&path);
            let before = fs::read(&path).unwrap();
            let source = Source::open(&path).unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "inspection::linux::tests::inspection_ofd_blocks_native_writer_process",
                    "--nocapture",
                ])
                .env(CHILD, &path)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "child output: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            assert_eq!(fs::read(&path).unwrap(), before);
            assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
            source.verify().unwrap();
        }

        #[test]
        fn inspection_ofd_detects_replaced_source() {
            let _test = TEST_LOCK.lock().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("fixture.sqlite3");
            source_fixture(&path);
            let source = Source::open(&path).unwrap();
            fs::rename(&path, directory.path().join("retained.sqlite3")).unwrap();
            source_fixture(&path);
            assert_eq!(source.verify(), Err(InspectionError::SourceChanged));
        }

        #[test]
        fn inspection_refusal_preserves_existing_process_writer_lock() {
            const CHILD: &str = "EVENTLOG_INSPECTION_EXISTING_WRITER_CHILD";
            if let Some(path) = std::env::var_os(CHILD) {
                writer_refused(std::path::Path::new(&path));
                return;
            }
            let _test = TEST_LOCK.lock().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("fixture.sqlite3");
            source_fixture(&path);
            let writer = Connection::open(&path).unwrap();
            writer.execute_batch("BEGIN IMMEDIATE").unwrap();
            let child = || {
                std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "inspection::linux::tests::inspection_refusal_preserves_existing_process_writer_lock", "--nocapture"])
                    .env(CHILD, &path).output().unwrap()
            };
            assert!(
                child().status.success(),
                "control: native writer lock must already hold"
            );
            assert!(matches!(
                Source::open(&path),
                Err(InspectionError::SourceBusy)
            ));
            let after = child();
            assert!(
                after.status.success(),
                "inspector refusal released another connection's lock: {} {}",
                String::from_utf8_lossy(&after.stdout),
                String::from_utf8_lossy(&after.stderr)
            );
            writer.execute_batch("ROLLBACK").unwrap();
        }

        #[test]
        fn inspection_concurrent_call_cannot_unlock_another_observation() {
            let _test = TEST_LOCK.lock().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("fixture.sqlite3");
            source_fixture(&path);
            let source = Source::open(&path).unwrap();
            let same = path.clone();
            assert!(
                std::thread::spawn(move || matches!(
                    Source::open(&same),
                    Err(InspectionError::SourceBusy)
                ))
                .join()
                .unwrap()
            );
            writer_refused(&path);
            drop(source);
            Source::open(&path).unwrap().verify().unwrap();
        }

        #[test]
        fn inspection_descriptor_exhaustion_never_opens_or_closes_source() {
            const CHILD: &str = "EVENTLOG_INSPECTION_EXHAUSTION_CHILD";
            if std::env::var_os(CHILD).is_none() {
                let output = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "inspection::linux::tests::inspection_descriptor_exhaustion_never_opens_or_closes_source", "--nocapture"])
                    .env(CHILD, "1").output().unwrap();
                assert!(
                    output.status.success(),
                    "{} {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
                return;
            }
            let directory = tempfile::tempdir().unwrap();
            for index in 0..super::DESCRIPTOR_LIMIT {
                let path = directory.path().join(format!("source-{index}.sqlite3"));
                source_fixture(&path);
                Source::open(&path).unwrap();
            }
            assert_eq!(
                super::DESCRIPTORS.lock().unwrap().len(),
                super::DESCRIPTOR_LIMIT
            );
            let first = directory.path().join("source-0.sqlite3");
            let alias = directory.path().join("alias.sqlite3");
            fs::hard_link(&first, &alias).unwrap();
            Source::open(&alias).unwrap();
            assert_eq!(
                super::DESCRIPTORS.lock().unwrap().len(),
                super::DESCRIPTOR_LIMIT
            );
            let absent_slot = directory.path().join("source-extra.sqlite3");
            source_fixture(&absent_slot);
            let before = fs::read(&absent_slot).unwrap();
            let descriptors_before = fs::read_dir("/proc/self/fd").unwrap().count();
            assert!(matches!(
                Source::open(&absent_slot),
                Err(InspectionError::SourceBusy)
            ));
            assert_eq!(
                fs::read_dir("/proc/self/fd").unwrap().count(),
                descriptors_before
            );
            assert_eq!(
                super::DESCRIPTORS.lock().unwrap().len(),
                super::DESCRIPTOR_LIMIT
            );
            assert_eq!(fs::read(&absent_slot).unwrap(), before);
            assert_eq!(
                fs::read_dir(directory.path()).unwrap().count(),
                super::DESCRIPTOR_LIMIT + 2
            );
        }

        #[test]
        fn inspection_refusal_preserves_existing_process_reader_lock() {
            const CHILD: &str = "EVENTLOG_INSPECTION_EXISTING_READER_CHILD";
            if let Some(path) = std::env::var_os(CHILD) {
                let writer = Connection::open(path).unwrap();
                writer.busy_timeout(Duration::ZERO).unwrap();
                assert_eq!(
                    writer
                        .execute_batch("BEGIN EXCLUSIVE")
                        .unwrap_err()
                        .sqlite_error_code(),
                    Some(rusqlite::ErrorCode::DatabaseBusy)
                );
                return;
            }
            let _test = TEST_LOCK.lock().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("fixture.sqlite3");
            source_fixture(&path);
            let reader = Connection::open(&path).unwrap();
            reader
                .execute_batch("BEGIN; SELECT * FROM fixture")
                .unwrap();
            let child = || {
                std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "inspection::linux::tests::inspection_refusal_preserves_existing_process_reader_lock", "--nocapture"])
                .env(CHILD, &path).output().unwrap()
            };
            assert!(child().status.success());
            // This reaches SQLite's own connection/open/drop path, then refuses the
            // foreign schema. VFS descriptor accounting must retain the prior reader.
            assert_eq!(
                super::inspect(
                    &path,
                    "inspection",
                    eventlog_core::TenantId::new("inspection-tenant").unwrap(),
                    eventlog_conformance::INSPECTION_LIMITS
                ),
                Err(InspectionError::MissingSource)
            );
            let after = child();
            assert!(
                after.status.success(),
                "{} {}",
                String::from_utf8_lossy(&after.stdout),
                String::from_utf8_lossy(&after.stderr)
            );
            reader.execute_batch("ROLLBACK").unwrap();
        }
    }
}

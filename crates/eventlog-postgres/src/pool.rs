//! Bounded leases. A cancelled transaction never returns its connection to the idle list.
use eventlog_core::EventLogError;
use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{
    Client, Config, NoTls,
    config::{Host, SslMode},
};
use tokio_postgres_rustls::MakeRustlsConnect;

/// Limits are per process. A host must also admit the sum across its replicas.
#[derive(Clone, Debug)]
pub struct PoolOptions {
    pub max_connections: usize,
    pub max_waiters: usize,
    pub acquisition_timeout: Duration,
    pub connect_timeout: Duration,
    pub statement_timeout: Duration,
    pub lock_timeout: Duration,
    pub transaction_timeout: Duration,
    pub shutdown_timeout: Duration,
}
impl Default for PoolOptions {
    fn default() -> Self {
        Self {
            max_connections: 4,
            max_waiters: 32,
            acquisition_timeout: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(2),
            statement_timeout: Duration::from_secs(5),
            lock_timeout: Duration::from_secs(2),
            transaction_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}
impl PoolOptions {
    pub(crate) fn validate(&self) -> Result<(), EventLogError> {
        if self.max_connections == 0
            || self.max_connections > 1024
            || self.max_waiters > 65_536
            || [
                self.acquisition_timeout,
                self.connect_timeout,
                self.statement_timeout,
                self.lock_timeout,
                self.transaction_timeout,
                self.shutdown_timeout,
            ]
            .iter()
            .any(|value| value.is_zero() || value.as_millis() > 86_400_000)
        {
            return Err(EventLogError::Invalid(
                "invalid bounded PostgreSQL pool configuration".into(),
            ));
        }
        Ok(())
    }
}

/// Host-owned connection settings. Debug output deliberately excludes connection material.
#[derive(Clone)]
pub struct PostgresConfig {
    pub(crate) connection: Config,
    pub(crate) tls: Option<rustls::ClientConfig>,
    pub(crate) schema: String,
    pub(crate) prefix: String,
    pub(crate) production: bool,
}
impl PostgresConfig {
    /// Admit plaintext only for an explicitly isolated loopback or Unix-socket test database.
    /// # Errors
    /// Refuses remote hosts, malformed connection settings and invalid owner names.
    pub fn isolated(url: &str, prefix: &str) -> Result<Self, EventLogError> {
        let mut connection = url.parse::<Config>().map_err(|_| {
            EventLogError::Invalid("invalid PostgreSQL connection configuration".into())
        })?;
        if connection
            .get_hostaddrs()
            .iter()
            .any(|address| !address.is_loopback())
            || connection.get_hosts().is_empty()
            || connection.get_hosts().iter().any(|host| match host {
                Host::Tcp(name) => {
                    name != "localhost"
                        && name
                            .parse::<std::net::IpAddr>()
                            .map_or(true, |ip| !ip.is_loopback())
                }
                #[cfg(unix)]
                Host::Unix(_) => false,
            })
        {
            return Err(EventLogError::Invalid(
                "plaintext PostgreSQL requires an explicit isolated local host".into(),
            ));
        }
        super::validate_prefix(prefix)?;
        connection.ssl_mode(SslMode::Disable);
        Ok(Self {
            connection,
            tls: None,
            schema: "public".into(),
            prefix: prefix.into(),
            production: false,
        })
    }
    /// Configure server-name and certificate verification for a hosted owner schema.
    /// # Errors
    /// Refuses empty trust roots or invalid configuration; no insecure verifier is accepted.
    pub fn verified(
        url: &str,
        schema: &str,
        prefix: &str,
        roots: rustls::RootCertStore,
    ) -> Result<Self, EventLogError> {
        super::validate_prefix(prefix)?;
        eventlog_core::validate_identifier("owner schema", schema)?;
        if roots.is_empty() {
            return Err(EventLogError::Invalid(
                "hosted PostgreSQL requires trusted CA certificates".into(),
            ));
        }
        let mut connection = url.parse::<Config>().map_err(|_| {
            EventLogError::Invalid("invalid PostgreSQL connection configuration".into())
        })?;
        connection.ssl_mode(SslMode::Require);
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Self {
            connection,
            tls: Some(tls),
            schema: schema.into(),
            prefix: prefix.into(),
            production: true,
        })
    }
    /// Select an existing test-owned schema without changing the transport admission.
    /// # Errors
    /// Refuses an invalid SQL identifier.
    pub fn with_schema(mut self, schema: &str) -> Result<Self, EventLogError> {
        eventlog_core::validate_identifier("owner schema", schema)?;
        self.schema = schema.into();
        Ok(self)
    }
}

/// Observed resource counts; secrets and database error detail never enter these diagnostics.
#[derive(Clone, Debug, serde::Serialize)]
pub struct PoolStatus {
    pub max_connections: usize,
    pub max_waiters: usize,
    pub checked_out: usize,
    pub waiting: usize,
    pub idle: usize,
    pub closed: bool,
}

pub(crate) struct Pool {
    config: PostgresConfig,
    pub(crate) options: PoolOptions,
    connections: Arc<Semaphore>,
    admission: Arc<Semaphore>,
    idle: Mutex<Vec<Connection>>,
    active: AtomicUsize,
    closed: AtomicBool,
    returned: Notify,
}
impl Pool {
    pub(crate) fn new(
        config: PostgresConfig,
        options: PoolOptions,
    ) -> Result<Arc<Self>, EventLogError> {
        options.validate()?;
        Ok(Arc::new(Self {
            connections: Arc::new(Semaphore::new(options.max_connections)),
            admission: Arc::new(Semaphore::new(
                options.max_connections + options.max_waiters,
            )),
            config,
            options,
            idle: Mutex::new(Vec::new()),
            active: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            returned: Notify::new(),
        }))
    }
    pub(crate) async fn acquire(self: &Arc<Self>) -> Result<Lease, EventLogError> {
        let admission = Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| {
                if self.closed.load(Ordering::Acquire) {
                    EventLogError::Closed
                } else {
                    EventLogError::Overloaded
                }
            })?;
        let connection = tokio::time::timeout(
            self.options.acquisition_timeout,
            Arc::clone(&self.connections).acquire_owned(),
        )
        .await
        .map_err(|_| EventLogError::Deadline {
            operation: "pool acquisition",
        })?
        .map_err(|_| EventLogError::Closed)?;
        self.active.fetch_add(1, Ordering::AcqRel);
        // Construct the lease before awaiting connect so cancellation balances the active count.
        let mut lease = Lease {
            pool: Arc::clone(self),
            client: None,
            reusable: false,
            connection_permit: Some(connection),
            admission_permit: Some(admission),
        };
        loop {
            let idle = self.idle.lock().map_err(super::poisoned)?.pop();
            match idle {
                Some(client) if !client.client.is_closed() => {
                    lease.client = Some(client);
                    break;
                }
                Some(_) => {}
                None => {
                    lease.client = Some(self.connect().await?);
                    let connection = lease.client.as_ref().expect("connected lease");
                    tokio::time::timeout(self.options.connect_timeout,connection.client.batch_execute(&format!("SET search_path TO {}; SET default_transaction_isolation = 'read committed'; SET default_transaction_read_only = off; SET statement_timeout = {}; SET lock_timeout = {}; SET idle_in_transaction_session_timeout = {}",self.config.schema,self.options.statement_timeout.as_millis(),self.options.lock_timeout.as_millis(),self.options.transaction_timeout.as_millis()))).await.map_err(|_|EventLogError::Deadline {operation:"session configuration"})?.map_err(super::backend)?;
                    break;
                }
            }
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(EventLogError::Closed);
        }
        Ok(lease)
    }
    async fn connect(&self) -> Result<Connection, EventLogError> {
        let work = async {
            let mut config = self.config.connection.clone();
            config.connect_timeout(self.options.connect_timeout);
            let client = if let Some(tls) = &self.config.tls {
                let (client, connection) = config
                    .connect(MakeRustlsConnect::new(tls.clone()))
                    .await
                    .map_err(super::backend)?;
                let driver = tokio::spawn(async move {
                    let _ = connection.await;
                });
                Connection {
                    client,
                    driver: Some(driver),
                }
            } else {
                let (client, connection) = config.connect(NoTls).await.map_err(super::backend)?;
                let driver = tokio::spawn(async move {
                    let _ = connection.await;
                });
                Connection {
                    client,
                    driver: Some(driver),
                }
            };
            Ok(client)
        };
        tokio::time::timeout(self.options.connect_timeout, work)
            .await
            .map_err(|_| EventLogError::Deadline {
                operation: "connection",
            })?
    }
    pub(crate) fn status(&self) -> PoolStatus {
        let checked_out = self.active.load(Ordering::Acquire);
        PoolStatus {
            max_connections: self.options.max_connections,
            max_waiters: self.options.max_waiters,
            checked_out,
            waiting: (self.options.max_connections + self.options.max_waiters
                - self.admission.available_permits())
            .saturating_sub(checked_out),
            idle: self.idle.lock().map_or(0, |idle| idle.len()),
            closed: self.closed.load(Ordering::Acquire),
        }
    }
    pub(crate) async fn shutdown(&self) -> Result<(), EventLogError> {
        self.closed.store(true, Ordering::Release);
        self.connections.close();
        self.admission.close();
        let idle = std::mem::take(&mut *self.idle.lock().map_err(super::poisoned)?);
        for connection in idle {
            connection.close().await;
        }
        tokio::time::timeout(self.options.shutdown_timeout, async {
            loop {
                let notified = self.returned.notified();
                if self.active.load(Ordering::Acquire) == 0 {
                    break;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| EventLogError::Deadline {
            operation: "shutdown",
        })
    }
}

pub(crate) struct Lease {
    pool: Arc<Pool>,
    client: Option<Connection>,
    reusable: bool,
    connection_permit: Option<OwnedSemaphorePermit>,
    admission_permit: Option<OwnedSemaphorePermit>,
}
impl Lease {
    pub(crate) fn quarantine(&mut self) {
        self.reusable = false;
    }
    pub(crate) fn settled(&mut self) {
        self.reusable = true;
    }
}
impl Deref for Lease {
    type Target = Client;
    fn deref(&self) -> &Client {
        &self.client.as_ref().expect("connected lease").client
    }
}
impl DerefMut for Lease {
    fn deref_mut(&mut self) -> &mut Client {
        &mut self.client.as_mut().expect("connected lease").client
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if self.reusable
            && !self.pool.closed.load(Ordering::Acquire)
            && let Some(connection) = self.client.take()
        {
            if !connection.client.is_closed()
                && let Ok(mut idle) = self.pool.idle.lock()
            {
                idle.push(connection);
                self.pool.active.fetch_sub(1, Ordering::AcqRel);
                self.pool.returned.notify_waiters();
                return;
            }
            self.client = Some(connection);
        }
        // Closing is part of the outstanding lease. Neither permit returns on abort() alone.
        let connection = self.client.take();
        let permit = self.connection_permit.take();
        let admission = self.admission_permit.take();
        let pool = Arc::clone(&self.pool);
        tokio::spawn(async move {
            if let Some(connection) = connection {
                connection.close().await;
            }
            drop(permit);
            drop(admission);
            pool.active.fetch_sub(1, Ordering::AcqRel);
            pool.returned.notify_waiters();
        });
    }
}

struct Connection {
    client: Client,
    driver: Option<tokio::task::JoinHandle<()>>,
}
impl Connection {
    async fn close(mut self) {
        if let Some(driver) = self.driver.take() {
            driver.abort();
            let _ = driver.await;
        }
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(driver) = &self.driver {
            driver.abort();
        }
    }
}

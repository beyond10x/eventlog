//! Bounded leases. A cancelled transaction never returns its connection to the idle list.
use eventlog_core::EventLogError;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};
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

#[cfg(test)]
#[derive(Clone)]
struct RecyclePause {
    reached: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum PausePoint {
    Checkout,
    Recycled,
    Retired,
}

pub(crate) struct Pool {
    config: PostgresConfig,
    pub(crate) options: PoolOptions,
    connections: Arc<Semaphore>,
    waiters: Arc<Semaphore>,
    state: Mutex<PoolState>,
    returned: Notify,
    #[cfg(test)]
    recycle_pause: Mutex<Option<RecyclePause>>,
    #[cfg(test)]
    transition_pause: Mutex<Option<(PausePoint, RecyclePause)>>,
    #[cfg(test)]
    driver_pause: Mutex<Option<RecyclePause>>,
    #[cfg(test)]
    connect_attempts: AtomicUsize,
}

#[derive(Default)]
struct PoolState {
    // Includes connection establishment and quarantine until its driver has stopped.
    active: usize,
    waiting: usize,
    idle: Vec<Connection>,
    closed: bool,
}

// Own queued membership even when timeout/cancellation drops an unpolled semaphore grant.
struct Waiting {
    pool: Arc<Pool>,
    permit: Option<OwnedSemaphorePermit>,
}
impl Waiting {
    fn finish(&mut self, state: &mut PoolState) {
        if let Some(permit) = self.permit.take() {
            state.waiting -= 1;
            drop(permit);
        }
    }
}
impl Drop for Waiting {
    fn drop(&mut self) {
        if self.permit.is_some() {
            let pool = Arc::clone(&self.pool);
            let mut state = pool
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.finish(&mut state);
        }
    }
}

impl Pool {
    pub(crate) fn new(
        config: PostgresConfig,
        options: PoolOptions,
    ) -> Result<Arc<Self>, EventLogError> {
        options.validate()?;
        Ok(Arc::new(Self {
            connections: Arc::new(Semaphore::new(options.max_connections)),
            waiters: Arc::new(Semaphore::new(options.max_waiters)),
            config,
            options,
            state: Mutex::new(PoolState::default()),
            returned: Notify::new(),
            #[cfg(test)]
            recycle_pause: Mutex::new(None),
            #[cfg(test)]
            transition_pause: Mutex::new(None),
            #[cfg(test)]
            driver_pause: Mutex::new(None),
            #[cfg(test)]
            connect_attempts: AtomicUsize::new(0),
        }))
    }
    pub(crate) async fn acquire(self: &Arc<Self>) -> Result<Lease, EventLogError> {
        let mut waiting = None;
        let immediate = {
            let mut state = self.state.lock().map_err(super::poisoned)?;
            if state.closed {
                return Err(EventLogError::Closed);
            }
            match Arc::clone(&self.connections).try_acquire_owned() {
                Ok(permit) => Some(self.checkout(&mut state, permit)),
                Err(TryAcquireError::Closed) => return Err(EventLogError::Closed),
                Err(TryAcquireError::NoPermits) => {
                    let permit = Arc::clone(&self.waiters)
                        .try_acquire_owned()
                        .map_err(|_| EventLogError::Overloaded)?;
                    state.waiting += 1;
                    waiting = Some(Waiting {
                        pool: Arc::clone(self),
                        permit: Some(permit),
                    });
                    None
                }
            }
        };
        let mut lease = if let Some(lease) = immediate {
            lease
        } else {
            let permit = tokio::time::timeout(
                self.options.acquisition_timeout,
                Arc::clone(&self.connections).acquire_owned(),
            )
            .await
            .map_err(|_| EventLogError::Deadline {
                operation: "pool acquisition",
            })?
            .map_err(|_| EventLogError::Closed)?;
            let mut state = self.state.lock().map_err(super::poisoned)?;
            waiting
                .as_mut()
                .expect("queued admission")
                .finish(&mut state);
            if state.closed {
                return Err(EventLogError::Closed);
            }
            self.checkout(&mut state, permit)
        };
        #[cfg(test)]
        self.pause(PausePoint::Checkout);
        loop {
            if let Some(connection) = &mut lease.client {
                if !connection.client.is_closed() {
                    break;
                }
                // Keep the driver inside the lease while awaiting it. Cancelling this await
                // transfers that same connection and permit to quarantine, not to a replacement.
                connection.close().await;
                lease.client = self.state.lock().map_err(super::poisoned)?.idle.pop();
            } else {
                if self.state.lock().map_err(super::poisoned)?.closed {
                    return Err(EventLogError::Closed);
                }
                lease.client = Some(self.connect().await?);
                let connection = lease.client.as_ref().expect("connected lease");
                tokio::time::timeout(self.options.connect_timeout,connection.client.batch_execute(&format!("SET search_path TO {}; SET default_transaction_isolation = 'read committed'; SET default_transaction_read_only = off; SET statement_timeout = {}; SET lock_timeout = {}; SET idle_in_transaction_session_timeout = {}",self.config.schema,self.options.statement_timeout.as_millis(),self.options.lock_timeout.as_millis(),self.options.transaction_timeout.as_millis()))).await.map_err(|_|EventLogError::Deadline {operation:"session configuration"})?.map_err(super::backend)?;
                break;
            }
        }
        if self.state.lock().map_err(super::poisoned)?.closed {
            return Err(EventLogError::Closed);
        }
        Ok(lease)
    }
    fn checkout(self: &Arc<Self>, state: &mut PoolState, permit: OwnedSemaphorePermit) -> Lease {
        state.active += 1;
        Lease {
            pool: Arc::clone(self),
            client: state.idle.pop(),
            reusable: false,
            connection_permit: Some(permit),
        }
    }
    async fn connect(&self) -> Result<Connection, EventLogError> {
        #[cfg(test)]
        self.connect_attempts.fetch_add(1, Ordering::AcqRel);
        let work = async {
            let mut config = self.config.connection.clone();
            config.connect_timeout(self.options.connect_timeout);
            let client = if let Some(tls) = &self.config.tls {
                let (client, connection) = config
                    .connect(MakeRustlsConnect::new(tls.clone()))
                    .await
                    .map_err(super::backend)?;
                #[cfg(test)]
                let pause = self.driver_pause.lock().unwrap().take();
                let driver = tokio::spawn(async move {
                    #[cfg(not(test))]
                    let _ = connection.await;
                    #[cfg(test)]
                    {
                        let mut connection = connection;
                        let _ = (&mut connection).await;
                        if let Some(pause) = pause {
                            tokio::task::block_in_place(|| {
                                pause.reached.wait();
                                pause.resume.wait();
                            });
                        }
                    }
                });
                Connection {
                    client,
                    driver: Some(driver),
                }
            } else {
                let (client, connection) = config.connect(NoTls).await.map_err(super::backend)?;
                #[cfg(test)]
                let pause = self.driver_pause.lock().unwrap().take();
                let driver = tokio::spawn(async move {
                    #[cfg(not(test))]
                    let _ = connection.await;
                    #[cfg(test)]
                    {
                        let mut connection = connection;
                        let _ = (&mut connection).await;
                        if let Some(pause) = pause {
                            tokio::task::block_in_place(|| {
                                pause.reached.wait();
                                pause.resume.wait();
                            });
                        }
                    }
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
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        PoolStatus {
            max_connections: self.options.max_connections,
            max_waiters: self.options.max_waiters,
            checked_out: state.active,
            waiting: state.waiting,
            idle: state.idle.len(),
            closed: state.closed,
        }
    }
    #[cfg(test)]
    fn pause(&self, point: PausePoint) {
        let pause = {
            let mut held = self.transition_pause.lock().unwrap();
            if held
                .as_ref()
                .is_some_and(|(selected, _)| *selected == point)
            {
                held.take().map(|(_, pause)| pause)
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause.reached.wait();
            pause.resume.wait();
        }
    }
    pub(crate) async fn shutdown(self: &Arc<Self>) -> Result<(), EventLogError> {
        let idle = {
            let mut state = self.state.lock().map_err(super::poisoned)?;
            state.closed = true;
            self.connections.close();
            self.waiters.close();
            let idle = std::mem::take(&mut state.idle);
            state.active += idle.len();
            idle
        };
        for connection in idle {
            // Shutdown cancellation cannot detach idle retirement from occupancy accounting.
            drop(Lease {
                pool: Arc::clone(self),
                client: Some(connection),
                reusable: false,
                connection_permit: None,
            });
        }
        tokio::time::timeout(self.options.shutdown_timeout, async {
            loop {
                let notified = self.returned.notified();
                if self.state.lock().map_err(super::poisoned)?.active == 0 {
                    break;
                }
                notified.await;
            }
            Ok::<(), EventLogError>(())
        })
        .await
        .map_err(|_| EventLogError::Deadline {
            operation: "shutdown",
        })?
    }
}

pub(crate) struct Lease {
    pool: Arc<Pool>,
    client: Option<Connection>,
    reusable: bool,
    connection_permit: Option<OwnedSemaphorePermit>,
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
        let reusable = self.reusable
            && !self
                .pool
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .closed;
        if reusable && let Some(connection) = self.client.take() {
            #[cfg(test)]
            if let Some(pause) = self.pool.recycle_pause.lock().unwrap().take() {
                pause.reached.wait();
                pause.resume.wait();
            }
            let mut state = self
                .pool
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !connection.client.is_closed() && !state.closed {
                state.idle.push(connection);
                state.active -= 1;
                drop(self.connection_permit.take());
                drop(state);
                self.pool.returned.notify_waiters();
                #[cfg(test)]
                self.pool.pause(PausePoint::Recycled);
                return;
            }
            drop(state);
            self.client = Some(connection);
        }
        // Closing is part of the outstanding lease. Capacity never returns on abort() alone.
        let connection = self.client.take();
        let permit = self.connection_permit.take();
        let pool = Arc::clone(&self.pool);
        tokio::spawn(async move {
            if let Some(mut connection) = connection {
                connection.close().await;
            }
            {
                let mut state = pool
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.active -= 1;
                drop(permit);
            }
            pool.returned.notify_waiters();
            #[cfg(test)]
            pool.pause(PausePoint::Retired);
        });
    }
}

struct Connection {
    client: Client,
    driver: Option<tokio::task::JoinHandle<()>>,
}
impl Connection {
    async fn close(&mut self) {
        if let Some(driver) = &mut self.driver {
            driver.abort();
            let _ = driver.await;
        }
        self.driver = None;
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(driver) = &self.driver {
            driver.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll, Waker},
    };

    fn fixture(connections: usize, waiters: usize) -> Option<Arc<Pool>> {
        let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL")
            .ok()
            .filter(|value| !value.trim().is_empty());
        assert!(
            url.is_some() || std::env::var_os("EVENTLOG_REQUIRE_POSTGRES").is_none(),
            "required PostgreSQL proof cannot skip an absent database URL"
        );
        let Some(url) = url else {
            eprintln!("skipped: pool transitions need EVENTLOG_TEST_POSTGRES_URL");
            return None;
        };
        Some(
            Pool::new(
                PostgresConfig::isolated(&url, "pool_observation").unwrap(),
                PoolOptions {
                    max_connections: connections,
                    max_waiters: waiters,
                    acquisition_timeout: Duration::from_millis(100),
                    ..PoolOptions::default()
                },
            )
            .unwrap(),
        )
    }

    fn pending<F: Future>(future: Pin<&mut F>) {
        assert!(matches!(
            future.poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
    }

    fn transition(pool: &Pool, point: PausePoint) -> RecyclePause {
        let pause = RecyclePause {
            reached: Arc::new(std::sync::Barrier::new(2)),
            resume: Arc::new(std::sync::Barrier::new(2)),
        };
        *pool.transition_pause.lock().unwrap() = Some((point, pause.clone()));
        pause
    }

    fn bounded(status: &PoolStatus) {
        assert!(status.checked_out <= status.max_connections, "{status:?}");
        assert!(status.waiting <= status.max_waiters, "{status:?}");
        assert!(
            status.checked_out + status.idle <= status.max_connections,
            "{status:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reusable_retirement_preserves_two_connection_four_waiter_snapshot() {
        let Some(pool) = fixture(2, 4) else { return };
        let mut first = pool.acquire().await.unwrap();
        let mut second = pool.acquire().await.unwrap();
        first.settled();
        second.settled();
        let mut queued = (0..4).map(|_| Box::pin(pool.acquire())).collect::<Vec<_>>();
        for future in &mut queued {
            pending(future.as_mut());
        }
        let saturated = pool.status();
        assert_eq!(
            (saturated.checked_out, saturated.waiting, saturated.idle),
            (2, 4, 0)
        );
        let pause = transition(&pool, PausePoint::Recycled);
        let returning = tokio::task::spawn_blocking(move || drop(first));
        pause.reached.wait();
        let observed = pool.status();
        eprintln!("reusable retirement snapshot: {observed:?}");
        pause.resume.wait();
        returning.await.unwrap();
        drop(queued);
        drop(second);
        pool.shutdown().await.unwrap();
        bounded(&observed);
        assert_eq!(
            (observed.checked_out, observed.waiting, observed.idle),
            (1, 4, 1)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quarantine_retirement_cannot_count_a_replacement_twice() {
        let Some(pool) = fixture(2, 4) else { return };
        let first = pool.acquire().await.unwrap();
        let mut second = pool.acquire().await.unwrap();
        second.settled();
        let mut replacement = Box::pin(pool.acquire());
        pending(replacement.as_mut());
        let pause = transition(&pool, PausePoint::Retired);
        drop(first);
        pause.reached.wait();
        pending(replacement.as_mut());
        let observed = pool.status();
        eprintln!("quarantine replacement snapshot: {observed:?}");
        pause.resume.wait();
        drop(replacement);
        drop(second);
        pool.shutdown().await.unwrap();
        bounded(&observed);
        assert_eq!(observed.checked_out, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_checkout_has_one_coherent_owner() {
        let Some(pool) = fixture(2, 4) else { return };
        let mut first = pool.acquire().await.unwrap();
        let mut second = pool.acquire().await.unwrap();
        first.settled();
        second.settled();
        drop(first);
        drop(second);
        assert_eq!(pool.status().idle, 2);
        let pause = transition(&pool, PausePoint::Checkout);
        let checking = pool.clone();
        let acquisition = tokio::spawn(async move { checking.acquire().await });
        pause.reached.wait();
        let observed = pool.status();
        eprintln!("idle checkout snapshot: {observed:?}");
        pause.resume.wait();
        let mut acquired = acquisition.await.unwrap().unwrap();
        acquired.settled();
        drop(acquired);
        pool.shutdown().await.unwrap();
        bounded(&observed);
        assert_eq!((observed.checked_out, observed.idle), (1, 1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_cancellation_timeout_and_granted_cancellation_release_capacity() {
        let Some(pool) = fixture(1, 1) else { return };
        let mut held = pool.acquire().await.unwrap();
        held.settled();
        let mut queued = Box::pin(pool.acquire());
        pending(queued.as_mut());
        assert_eq!(pool.status().waiting, 1);
        assert!(matches!(
            pool.acquire().await,
            Err(EventLogError::Overloaded)
        ));
        drop(queued);
        assert_eq!(pool.status().waiting, 0);
        let timed = pool.acquire().await;
        assert!(matches!(
            timed,
            Err(EventLogError::Deadline {
                operation: "pool acquisition"
            })
        ));
        assert_eq!(pool.status().waiting, 0);
        let mut granted = Box::pin(pool.acquire());
        pending(granted.as_mut());
        drop(held);
        // Tokio has handed the released permit to this queued future, which is never polled again.
        drop(granted);
        assert_eq!(pool.status().waiting, 0);
        let mut recovered = pool.acquire().await.unwrap();
        recovered.settled();
        drop(recovered);
        pool.shutdown().await.unwrap();
        bounded(&pool.status());
        assert_eq!(pool.status().checked_out, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zero_waiter_pool_reuses_and_refuses_without_queueing() {
        let Some(pool) = fixture(1, 0) else { return };
        let mut held = pool.acquire().await.unwrap();
        held.settled();
        assert!(matches!(
            pool.acquire().await,
            Err(EventLogError::Overloaded)
        ));
        assert_eq!(pool.status().waiting, 0);
        drop(held);
        let recovered = pool.acquire().await.unwrap();
        assert_eq!(pool.status().checked_out, 1);
        drop(recovered);
        pool.shutdown().await.unwrap();
        assert!(matches!(pool.acquire().await, Err(EventLogError::Closed)));
        bounded(&pool.status());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_cancels_waiters_and_drains_quarantine() {
        let Some(pool) = fixture(1, 1) else { return };
        let held = pool.acquire().await.unwrap();
        let mut queued = Box::pin(pool.acquire());
        pending(queued.as_mut());
        let closing = pool.clone();
        let shutdown = tokio::spawn(async move { closing.shutdown().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !pool.status().closed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(queued.await, Err(EventLogError::Closed)));
        assert!(
            !shutdown.is_finished(),
            "shutdown must retain the outstanding quarantined lease"
        );
        drop(held);
        shutdown.await.unwrap().unwrap();
        let observed = pool.status();
        bounded(&observed);
        assert_eq!(
            (observed.checked_out, observed.waiting, observed.idle),
            (0, 0, 0)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closed_idle_driver_is_joined_before_replacement_connects() {
        let Some(pool) = fixture(1, 0) else { return };
        let pause = RecyclePause {
            reached: Arc::new(std::sync::Barrier::new(2)),
            resume: Arc::new(std::sync::Barrier::new(2)),
        };
        *pool.driver_pause.lock().unwrap() = Some(pause.clone());
        let mut held = pool.acquire().await.unwrap();
        let pid: i32 = held
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        held.settled();
        drop(held);
        let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").unwrap();
        let (observer, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        let observer_driver = tokio::spawn(connection);
        let terminated: bool = observer
            .query_one("SELECT pg_terminate_backend($1)", &[&pid])
            .await
            .unwrap()
            .get(0);
        assert!(terminated);
        pause.reached.wait();
        let before = pool.connect_attempts.load(Ordering::Acquire);
        let mut replacement = Box::pin(pool.acquire());
        pending(replacement.as_mut());
        let during = pool.connect_attempts.load(Ordering::Acquire);
        eprintln!(
            "closed-idle replacement attempts before={before}, while old driver paused={during}"
        );
        // Cancel even while retirement is pending; the lease must retain the driver and capacity.
        drop(replacement);
        let cancelled = pool.status();
        let excess = pool.acquire().await;
        pause.resume.wait();
        pool.shutdown().await.unwrap();
        drop(observer);
        observer_driver.await.unwrap().unwrap();
        assert_eq!(cancelled.checked_out, 1);
        assert!(matches!(excess, Err(EventLogError::Overloaded)));
        assert_eq!(
            during, before,
            "a replacement cannot start until the retired driver has stopped"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_does_not_recycle_a_returning_connection_after_close() {
        let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL")
            .ok()
            .filter(|value| !value.trim().is_empty());
        assert!(
            url.is_some() || std::env::var_os("EVENTLOG_REQUIRE_POSTGRES").is_none(),
            "required PostgreSQL proof cannot skip an absent database URL"
        );
        let Some(url) = url else {
            eprintln!("skipped: PostgreSQL shutdown race needs EVENTLOG_TEST_POSTGRES_URL");
            return;
        };
        let pool = Pool::new(
            PostgresConfig::isolated(&url, "shutdown_race").unwrap(),
            PoolOptions::default(),
        )
        .unwrap();
        let mut lease = pool.acquire().await.unwrap();
        lease.settled();
        let reached = Arc::new(std::sync::Barrier::new(2));
        let resume = Arc::new(std::sync::Barrier::new(2));
        *pool.recycle_pause.lock().unwrap() = Some(RecyclePause {
            reached: reached.clone(),
            resume: resume.clone(),
        });
        let returning = tokio::task::spawn_blocking(move || drop(lease));
        reached.wait();
        let closing_pool = pool.clone();
        let closing = tokio::spawn(async move { closing_pool.shutdown().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !pool.status().closed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // The original return decision predates closure, while the idle insertion follows it.
        resume.wait();
        returning.await.unwrap();
        closing.await.unwrap().unwrap();
        let status = pool.status();
        eprintln!(
            "shutdown completed: closed={}, checked_out={}, idle={}",
            status.closed, status.checked_out, status.idle
        );
        assert!(status.closed);
        assert_eq!(status.checked_out, 0);
        assert_eq!(
            status.idle, 0,
            "shutdown must close a lease returned during closure"
        );
    }
}

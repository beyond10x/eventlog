//! The comparative capacity lane's connection budget and the tag that scopes it.

/// The `application_name` the sweep gives every worker process of both adapters.
pub const WORKLOAD_APPLICATION: &str = "eventlog-capacity-workload";

/// Two worker processes, each one pool of `PoolOptions::default()` (four connections).
pub const WORKLOAD_CONNECTION_BUDGET: u64 = 8;

/// The caller's connection string with `application_name` set, in URI or key/value form.
pub fn tagged_url(url: &str, application: &str) -> String {
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let separator = if url.contains('?') { '&' } else { '?' };
        format!("{url}{separator}application_name={application}")
    } else {
        format!("{url} application_name={application}")
    }
}

/// Whether a collector summary held the workload within its connection budget.
///
/// Counts only the backends carrying the workload's tag: the server also answers clients that
/// are not the workload, such as a health check's `pg_isready` backends, and those stay in the
/// all-backend count and its attribution. A maximum of zero means the tag never reached the
/// server, which proves nothing about the budget, so it fails.
pub fn within_connection_budget(metrics: &serde_json::Value) -> bool {
    metrics["workload_connections_max"]
        .as_u64()
        .is_some_and(|value| (1..=WORKLOAD_CONNECTION_BUDGET).contains(&value))
}

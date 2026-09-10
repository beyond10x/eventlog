//! Indexed document reads shared by the outer store and its transaction-scoped projection view.
use eventlog_core::{
    EventLogError, ProjectionPage, ProjectionQuery, ProjectionSpec, TenantId, bounded_limit,
};
use tokio_postgres::GenericClient;

use crate::{backend, projection_table, schema, to_i64};

pub(crate) async fn query<C: GenericClient>(
    client: &C,
    prefix: &str,
    projection: &ProjectionSpec,
    tenant: &TenantId,
    query: &ProjectionQuery,
) -> Result<ProjectionPage, EventLogError> {
    if !schema::projection_documents(client, prefix, projection).await? {
        return Err(EventLogError::Invalid(
            "document projection requires explicit migration".into(),
        ));
    }
    let table = projection_table(prefix, projection.name);
    let key_prefix = query.prefix.as_deref().unwrap_or("");
    let after = query.after.as_deref().unwrap_or("").max(key_prefix);
    let limit = bounded_limit(query.limit);
    let fetched = client.query(
        &format!("SELECT row_key,body FROM {table} WHERE tenant_id=$1 AND row_key COLLATE \"C\" > $2 AND starts_with(row_key,$3) AND body @> $4 ORDER BY row_key COLLATE \"C\" LIMIT $5"),
        &[&tenant.as_str(), &after, &key_prefix, &query.matching, &to_i64((limit + 1) as u64)?],
    ).await.map_err(backend)?;
    let more = fetched.len() > limit;
    let rows: Vec<(String, serde_json::Value)> = fetched
        .iter()
        .take(limit)
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    let next_cursor = if more {
        rows.last().map(|(key, _)| key.clone())
    } else {
        None
    };
    Ok(ProjectionPage { rows, next_cursor })
}

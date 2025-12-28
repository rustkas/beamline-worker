use crate::protocol::{ExecStatus, Job};
use serde_json::{Value, json};
use sqlx::{postgres::PgPoolOptions, Row, Column, Pool, Postgres};
use std::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use super::HandlerResult;

pub async fn handle_sql(
    pool_cache: &Arc<Mutex<HashMap<String, Pool<Postgres>>>>,
    job: &Job
) -> HandlerResult {
    let connection_string = match job.payload.get("connection_string").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("MISSING_CONNECTION_STRING".to_string()),
            Some("Missing 'connection_string' in payload".to_string())
        ),
    };

    let query_str = match job.payload.get("query").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("MISSING_QUERY".to_string()),
            Some("Missing 'query' in payload".to_string())
        ),
    };

    // 1. Try to get from cache
    let pool = {
        let cache = pool_cache.lock().await;
        cache.get(connection_string).cloned()
    };

    let pool = match pool {
        Some(p) => p,
        None => {
            // 2. Create new connection if not in cache
            // We release the lock during connection establishment to avoid blocking other jobs
            let new_pool = match PgPoolOptions::new()
                .max_connections(5) // Increased from 1 for better concurrency per DB
                .acquire_timeout(Duration::from_secs(10))
                .connect(connection_string)
                .await {
                    Ok(p) => p,
                    Err(e) => return (
                        ExecStatus::Error,
                        job.r#type.clone(),
                        None,
                        Some("DB_CONNECTION_ERROR".to_string()),
                        Some(e.to_string())
                    ),
                };
            
            // 3. Insert into cache
            let mut cache = pool_cache.lock().await;
            // Double-check if another task inserted it while we were connecting
            if let Some(p) = cache.get(connection_string) {
                p.clone()
            } else {
                cache.insert(connection_string.to_string(), new_pool.clone());
                new_pool
            }
        }
    };

    let mut query = sqlx::query(query_str);

    if let Some(args) = job.payload.get("args").and_then(|v| v.as_array()) {
         for arg in args {
             match arg {
                 Value::Null => query = query.bind(Option::<String>::None),
                 Value::Bool(b) => query = query.bind(b),
                 Value::Number(n) => {
                     if let Some(i) = n.as_i64() {
                         query = query.bind(i);
                     } else if let Some(f) = n.as_f64() {
                         query = query.bind(f);
                     } else {
                         query = query.bind(n.to_string());
                     }
                 },
                 Value::String(s) => query = query.bind(s),
                 Value::Array(_) | Value::Object(_) => {
                     query = query.bind(arg);
                 }
             }
         }
    }

    let result = match query.fetch_all(&pool).await {
        Ok(rows) => {
             let mut json_rows = Vec::new();
             for row in &rows {
                 let mut json_row = serde_json::Map::new();
                 for col in row.columns() {
                     let col_name = col.name();
                     let val: Value = if let Ok(v) = row.try_get::<bool, _>(col_name) {
                         json!(v)
                     } else if let Ok(v) = row.try_get::<i64, _>(col_name) {
                         json!(v)
                     } else if let Ok(v) = row.try_get::<f64, _>(col_name) {
                         json!(v)
                     } else if let Ok(v) = row.try_get::<String, _>(col_name) {
                         json!(v)
                     } else if let Ok(v) = row.try_get::<serde_json::Value, _>(col_name) {
                         v
                     } else {
                         json!("UNSUPPORTED_TYPE")
                     };
                     json_row.insert(col_name.to_string(), val);
                 }
                 json_rows.push(Value::Object(json_row));
             }
             
             json!({
                 "rows": json_rows,
                 "rows_affected": rows.len()
             })
        },
        Err(e) => {
             return (
                ExecStatus::Error,
                job.r#type.clone(),
                None,
                Some("DB_QUERY_ERROR".to_string()),
                Some(e.to_string())
             );
        }
    };

    (ExecStatus::Success, job.r#type.clone(), Some(result), None, None)
}

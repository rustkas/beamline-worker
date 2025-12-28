use crate::protocol::{ExecStatus, Job};
use serde_json::{Value, json};
use super::HandlerResult;
use tokio::time::sleep;
use std::time::Duration;

pub async fn handle_http(client: &reqwest::Client, job: &Job) -> HandlerResult {
    let url = match job.payload.get("url").and_then(|v| v.as_str()) {
        Some(u) => u,
        None => return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("MISSING_URL".to_string()),
            Some("Missing 'url' in payload".to_string())
        ),
    };

    let method_str = job.payload.get("method").and_then(|v| v.as_str()).unwrap_or("GET");
    let method = match reqwest::Method::from_bytes(method_str.as_bytes()) {
        Ok(m) => m,
        Err(_) => return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("INVALID_METHOD".to_string()),
            Some(format!("Invalid HTTP method: {}", method_str))
        ),
    };

    let mut req_builder = client.request(method, url);

    if let Some(headers) = job.payload.get("headers").and_then(|v| v.as_object()) {
        for (k, v) in headers {
            if let Some(v_str) = v.as_str() {
                req_builder = req_builder.header(k, v_str);
            }
        }
    }

    if let Some(body) = job.payload.get("body") {
        if let Some(s) = body.as_str() {
             req_builder = req_builder.body(s.to_string());
        } else {
             req_builder = req_builder.json(body);
        }
    }

    let request = match req_builder.build() {
        Ok(r) => r,
        Err(e) => return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("REQUEST_BUILD_ERROR".to_string()),
            Some(e.to_string())
        ),
    };

    let mut attempt = 0;
    let max_retries = 3;

    loop {
        let req_clone = match request.try_clone() {
            Some(r) => r,
            None => {
                 // Cannot clone (e.g. stream body), execute once
                 return execute_request(client, request, job).await;
            }
        };

        match client.execute(req_clone).await {
            Ok(res) => {
                if res.status().is_server_error() {
                    if attempt < max_retries {
                        attempt += 1;
                        let backoff = Duration::from_millis(100 * 2_u64.pow(attempt));
                        sleep(backoff).await;
                        continue;
                    }
                }
                // Success or client error, or max retries reached for server error
                return process_response(res, job).await;
            },
            Err(e) => {
                if attempt < max_retries {
                    attempt += 1;
                    let backoff = Duration::from_millis(100 * 2_u64.pow(attempt));
                    sleep(backoff).await;
                    continue;
                }
                return (
                    ExecStatus::Error,
                    job.r#type.clone(),
                    None,
                    Some("HTTP_REQUEST_FAILED".to_string()),
                    Some(e.to_string())
                );
            }
        }
    }
}

async fn execute_request(client: &reqwest::Client, req: reqwest::Request, job: &Job) -> HandlerResult {
    match client.execute(req).await {
        Ok(res) => process_response(res, job).await,
        Err(e) => (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("HTTP_REQUEST_FAILED".to_string()),
            Some(e.to_string())
        )
    }
}

async fn process_response(res: reqwest::Response, job: &Job) -> HandlerResult {
    let status_code = res.status().as_u16();
    let headers_map = res.headers().clone();
    let mut headers_json = serde_json::Map::new();
    for (k, v) in headers_map.iter() {
            if let Ok(v_str) = v.to_str() {
                headers_json.insert(k.to_string(), Value::String(v_str.to_string()));
            }
    }

    let body_result = res.text().await.unwrap_or_default();
    let body_json = serde_json::from_str::<Value>(&body_result).unwrap_or(Value::String(body_result));

    let output = json!({
        "status": status_code,
        "headers": headers_json,
        "body": body_json
    });

    (ExecStatus::Success, job.r#type.clone(), Some(output), None, None)
}

pub async fn handle_graphql(client: &reqwest::Client, job: &Job) -> HandlerResult {
    let url = match job.payload.get("url").and_then(|v| v.as_str()) {
         Some(u) => u,
         None => return (
             ExecStatus::Error,
             job.r#type.clone(),
             None,
             Some("MISSING_URL".to_string()),
             Some("Missing 'url' in payload".to_string())
         ),
    };

    let query = match job.payload.get("query").and_then(|v| v.as_str()) {
         Some(q) => q,
         None => return (
             ExecStatus::Error,
             job.r#type.clone(),
             None,
             Some("MISSING_QUERY".to_string()),
             Some("Missing 'query' in payload".to_string())
         ),
    };
    
    let default_vars = json!({});
    let variables = job.payload.get("variables").unwrap_or(&default_vars);
    let operation_name = job.payload.get("operationName").and_then(|v| v.as_str());

    let mut req_builder = client.post(url);
    
    if let Some(headers) = job.payload.get("headers").and_then(|v| v.as_object()) {
        for (k, v) in headers {
            if let Some(v_str) = v.as_str() {
                req_builder = req_builder.header(k, v_str);
            }
        }
    }

    let body = json!({
        "query": query,
        "variables": variables,
        "operationName": operation_name
    });

    let request = match req_builder.json(&body).build() {
        Ok(r) => r,
        Err(e) => return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("REQUEST_BUILD_ERROR".to_string()),
            Some(e.to_string())
        ),
    };

    let mut attempt = 0;
    let max_retries = 3;

    loop {
        let req_clone = match request.try_clone() {
            Some(r) => r,
            None => {
                 // Fallback to single execution
                 return execute_graphql_request(client, request, job).await;
            }
        };

        match client.execute(req_clone).await {
            Ok(res) => {
                if res.status().is_server_error() {
                     if attempt < max_retries {
                         attempt += 1;
                         let backoff = Duration::from_millis(100 * 2_u64.pow(attempt));
                         sleep(backoff).await;
                         continue;
                     }
                }
                return process_graphql_response(res, job).await;
            },
            Err(e) => {
                if attempt < max_retries {
                    attempt += 1;
                    let backoff = Duration::from_millis(100 * 2_u64.pow(attempt));
                    sleep(backoff).await;
                    continue;
                }
                return (
                     ExecStatus::Error,
                     job.r#type.clone(),
                     None,
                     Some("GRAPHQL_REQUEST_FAILED".to_string()),
                     Some(e.to_string())
                );
            }
        }
    }
}

async fn execute_graphql_request(client: &reqwest::Client, req: reqwest::Request, job: &Job) -> HandlerResult {
    match client.execute(req).await {
        Ok(res) => process_graphql_response(res, job).await,
        Err(e) => (
             ExecStatus::Error,
             job.r#type.clone(),
             None,
             Some("GRAPHQL_REQUEST_FAILED".to_string()),
             Some(e.to_string())
        )
    }
}

async fn process_graphql_response(res: reqwest::Response, job: &Job) -> HandlerResult {
    let body_json: Value = match res.json().await {
        Ok(v) => v,
        Err(e) => return (
             ExecStatus::Error,
             job.r#type.clone(),
             None,
             Some("GRAPHQL_RESPONSE_PARSE_ERROR".to_string()),
             Some(e.to_string())
        ),
    };

    (ExecStatus::Success, job.r#type.clone(), Some(body_json), None, None)
}

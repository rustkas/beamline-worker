use crate::protocol::{ExecAssignment, ExecResult, ExecStatus};
use crate::handlers;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct Executor {
    worker_id: String,
    http_client: reqwest::Client,
    db_pool_cache: Arc<Mutex<HashMap<String, Pool<Postgres>>>>,
    fs_base_dir: String,
}

impl Executor {
    pub fn new(worker_id: String, fs_base_dir: String) -> Self {
        Self {
            worker_id,
            http_client: reqwest::Client::new(),
            db_pool_cache: Arc::new(Mutex::new(HashMap::new())),
            fs_base_dir,
        }
    }
    pub fn id(&self) -> &str {
        &self.worker_id
    }

    pub async fn execute(&self, assignment: ExecAssignment) -> ExecResult {
        let start = std::time::Instant::now();
        
        // Execute the job logic
        let (status, job_output, output, error_code, error_message) = match assignment.job.r#type.as_str() {
            "echo" => handlers::common::handle_echo(&assignment.job).await,
            "sleep" => handlers::common::handle_sleep(&assignment.job).await,
            "http" => handlers::http::handle_http(&self.http_client, &assignment.job).await,
            "jmespath" => handlers::script::handle_jmespath(&assignment.job).await,
            "javascript" => handlers::script::handle_javascript(&assignment.job).await,
            "sql" => handlers::sql::handle_sql(&self.db_pool_cache, &assignment.job).await,
            "graphql" => handlers::http::handle_graphql(&self.http_client, &assignment.job).await,
            "fs_blob_get" => handlers::fs::handle_fs_blob_get(&self.fs_base_dir, &assignment.job).await,
            "fs_blob_put" => handlers::fs::handle_fs_blob_put(&self.fs_base_dir, &assignment.job).await,
            "human_approval" => handlers::human::handle_human_approval(&assignment.job).await,
            _ => (
                ExecStatus::Error,
                assignment.job.r#type.clone(),
                None,
                Some("UNKNOWN_JOB_TYPE".to_string()),
                Some(format!("Unknown job type: {}", assignment.job.r#type)),
            ),
        };

        let duration = start.elapsed();
        
        ExecResult {
            version: "1.0".to_string(),
            assignment_id: assignment.assignment_id,
            request_id: assignment.request_id,
            status,
            provider_id: self.worker_id.clone(),
            job_type: job_output,
            output,
            latency_ms: duration.as_millis() as u64,
            cost: 0.0,
            trace_id: assignment.trace_id,
            tenant_id: Some(assignment.tenant_id),
            run_id: assignment.run_id,
            error_code,
            error_message,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Job;
    use serde_json::json;

    #[tokio::test]
    async fn test_echo_job() {
        let executor = Executor::new("worker-test".to_string(), "/tmp".to_string());
        let assignment = ExecAssignment {
            version: "1.0".to_string(),
            assignment_id: "a1".to_string(),
            request_id: "r1".to_string(),
            tenant_id: "t1".to_string(),
            job: Job {
                r#type: "echo".to_string(),
                payload: json!({"hello": "world"}),
            },
            trace_id: None,
            run_id: None,
            flow_id: None,
            step_id: None,
        };

        let result = executor.execute(assignment).await;
        matches!(result.status, ExecStatus::Success);
        assert_eq!(result.job_type, "echo");
        assert_eq!(result.provider_id, "worker-test");
    }

    #[tokio::test]
    async fn test_unknown_job() {
        let executor = Executor::new("worker-test".to_string(), "/tmp".to_string());
        let assignment = ExecAssignment {
            version: "1.0".to_string(),
            assignment_id: "a1".to_string(),
            request_id: "r1".to_string(),
            tenant_id: "t1".to_string(),
            job: Job {
                r#type: "quantum_compute".to_string(),
                payload: json!({}),
            },
            trace_id: None,
            run_id: None,
            flow_id: None,
            step_id: None,
        };

        let result = executor.execute(assignment).await;
        matches!(result.status, ExecStatus::Error);
        assert_eq!(result.error_code, Some("UNKNOWN_JOB_TYPE".to_string()));
    }

    #[tokio::test]
    async fn test_http_job_real() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        
        tokio::spawn(async move {
            let app = axum::Router::new().route("/", axum::routing::post(|body: String| async move {
                body
            }));
            axum::serve(listener, app).await.unwrap();
        });

        let executor = Executor::new("worker-test".to_string(), "/tmp".to_string());
        let assignment = ExecAssignment {
            version: "1.0".to_string(),
            assignment_id: "a1".to_string(),
            request_id: "r1".to_string(),
            tenant_id: "t1".to_string(),
            job: Job {
                r#type: "http".to_string(),
                payload: json!({
                    "method": "POST",
                    "url": format!("http://127.0.0.1:{}", port),
                    "body": "hello_http"
                }),
            },
            trace_id: None,
            run_id: None,
            flow_id: None,
            step_id: None,
        };

        let result = executor.execute(assignment).await;
        match result.status {
            ExecStatus::Success => {
                 let output = result.output.unwrap();
                 let body = output.get("body").unwrap().as_str().unwrap();
                 assert_eq!(body, "hello_http");
            }
            _ => panic!("HTTP job failed: {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_jmespath_job() {
        let executor = Executor::new("worker-test".to_string(), "/tmp".to_string());
        let assignment = ExecAssignment {
            version: "1.0".to_string(),
            assignment_id: "a1".to_string(),
            request_id: "r1".to_string(),
            tenant_id: "t1".to_string(),
            job: Job {
                r#type: "jmespath".to_string(),
                payload: json!({
                    "expression": "foo.bar",
                    "data": {
                        "foo": {
                            "bar": "baz"
                        }
                    }
                }),
            },
            trace_id: None,
            run_id: None,
            flow_id: None,
            step_id: None,
        };

        let result = executor.execute(assignment).await;
        matches!(result.status, ExecStatus::Success);
        assert_eq!(result.output, Some(json!("baz")));
    }

    #[tokio::test]
    async fn test_javascript_job() {
        let executor = Executor::new("worker-test".to_string(), "/tmp".to_string());
        let assignment = ExecAssignment {
            version: "1.0".to_string(),
            assignment_id: "a1".to_string(),
            request_id: "r1".to_string(),
            tenant_id: "t1".to_string(),
            job: Job {
                r#type: "javascript".to_string(),
                payload: json!({
                    "code": "x * 2",
                    "args": {
                        "x": 21
                    }
                }),
            },
            trace_id: None,
            run_id: None,
            flow_id: None,
            step_id: None,
        };

        let result = executor.execute(assignment).await;
        matches!(result.status, ExecStatus::Success);
        assert_eq!(result.output, Some(json!(42)));
    }

    #[tokio::test]
    async fn test_fs_blob_get_job() {
        use base64::engine::general_purpose;
        use base64::Engine as _;
        let executor = Executor::new("worker-test".to_string(), "/tmp".to_string());
        let path = "test_fs_blob_get.txt";
        let abs_path = "/tmp/test_fs_blob_get.txt";
        let content = "Hello File Blob";
        tokio::fs::write(abs_path, content).await.unwrap();

        let assignment = ExecAssignment {
            version: "1.0".to_string(),
            assignment_id: "a1".to_string(),
            request_id: "r1".to_string(),
            tenant_id: "t1".to_string(),
            job: Job {
                r#type: "fs_blob_get".to_string(),
                payload: json!({
                    "path": path
                }),
            },
            trace_id: None,
            run_id: None,
            flow_id: None,
            step_id: None,
        };

        let result = executor.execute(assignment).await;
        
        let _ = tokio::fs::remove_file(abs_path).await;

        match result.status {
            ExecStatus::Success => {
                 let output = result.output.unwrap();
                 let bytes_b64 = output.get("bytes").unwrap().as_str().unwrap();
                 let bytes = general_purpose::STANDARD.decode(bytes_b64).unwrap();
                 assert_eq!(String::from_utf8(bytes).unwrap(), content);
                 assert_eq!(output.get("path").unwrap().as_str().unwrap(), path);
            }
            _ => panic!("FS Blob Get job failed: {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_fs_blob_put_job() {
        use base64::engine::general_purpose;
        use base64::Engine as _;
        let executor = Executor::new("worker-test".to_string(), "/tmp".to_string());
        let path = "test_fs_blob_put.txt";
        let abs_path = "/tmp/test_fs_blob_put.txt";
        let content = "Hello Write Blob";
        let content_b64 = general_purpose::STANDARD.encode(content);

        let assignment = ExecAssignment {
            version: "1.0".to_string(),
            assignment_id: "a1".to_string(),
            request_id: "r1".to_string(),
            tenant_id: "t1".to_string(),
            job: Job {
                 r#type: "fs_blob_put".to_string(),
                 payload: json!({
                     "path": path,
                     "bytes": content_b64
                }),
            },
            trace_id: None,
            run_id: None,
            flow_id: None,
            step_id: None,
        };

        let result = executor.execute(assignment).await;
        
        match result.status {
            ExecStatus::Success => {
                 let output = result.output.unwrap();
                 assert_eq!(output.get("path").unwrap().as_str().unwrap(), path);
                 
                 let read_content = tokio::fs::read_to_string(abs_path).await.unwrap();
                 assert_eq!(read_content, content);
            }
            _ => panic!("FS Blob Put job failed: {:?}", result),
        }

        let _ = tokio::fs::remove_file(abs_path).await;
    }

    #[tokio::test]
    async fn test_human_approval_job() {
         let executor = Executor::new("worker-test".to_string(), "/tmp".to_string());
         
         let assignment = ExecAssignment {
             version: "1.0".to_string(),
             assignment_id: "a1".to_string(),
             request_id: "r1".to_string(),
             tenant_id: "t1".to_string(),
             job: Job {
                 r#type: "human_approval".to_string(),
                 payload: json!({
                     "prompt": "Do you approve this deployment?",
                     "options": ["Yes", "No"],
                     "response": "Yes" 
                 }),
             },
             trace_id: None,
             run_id: None,
             flow_id: None,
             step_id: None,
         };

         let result = executor.execute(assignment).await;
         
         match result.status {
             ExecStatus::Success => {
                  let output = result.output.unwrap();
                  assert_eq!(output.get("decision").unwrap().as_str().unwrap(), "Yes");
                  assert_eq!(output.get("prompt").unwrap().as_str().unwrap(), "Do you approve this deployment?");
             }
             _ => panic!("Human Approval job failed: {:?}", result),
         }
    }
}

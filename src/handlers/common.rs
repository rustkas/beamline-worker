use crate::protocol::{ExecStatus, Job};
use std::time::Duration;
use tokio::time::sleep;
use super::HandlerResult;

pub async fn handle_echo(job: &Job) -> HandlerResult {
    (ExecStatus::Success, job.r#type.clone(), Some(job.payload.clone()), None, None)
}

pub async fn handle_sleep(job: &Job) -> HandlerResult {
    let ms = job.payload.get("ms").and_then(|v| v.as_u64()).unwrap_or(100);
    sleep(Duration::from_millis(ms)).await;
    (ExecStatus::Success, job.r#type.clone(), None, None, None)
}

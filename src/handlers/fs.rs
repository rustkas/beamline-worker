use crate::protocol::{ExecStatus, Job};
use serde_json::json;
use base64::{Engine as _, engine::general_purpose};
use super::HandlerResult;
use std::path::Path;

pub async fn handle_fs_blob_get(base_dir: &str, job: &Job) -> HandlerResult {
    let path_str = match job.payload.get("path").and_then(|v| v.as_str()) {
        Some(p) => p,
        None => return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("MISSING_PATH".to_string()),
            Some("Missing 'path' in payload".to_string())
        ),
    };

    if path_str.contains("..") || Path::new(path_str).is_absolute() {
         return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("INVALID_PATH".to_string()),
            Some("Path traversal or absolute path not allowed".to_string())
         );
    }
    let full_path = Path::new(base_dir).join(path_str);

    match tokio::fs::read(&full_path).await {
        Ok(content) => {
            let encoded = general_purpose::STANDARD.encode(&content);
            let output = json!({
                "path": path_str,
                "bytes": encoded,
                "size": content.len()
            });
            (ExecStatus::Success, job.r#type.clone(), Some(output), None, None)
        },
        Err(e) => (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("FILE_READ_ERROR".to_string()),
            Some(e.to_string())
        )
    }
}

pub async fn handle_fs_blob_put(base_dir: &str, job: &Job) -> HandlerResult {
    let path_str = match job.payload.get("path").and_then(|v| v.as_str()) {
        Some(p) => p,
        None => return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("MISSING_PATH".to_string()),
            Some("Missing 'path' in payload".to_string())
        ),
    };

    if path_str.contains("..") || Path::new(path_str).is_absolute() {
         return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("INVALID_PATH".to_string()),
            Some("Path traversal or absolute path not allowed".to_string())
         );
    }
    let full_path = Path::new(base_dir).join(path_str);

    let content_bytes = if let Some(bytes_b64) = job.payload.get("bytes").and_then(|v| v.as_str()) {
         match general_purpose::STANDARD.decode(bytes_b64) {
             Ok(b) => b,
             Err(e) => return (
                ExecStatus::Error,
                job.r#type.clone(),
                None,
                Some("BASE64_DECODE_ERROR".to_string()),
                Some(e.to_string())
             )
         }
    } else if let Some(content_str) = job.payload.get("content").and_then(|v| v.as_str()) {
         content_str.as_bytes().to_vec()
    } else {
         return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("MISSING_CONTENT".to_string()),
            Some("Missing 'bytes' (base64) or 'content' (string) in payload".to_string())
         )
    };

    if let Some(parent) = full_path.parent() {
         if let Err(e) = tokio::fs::create_dir_all(parent).await {
             return (
                ExecStatus::Error,
                job.r#type.clone(),
                None,
                Some("DIR_CREATE_ERROR".to_string()),
                Some(e.to_string())
             );
         }
    }

    match tokio::fs::write(&full_path, &content_bytes).await {
        Ok(_) => {
            let output = json!({
                "path": path_str,
                "size": content_bytes.len()
            });
            (ExecStatus::Success, job.r#type.clone(), Some(output), None, None)
        },
        Err(e) => (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("FILE_WRITE_ERROR".to_string()),
            Some(e.to_string())
        )
    }
}

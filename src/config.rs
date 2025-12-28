use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub nats_url: String,
    pub caf_assign_subject: String,
    pub caf_result_subject: String,
    pub caf_heartbeat_subject: String,
    pub caf_heartbeat_interval_ms: u64,
    pub worker_id: String,
    pub health_bind: String,
    pub max_concurrency: usize,
    pub default_job_timeout_ms: u64,
    pub caf_dlq_subject: String,
    pub result_publish_max_retries: u32,
    pub dlq_path: String,
    pub dlq_max_bytes: u64,
    pub dlq_max_rotations: u32,
    pub dlq_total_max_bytes: u64,
    pub dlq_max_age_days: Option<u32>,
    pub fs_base_dir: String,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
        if nats_url.trim().is_empty() {
            return Err("NATS_URL cannot be empty".to_string());
        }

        let caf_assign_subject = env::var("CAF_ASSIGN_SUBJECT")
            .unwrap_or_else(|_| "caf.exec.assign.v1".to_string());
        
        let caf_result_subject = env::var("CAF_RESULT_SUBJECT")
            .unwrap_or_else(|_| "caf.exec.result.v1".to_string());
            
        let caf_heartbeat_subject = env::var("CAF_HEARTBEAT_SUBJECT")
            .unwrap_or_else(|_| "caf.status.heartbeat.v1".to_string());
            
        let caf_heartbeat_interval_ms = env::var("CAF_HEARTBEAT_INTERVAL_MS")
            .unwrap_or_else(|_| "5000".to_string())
            .parse::<u64>()
            .map_err(|_| "CAF_HEARTBEAT_INTERVAL_MS must be a number".to_string())?;
        if !(100..=600_000).contains(&caf_heartbeat_interval_ms) {
            return Err("CAF_HEARTBEAT_INTERVAL_MS must be between 100 and 600000".to_string());
        }

        let worker_id = env::var("WORKER_ID")
            .unwrap_or_else(|_| format!("worker-{}", uuid::Uuid::new_v4()));
        if worker_id.trim().is_empty() {
            return Err("WORKER_ID cannot be empty".to_string());
        }
        if !is_valid_subject(&caf_assign_subject) {
            return Err("CAF_ASSIGN_SUBJECT invalid format".to_string());
        }
        if !is_valid_subject(&caf_result_subject) {
            return Err("CAF_RESULT_SUBJECT invalid format".to_string());
        }
        if !is_valid_subject(&caf_heartbeat_subject) {
            return Err("CAF_HEARTBEAT_SUBJECT invalid format".to_string());
        }
            
        let health_bind = env::var("HEALTH_BIND")
            .unwrap_or_else(|_| "0.0.0.0:9091".to_string());

        let max_concurrency = env::var("WORKER_MAX_CONCURRENCY")
            .unwrap_or_else(|_| "8".to_string())
            .parse::<usize>()
            .map_err(|_| "WORKER_MAX_CONCURRENCY must be a number".to_string())?;
        if !(1..=256).contains(&max_concurrency) {
            return Err("WORKER_MAX_CONCURRENCY must be between 1 and 256".to_string());
        }

        let default_job_timeout_ms = env::var("DEFAULT_JOB_TIMEOUT_MS")
            .unwrap_or_else(|_| "60000".to_string())
            .parse::<u64>()
            .map_err(|_| "DEFAULT_JOB_TIMEOUT_MS must be a number".to_string())?;
        if !(100..=3_600_000).contains(&default_job_timeout_ms) {
            return Err("DEFAULT_JOB_TIMEOUT_MS must be between 100 and 3600000".to_string());
        }

        let caf_dlq_subject = env::var("CAF_DLQ_SUBJECT")
            .unwrap_or_else(|_| "caf.deadletter.v1".to_string());
        if !is_valid_subject(&caf_dlq_subject) {
            return Err("CAF_DLQ_SUBJECT invalid format".to_string());
        }

        let dlq_path = env::var("DLQ_PATH")
            .unwrap_or_else(|_| "/tmp/worker-dlq.jsonl".to_string());
        if dlq_path.trim().is_empty() {
            return Err("DLQ_PATH cannot be empty".to_string());
        }

        let result_publish_max_retries = env::var("RESULT_PUBLISH_MAX_RETRIES")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<u32>()
            .map_err(|_| "RESULT_PUBLISH_MAX_RETRIES must be a number".to_string())?;
        if !(0..=20).contains(&result_publish_max_retries) {
            return Err("RESULT_PUBLISH_MAX_RETRIES must be between 0 and 20".to_string());
        }

        let dlq_max_bytes = env::var("DLQ_MAX_BYTES")
            .unwrap_or_else(|_| (100_u64 * 1024 * 1024).to_string())
            .parse::<u64>()
            .map_err(|_| "DLQ_MAX_BYTES must be a number".to_string())?;
        if !(1_000_000..=10_000_000_000).contains(&dlq_max_bytes) {
            return Err("DLQ_MAX_BYTES must be between 1MB and 10GB".to_string());
        }

        let dlq_max_rotations = env::var("DLQ_MAX_ROTATIONS")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<u32>()
            .map_err(|_| "DLQ_MAX_ROTATIONS must be a number".to_string())?;
        if !(1..=100).contains(&dlq_max_rotations) {
            return Err("DLQ_MAX_ROTATIONS must be between 1 and 100".to_string());
        }

        let dlq_total_max_bytes = env::var("DLQ_TOTAL_MAX_BYTES")
            .unwrap_or_else(|_| (1024_u64 * 1024 * 1024).to_string()) // 1GB
            .parse::<u64>()
            .map_err(|_| "DLQ_TOTAL_MAX_BYTES must be a number".to_string())?;
        if !(1_000_000..=100_000_000_000).contains(&dlq_total_max_bytes) {
            return Err("DLQ_TOTAL_MAX_BYTES must be between 1MB and 100GB".to_string());
        }
        if dlq_total_max_bytes < dlq_max_bytes {
            return Err("DLQ_TOTAL_MAX_BYTES must be >= DLQ_MAX_BYTES".to_string());
        }

        let dlq_max_age_days = match env::var("DLQ_MAX_AGE_DAYS") {
            Ok(v) => {
                let d = v.parse::<u32>().map_err(|_| "DLQ_MAX_AGE_DAYS must be a number".to_string())?;
                if !(1..=36500).contains(&d) {
                    return Err("DLQ_MAX_AGE_DAYS must be between 1 and 36500".to_string());
                }
                Some(d)
            }
            Err(_) => None,
        };

        let fs_base_dir = env::var("FS_BASE_DIR")
            .unwrap_or_else(|_| "/tmp/worker-storage".to_string());

        Ok(Config {
            nats_url,
            caf_assign_subject,
            caf_result_subject,
            caf_heartbeat_subject,
            caf_heartbeat_interval_ms,
            worker_id,
            health_bind,
            max_concurrency,
            default_job_timeout_ms,
            caf_dlq_subject,
            result_publish_max_retries,
            dlq_path,
            dlq_max_bytes,
            dlq_max_rotations,
            dlq_total_max_bytes,
            dlq_max_age_days,
            fs_base_dir,
        })
    }
}

fn is_valid_subject(s: &str) -> bool {
    if s.trim().is_empty() {
        return false;
    }
    let allowed = s.chars().all(|c| {
        c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-'
    });
    allowed && !s.contains("..") && !s.starts_with('.') && !s.ends_with('.')
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_config_defaults() {
        // Ensure no env vars interfere
        env::remove_var("NATS_URL");
        env::remove_var("WORKER_ID");
        env::remove_var("CAF_ASSIGN_SUBJECT");
        env::remove_var("CAF_RESULT_SUBJECT");
        env::remove_var("CAF_HEARTBEAT_SUBJECT");
        env::remove_var("CAF_HEARTBEAT_INTERVAL_MS");
        env::remove_var("HEALTH_BIND");
        env::remove_var("WORKER_MAX_CONCURRENCY");
        env::remove_var("DEFAULT_JOB_TIMEOUT_MS");
        env::remove_var("CAF_DLQ_SUBJECT");
        env::remove_var("RESULT_PUBLISH_MAX_RETRIES");
        env::remove_var("FS_BASE_DIR");
        
        let config = Config::from_env().unwrap();
        assert_eq!(config.nats_url, "nats://localhost:4222");
        assert_eq!(config.health_bind, "0.0.0.0:9091");
        assert!(config.worker_id.starts_with("worker-"));
        assert_eq!(config.fs_base_dir, "/tmp/worker-storage");
    }

    #[test]
    #[serial]
    fn test_config_override() {
        env::set_var("NATS_URL", "nats://demo:4222");
        env::set_var("WORKER_ID", "my-worker");
        env::set_var("FS_BASE_DIR", "/var/lib/worker");
        
        let config = Config::from_env().unwrap();
        assert_eq!(config.nats_url, "nats://demo:4222");
        assert_eq!(config.worker_id, "my-worker");
        assert_eq!(config.fs_base_dir, "/var/lib/worker");
        
        env::remove_var("NATS_URL");
        env::remove_var("WORKER_ID");
        env::remove_var("FS_BASE_DIR");
    }

    #[test]
    #[serial]
    fn test_config_validation() {
        env::set_var("WORKER_MAX_CONCURRENCY", "0");
        assert!(Config::from_env().is_err());
        env::remove_var("WORKER_MAX_CONCURRENCY");

        env::set_var("CAF_ASSIGN_SUBJECT", "invalid space");
        assert!(Config::from_env().is_err());
        env::remove_var("CAF_ASSIGN_SUBJECT");
    }

    #[test]
    #[serial]
    fn test_invalid_subjects_and_intervals() {
        env::set_var("CAF_HEARTBEAT_INTERVAL_MS", "10");
        assert!(Config::from_env().is_err());
        env::remove_var("CAF_HEARTBEAT_INTERVAL_MS");
    }
}

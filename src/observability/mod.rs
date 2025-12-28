pub mod pii;
pub mod metrics;

use chrono::Utc;
use serde_json::{json, Value};
use self::pii::mask_pii;

#[derive(Clone)]
pub struct Logger {
    worker_id: String,
}

impl Logger {
    pub fn new(worker_id: String) -> Self {
        Self { worker_id }
    }

    pub fn info(&self, msg: &str, context: Option<&Value>) {
        let entry = self.build_entry("INFO", msg, context);
        println!("{}", serde_json::to_string(&entry).unwrap_or_default());
    }

    pub fn error(&self, msg: &str, context: Option<&Value>) {
        let entry = self.build_entry("ERROR", msg, context);
        eprintln!("{}", serde_json::to_string(&entry).unwrap_or_default());
    }

    fn build_entry(&self, level: &str, msg: &str, context: Option<&Value>) -> Value {
        let now = Utc::now().to_rfc3339();
        let safe_msg = mask_pii(msg);

        let mut base = json!({
            "ts": now,
            "level": level,
            "msg": safe_msg,
            "worker_id": self.worker_id,
        });

        if let Some(ctx) = context {
            if let Some(base_obj) = base.as_object_mut() {
                if let Some(ctx_obj) = ctx.as_object() {
                    for (k, v) in ctx_obj {
                        // Apply PII masking to string values in context
                        let safe_v = if let Some(s) = v.as_str() {
                            json!(mask_pii(s))
                        } else {
                            v.clone()
                        };
                        base_obj.insert(k.clone(), safe_v);
                    }
                }
            }
        }
        
        base
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logger_structure() {
        let logger = Logger::new("worker-test".to_string());
        let context = json!({"tenant_id": "tenant-1", "user_email": "admin@example.com"});
        
        let entry = logger.build_entry("INFO", "User login user@example.com", Some(&context));
        
        assert_eq!(entry["level"], "INFO");
        assert_eq!(entry["worker_id"], "worker-test");
        assert!(entry["ts"].is_string());
        
        // Check PII masking in msg
        assert_eq!(entry["msg"], "User login ***@***.***");
        
        // Check PII masking in context
        assert_eq!(entry["tenant_id"], "tenant-1");
        assert_eq!(entry["user_email"], "***@***.***");
    }
}

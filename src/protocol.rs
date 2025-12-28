use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum EnvelopeKind {
    #[serde(rename = "exec_assign")]
    ExecAssign,
    #[serde(rename = "exec_result")]
    ExecResult,
    #[serde(rename = "heartbeat")]
    Heartbeat,
    #[serde(rename = "dead_letter")]
    DeadLetter,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventEnvelopeV1 {
    pub version: String,
    pub kind: EnvelopeKind,
    pub data: Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeadLetter {
    pub reason: String,
    pub payload_ref: Value,
    pub ts: String,
}

impl EventEnvelopeV1 {
    #[allow(dead_code)]
    pub fn wrap_assignment(a: &ExecAssignment) -> Self {
        Self {
            version: "v1".to_string(),
            kind: EnvelopeKind::ExecAssign,
            data: serde_json::to_value(a).unwrap_or(Value::Null),
        }
    }
    pub fn wrap_result(r: &ExecResult) -> Self {
        Self {
            version: "v1".to_string(),
            kind: EnvelopeKind::ExecResult,
            data: serde_json::to_value(r).unwrap_or(Value::Null),
        }
    }
    pub fn wrap_heartbeat(h: &WorkerHeartbeat) -> Self {
        Self {
            version: "v1".to_string(),
            kind: EnvelopeKind::Heartbeat,
            data: serde_json::to_value(h).unwrap_or(Value::Null),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExecAssignment {
    pub version: String,
    pub assignment_id: String,
    pub request_id: String,
    pub tenant_id: String,
    pub job: Job,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Job {
    pub r#type: String,
    pub payload: Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ExecStatus {
    #[serde(rename = "success")]
    Success,
    #[serde(rename = "error")]
    Error,
    #[serde(rename = "timeout")]
    Timeout,
    #[serde(rename = "cancelled")]
    Cancelled,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExecResult {
    pub version: String,
    pub assignment_id: String,
    pub request_id: String,
    pub status: ExecStatus,
    pub provider_id: String,
    pub job_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    pub latency_ms: u64,
    pub cost: f64,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerHeartbeat {
    pub worker_id: String,
    pub timestamp: String,
    pub status: String, // e.g., "idle", "busy"
    pub load: f64,      // 0.0 to 1.0
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TaskState {
    #[serde(rename = "queued")]
    Queued,
    #[serde(rename = "running")]
    Running,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "failed")]
    Failed,
    #[serde(rename = "cancelled")]
    Cancelled,
    #[serde(rename = "timeout")]
    Timeout,
}

pub fn map_status_to_task_state(s: &ExecStatus) -> TaskState {
    match s {
        ExecStatus::Success => TaskState::Completed,
        ExecStatus::Error => TaskState::Failed,
        ExecStatus::Timeout => TaskState::Timeout,
        ExecStatus::Cancelled => TaskState::Cancelled,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_envelope_wrap_assignment() {
        let assignment = ExecAssignment {
            version: "1.0".to_string(),
            assignment_id: "assign-1".to_string(),
            request_id: "req-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            job: Job {
                r#type: "http".to_string(),
                payload: json!({"url": "http://example.com"}),
            },
            trace_id: Some("trace-1".to_string()),
            run_id: None,
            flow_id: None,
            step_id: None,
        };
        let env = EventEnvelopeV1::wrap_assignment(&assignment);
        assert!(matches!(env.kind, EnvelopeKind::ExecAssign));
        let parsed: ExecAssignment = serde_json::from_value(env.data).unwrap();
        assert_eq!(parsed.assignment_id, "assign-1");
    }

    #[test]
    fn test_envelope_wrap_result() {
        let result = ExecResult {
            version: "1.0".to_string(),
            assignment_id: "assign-1".to_string(),
            request_id: "req-1".to_string(),
            status: ExecStatus::Success,
            provider_id: "worker-1".to_string(),
            job_type: "http".to_string(),
            output: None,
            latency_ms: 100,
            cost: 0.0,
            trace_id: None,
            tenant_id: None,
            run_id: None,
            error_code: None,
            error_message: None,
        };
        let env = EventEnvelopeV1::wrap_result(&result);
        assert!(matches!(env.kind, EnvelopeKind::ExecResult));
        let parsed: ExecResult = serde_json::from_value(env.data).unwrap();
        assert_eq!(parsed.assignment_id, "assign-1");
    }

    #[test]
    fn test_assignment_serialization() {
        let assignment = ExecAssignment {
            version: "1.0".to_string(),
            assignment_id: "assign-1".to_string(),
            request_id: "req-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            job: Job {
                r#type: "http".to_string(),
                payload: json!({"url": "http://example.com"}),
            },
            trace_id: Some("trace-1".to_string()),
            run_id: None,
            flow_id: None,
            step_id: None,
        };

        let json = serde_json::to_string(&assignment).unwrap();
        let parsed: ExecAssignment = serde_json::from_str(&json).unwrap();
        
        assert_eq!(parsed.assignment_id, "assign-1");
        assert_eq!(parsed.job.r#type, "http");
    }

    #[test]
    fn test_result_serialization() {
        let result = ExecResult {
            version: "1.0".to_string(),
            assignment_id: "assign-1".to_string(),
            request_id: "req-1".to_string(),
            status: ExecStatus::Success,
            provider_id: "worker-1".to_string(),
            job_type: "http".to_string(),
            output: None,
            latency_ms: 100,
            cost: 0.0,
            trace_id: None,
            tenant_id: None,
            run_id: None,
            error_code: None,
            error_message: None,
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("success"));
        
        let parsed: ExecResult = serde_json::from_str(&json).unwrap();
        matches!(parsed.status, ExecStatus::Success);
    }
}

use worker::protocol::{EventEnvelopeV1, EnvelopeKind, ExecAssignment, Job, ExecStatus, map_status_to_task_state, TaskState};
use serde_json::json;

#[test]
fn envelope_roundtrip_assignment() {
    let a = ExecAssignment {
        version: "1.0".into(),
        assignment_id: "a1".into(),
        request_id: "r1".into(),
        tenant_id: "t1".into(),
        job: Job { r#type: "echo".into(), payload: json!({"hello": "world"}) },
        trace_id: Some("tr-1".into()),
        run_id: None,
        flow_id: None,
        step_id: None,
    };
    let env = EventEnvelopeV1::wrap_assignment(&a);
    assert!(matches!(env.kind, EnvelopeKind::ExecAssign));
    let parsed: ExecAssignment = serde_json::from_value(env.data).unwrap();
    assert_eq!(parsed.assignment_id, "a1");
}

#[test]
fn status_to_state_mapping() {
    assert!(matches!(map_status_to_task_state(&ExecStatus::Success), TaskState::Completed));
    assert!(matches!(map_status_to_task_state(&ExecStatus::Error), TaskState::Failed));
    assert!(matches!(map_status_to_task_state(&ExecStatus::Timeout), TaskState::Timeout));
    assert!(matches!(map_status_to_task_state(&ExecStatus::Cancelled), TaskState::Cancelled));
}

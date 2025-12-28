mod config;
mod observability;
mod health;
mod protocol;
mod executor;
mod handlers;
mod error;
mod dlq;

use config::Config;
use observability::{Logger, metrics::Metrics};
use executor::Executor;
use protocol::{ExecAssignment, EventEnvelopeV1, EnvelopeKind, TaskState, DeadLetter, map_status_to_task_state};
use serde_json::json;
use futures::StreamExt;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use tokio::sync::{Semaphore, broadcast};
use tokio::time::sleep;
use std::time::Duration;
use std::collections::{HashSet, VecDeque};
use chrono::Utc;
use error::classify_publish_error;
use dlq::write_deadletter_to_file;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Load Config
    let config = Config::from_env().expect("Failed to load configuration");
    
    // 2. Initialize Logger
    let logger = Logger::new(config.worker_id.clone());
    
    logger.info("Worker starting up", Some(&json!({
        "nats_url": config.nats_url,
        "health_bind": config.health_bind
    })));

    // 3. Start Health Server
    let health_bind = config.health_bind.clone();
    let health_worker_id = config.worker_id.clone();
    let readiness = Arc::new(AtomicBool::new(false));
    let version = env!("CARGO_PKG_VERSION").to_string();
    let metrics = Arc::new(Metrics::new());
    let shutdown = Arc::new(AtomicBool::new(false));
    let readiness_for_health = readiness.clone();
    let metrics_for_health = metrics.clone();
    let shutdown_for_health = shutdown.clone();
    
    tokio::spawn(async move {
        let logger = Logger::new(health_worker_id);
        logger.info(&format!("Health server listening on {}", health_bind), None);
        
        let state = health::HealthState { readiness: readiness_for_health, version, metrics: metrics_for_health, draining: shutdown_for_health.clone(), max_concurrency: config.max_concurrency };
        if let Err(e) = health::start_server(health_bind, state).await {
            logger.error(&format!("Health server crashed: {}", e), None);
            std::process::exit(1);
        }
    });

    // 4. Connect to NATS with exponential backoff
    logger.info(&format!("Connecting to NATS at {}", config.nats_url), None);
    let nc = {
        let mut attempt: u32 = 0;
        loop {
            metrics.nats_connect_attempts.inc();
            match async_nats::connect(&config.nats_url).await {
                Ok(nc) => {
                    logger.info("Connected to NATS", None);
                    metrics.nats_connected.set(1);
                    break nc;
                }
                Err(e) => {
                    readiness.store(false, Ordering::SeqCst);
                    metrics.nats_connected.set(0);
                    let backoff_ms = std::cmp::min(30_000, (500_u64).saturating_mul(2_u64.saturating_pow(attempt)));
                    logger.error("Failed to connect to NATS, will retry", Some(&json!({
                        "error": e.to_string(),
                        "attempt": attempt,
                        "backoff_ms": backoff_ms
                    })));
                    sleep(Duration::from_millis(backoff_ms)).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    };

    // 5. Subscribe to Assignments
    let mut subscription = match nc.subscribe(config.caf_assign_subject.clone()).await {
        Ok(sub) => sub,
        Err(e) => {
            logger.error(&format!("Failed to subscribe to {}: {}", config.caf_assign_subject, e), None);
            return Err(e.into());
        }
    };
    logger.info(&format!("Subscribed to {}", config.caf_assign_subject), None);
    readiness.store(true, Ordering::SeqCst);
    metrics.subs_active.set(1);

    // 6. Prepare Heartbeat (spawned after concurrency setup)
    let heartbeat_nc = nc.clone();
    let heartbeat_subject = config.caf_heartbeat_subject.clone();
    let heartbeat_interval = config.caf_heartbeat_interval_ms;
    let heartbeat_worker_id = config.worker_id.clone();
    let heartbeat_logger = Logger::new(heartbeat_worker_id.clone());

    // 7. Process Assignments
    let assign_logger = Logger::new(config.worker_id.clone());
    let executor = Executor::new(config.worker_id.clone(), config.fs_base_dir.clone());
    let result_producer = nc.clone();
    let result_subject = config.caf_result_subject.clone();
    let semaphore = Arc::new(Semaphore::new(config.max_concurrency));
    let default_timeout_ms = config.default_job_timeout_ms;
    let mut dedup = Dedup::new(4096);
    let metrics_for_loop = metrics.clone();
    let max_concurrency = config.max_concurrency;
    let shutdown_flag = shutdown.clone();
    let semaphore_for_loop = semaphore.clone();
    let nc_for_loop = nc.clone();
    let hb_subject_for_loop = heartbeat_subject.clone();
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let mut shutdown_rx_loop = shutdown_tx.subscribe();

    // Spawn Heartbeat Loop with dynamic load/status
    {
        let heartbeat_semaphore = semaphore.clone();
        let max_permits = config.max_concurrency;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(heartbeat_interval));
            loop {
                interval.tick().await;
                let available = heartbeat_semaphore.available_permits();
                let in_use = max_permits.saturating_sub(available);
                let load = if max_permits == 0 { 0.0 } else { (in_use as f64) / (max_permits as f64) };
                let status = if in_use > 0 { "busy".to_string() } else { "idle".to_string() };
                let hb = protocol::WorkerHeartbeat {
                    worker_id: heartbeat_worker_id.clone(),
                    timestamp: chrono::Utc::now().to_rfc3339(),
                    status,
                    load,
                };
                let env = EventEnvelopeV1::wrap_heartbeat(&hb);
                if let Ok(payload) = serde_json::to_vec(&env) {
                    if let Err(e) = heartbeat_nc.publish(hb_subject_for_loop.clone(), payload.into()).await {
                        heartbeat_logger.error(&format!("Failed to send heartbeat: {}", e), None);
                    }
                }
            }
        });
    }

    let config_loop = config.clone();
    tokio::spawn(async move {
        let config = config_loop;
        loop {
            let msg = tokio::select! {
                _ = shutdown_rx_loop.recv() => {
                    let _ = subscription.unsubscribe().await;
                    break;
                }
                next_msg = subscription.next() => next_msg,
            };

            if let Some(msg) = msg {
             // 1. Parse
             let assignment: ExecAssignment = match serde_json::from_slice::<EventEnvelopeV1>(&msg.payload) {
                 Ok(env) => {
                     match env.kind {
                         EnvelopeKind::ExecAssign => {
                             match serde_json::from_value::<ExecAssignment>(env.data) {
                                 Ok(a) => a,
                                 Err(e) => {
                                     assign_logger.error("Failed to decode envelope data", Some(&json!({"error": e.to_string()})));
                                    let dlq = DeadLetter {
                                        reason: "DECODE_ERROR".to_string(),
                                        payload_ref: json!({"subject": msg.subject, "len": msg.payload.len()}),
                                        ts: Utc::now().to_rfc3339(),
                                    };
                                    let _ = write_deadletter_to_file(&dlq, &config.dlq_path, config.dlq_max_bytes, config.dlq_max_rotations, config.dlq_total_max_bytes, config.dlq_max_age_days);
                                    let _ = result_producer.publish(config.caf_dlq_subject.clone(), serde_json::to_vec(&EventEnvelopeV1 {
                                        version: "v1".to_string(),
                                        kind: EnvelopeKind::DeadLetter,
                                        data: serde_json::to_value(dlq).unwrap(),
                                    }).unwrap().into()).await;
                                    continue;
                                }
                            }
                        }
                        _ => {
                            assign_logger.error("Unexpected envelope kind", Some(&json!({"kind": format!("{:?}", env.kind)})));
                            continue;
                        }
                    }
                }
                Err(_) => {
                    match serde_json::from_slice::<ExecAssignment>(&msg.payload) {
                        Ok(a) => a,
                        Err(e2) => {
                            assign_logger.error("Failed to parse assignment", Some(&json!({
                                "error": e2.to_string(),
                                "subject": msg.subject,
                                "payload_len": msg.payload.len()
                            })));
                             let dlq = DeadLetter {
                                 reason: "PARSE_ERROR".to_string(),
                                 payload_ref: json!({"subject": msg.subject, "len": msg.payload.len()}),
                                 ts: Utc::now().to_rfc3339(),
                             };
                             let _ = write_deadletter_to_file(&dlq, &config.dlq_path, config.dlq_max_bytes, config.dlq_max_rotations, config.dlq_total_max_bytes, config.dlq_max_age_days);
                             metrics_for_loop.dlq_published_total.inc();
                             let _ = result_producer.publish(config.caf_dlq_subject.clone(), serde_json::to_vec(&EventEnvelopeV1 {
                                 version: "v1".to_string(),
                                 kind: EnvelopeKind::DeadLetter,
                                 data: serde_json::to_value(dlq).unwrap(),
                             }).unwrap().into()).await;
                             continue;
                        }
                    }
                }
            };

             // 1a. Dedup at-least-once
             if dedup.contains(&assignment.assignment_id) {
                 assign_logger.info("Duplicate assignment detected, skipping", Some(&json!({
                     "assignment_id": assignment.assignment_id
                 })));
                 continue;
             } else {
                 dedup.insert(assignment.assignment_id.clone());
             }

             assign_logger.info("Task state changed", Some(&json!({
                 "assignment_id": assignment.assignment_id,
                 "trace_id": assignment.trace_id,
                 "state": serde_json::to_string(&TaskState::Queued).unwrap()
             })));

            // Backpressure via semaphore
            let permit = match semaphore_for_loop.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    assign_logger.error("Backpressure: concurrency limit reached", Some(&json!({
                        "max_concurrency": semaphore_for_loop.available_permits()
                    })));
                    // Wait for a permit to avoid dropping messages
                    let p = semaphore_for_loop.clone().acquire_owned().await.unwrap();
                    p
                }
            };
            let in_use_after_acquire = max_concurrency.saturating_sub(semaphore_for_loop.available_permits());
            metrics_for_loop.tasks_in_progress.set(in_use_after_acquire as i64);

             assign_logger.info("Task state changed", Some(&json!({
                 "assignment_id": assignment.assignment_id,
                 "trace_id": assignment.trace_id,
                 "state": serde_json::to_string(&TaskState::Running).unwrap()
             })));

             assign_logger.info("Processing assignment", Some(&json!({
                 "assignment_id": assignment.assignment_id,
                 "trace_id": assignment.trace_id,
                 "job_type": assignment.job.r#type
             })));
            metrics_for_loop.task_received.inc();

            // Prepare clones for spawned task
            let executor = executor.clone();
            let result_producer = result_producer.clone();
            let result_subject = result_subject.clone();
            let config = config.clone();
            let assign_logger = assign_logger.clone();
            let metrics_for_loop = metrics_for_loop.clone();
            let semaphore_for_loop = semaphore_for_loop.clone();
            let assignment = assignment.clone();

            tokio::spawn(async move {
             // 2. Execute
             let timeout_ms = assignment.job.payload.get("timeout_ms")
                 .and_then(|v| v.as_u64())
                 .unwrap_or(default_timeout_ms);
             let exec_fut = executor.execute(assignment.clone());
            let result = match tokio::time::timeout(Duration::from_millis(timeout_ms), exec_fut).await {
                Ok(res) => res,
                Err(_) => protocol::ExecResult {
                     version: "1.0".to_string(),
                     assignment_id: assignment.assignment_id,
                     request_id: assignment.request_id,
                     status: protocol::ExecStatus::Timeout,
                     provider_id: executor.id().to_string(),
                     job_type: assignment.job.r#type,
                     output: None,
                     latency_ms: timeout_ms,
                     cost: 0.0,
                     trace_id: assignment.trace_id,
                     tenant_id: Some(assignment.tenant_id),
                     run_id: assignment.run_id,
                     error_code: Some("TIMEOUT".to_string()),
                     error_message: Some("Task timed out".to_string()),
                 }
             };

             let final_state = map_status_to_task_state(&result.status);
             assign_logger.info("Task state changed", Some(&json!({
                 "assignment_id": result.assignment_id,
                 "trace_id": result.trace_id,
                 "state": serde_json::to_string(&final_state).unwrap()
             })));
            match final_state {
                TaskState::Completed => metrics_for_loop.task_completed.inc(),
                TaskState::Failed => metrics_for_loop.task_failed.inc(),
                TaskState::Timeout => metrics_for_loop.task_timeout.inc(),
                _ => {}
            }
            metrics_for_loop.task_duration_seconds.observe(result.latency_ms as f64 / 1000.0);
            metrics_for_loop.task_duration_seconds.observe(result.latency_ms as f64 / 1000.0);

             // 3. Publish Result
             let envelope = EventEnvelopeV1::wrap_result(&result);
            match serde_json::to_vec(&envelope) {
                Ok(payload) => {
                    let mut attempt = 0_u32;
                    loop {
                        match result_producer.publish(result_subject.clone(), payload.clone().into()).await {
                            Ok(_) => {
                                assign_logger.info("Result published", Some(&json!({
                                    "assignment_id": result.assignment_id,
                                    "trace_id": result.trace_id,
                                    "status": format!("{:?}", result.status),
                                    "latency_ms": result.latency_ms
                                })));
                                break;
                            }
                            Err(e) => {
                                let we = classify_publish_error(&e);
                                if we.is_transient() && attempt < config.result_publish_max_retries {
                                    attempt += 1;
                                    let backoff_ms = std::cmp::min(30_000, (500_u64).saturating_mul(2_u64.saturating_pow(attempt)));
                                    assign_logger.error("Publish transient error, retrying", Some(&json!({
                                        "assignment_id": result.assignment_id,
                                        "trace_id": result.trace_id,
                                        "attempt": attempt,
                                        "error": e.to_string(),
                                        "we_msg": we.message(),
                                        "kind": "transient",
                                        "backoff_ms": backoff_ms
                                    })));
                                    sleep(Duration::from_millis(backoff_ms)).await;
                                    continue;
                                } else {
                                    assign_logger.error("Publish failed, sending to DLQ", Some(&json!({
                                        "assignment_id": result.assignment_id,
                                        "trace_id": result.trace_id,
                                        "error": e.to_string(),
                                        "we_msg": we.message(),
                                        "kind": if we.is_transient() { "transient" } else { "permanent" }
                                    })));
                                    let dlq = DeadLetter {
                                        reason: "PUBLISH_ERROR".to_string(),
                                        payload_ref: json!({"assignment_id": result.assignment_id, "trace_id": result.trace_id}),
                                        ts: Utc::now().to_rfc3339(),
                                    };
                                     let env = EventEnvelopeV1 {
                                         version: "v1".to_string(),
                                         kind: EnvelopeKind::DeadLetter,
                                         data: serde_json::to_value(&dlq).unwrap(),
                                     };
                                     let _ = write_deadletter_to_file(&dlq, &config.dlq_path, config.dlq_max_bytes, config.dlq_max_rotations, config.dlq_total_max_bytes, config.dlq_max_age_days);
                                     metrics_for_loop.dlq_published_total.inc();
                                     let _ = result_producer.publish(config.caf_dlq_subject.clone(), serde_json::to_vec(&env).unwrap().into()).await;
                                     break;
                                 }
                            }
                        }
                    }
                }
                Err(e) => {
                    assign_logger.error("Failed to serialize result", Some(&json!({
                        "assignment_id": result.assignment_id,
                        "trace_id": result.trace_id,
                        "error": e.to_string()
                    })));
                }
            }
            drop(permit);
            let in_use_after_release = max_concurrency.saturating_sub(semaphore_for_loop.available_permits());
            metrics_for_loop.tasks_in_progress.set(in_use_after_release as i64);
            });
            } // End of if let Some(msg)
            
            // Check shutdown before resubscribe logic (if stream ended)
            if shutdown_flag.load(Ordering::SeqCst) {
                 break;
            }

            // Stream ended (None from next()), try to resubscribe
            metrics_for_loop.subs_active.set(0);
            sleep(Duration::from_secs(1)).await;
            
            // Check shutdown again after sleep
            if shutdown_flag.load(Ordering::SeqCst) {
                break;
            }
            match nc_for_loop.subscribe(config.caf_assign_subject.clone()).await {
                Ok(sub) => {
                    subscription = sub;
                    metrics_for_loop.subs_active.set(1);
                    assign_logger.info("Resubscribed after stream end", Some(&json!({"subject": config.caf_assign_subject})));
                }
                Err(e) => {
                    assign_logger.error("Failed to resubscribe", Some(&json!({"error": e.to_string()})));
                }
            }
        }
    });

    // Keep main alive
    tokio::signal::ctrl_c().await?;
    readiness.store(false, Ordering::SeqCst);
    shutdown.store(true, Ordering::SeqCst);
    let _ = shutdown_tx.send(()); // Notify loop to stop
    metrics.subs_active.set(0);
    // Subscription is unsubscribed inside the processing task on shutdown_flag
    // Send intermediate draining heartbeat
    let available = semaphore.available_permits();
    let in_use = max_concurrency.saturating_sub(available);
    let load = if max_concurrency == 0 { 0.0 } else { (in_use as f64) / (max_concurrency as f64) };
    let draining_hb = protocol::WorkerHeartbeat {
        worker_id: config.worker_id.clone(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        status: "draining".to_string(),
        load,
    };
    let env_d = EventEnvelopeV1::wrap_heartbeat(&draining_hb);
    if let Ok(payload) = serde_json::to_vec(&env_d) {
        let _ = nc.publish(heartbeat_subject.clone(), payload.into()).await;
    }
    let _ = semaphore.clone().acquire_many_owned(max_concurrency as u32).await;
    let final_hb = protocol::WorkerHeartbeat {
        worker_id: config.worker_id.clone(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        status: "stopped".to_string(),
        load: 0.0,
    };
    let env = EventEnvelopeV1::wrap_heartbeat(&final_hb);
    if let Ok(payload) = serde_json::to_vec(&env) {
        let _ = nc.publish(heartbeat_subject.clone(), payload.into()).await;
    }
    logger.info("Worker shutdown", None);

    Ok(())
}

struct Dedup {
    set: HashSet<String>,
    queue: VecDeque<String>,
    capacity: usize,
}

impl Dedup {
    fn new(capacity: usize) -> Self {
        Self {
            set: HashSet::new(),
            queue: VecDeque::new(),
            capacity,
        }
    }
    fn insert(&mut self, key: String) {
        if self.set.insert(key.clone()) {
            self.queue.push_back(key);
            if self.queue.len() > self.capacity {
                if let Some(old) = self.queue.pop_front() {
                    self.set.remove(&old);
                }
            }
        }
    }
    fn contains(&self, key: &str) -> bool {
        self.set.contains(key)
    }
}

#[cfg(test)]
mod main_tests {
    use super::*;

    #[test]
    fn test_dedup_basic() {
        let mut d = Dedup::new(2);
        d.insert("a".to_string());
        assert!(d.contains("a"));
        d.insert("b".to_string());
        assert!(d.contains("b"));
        d.insert("c".to_string()); // evicts "a"
        assert!(!d.contains("a"));
        assert!(d.contains("b"));
        assert!(d.contains("c"));
    }
}

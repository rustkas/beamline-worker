use axum::{routing::get, Router, extract::State, http::StatusCode};
use tokio::net::TcpListener;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use crate::observability::metrics::Metrics;
use serde_json::json;

#[derive(Clone)]
pub struct HealthState {
    pub readiness: Arc<AtomicBool>,
    pub version: String,
    pub metrics: Arc<Metrics>,
    pub draining: Arc<AtomicBool>,
    pub max_concurrency: usize,
}

pub async fn start_server(bind_addr: String, state: HealthState) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = Router::new()
        .route("/_health", get(health_handler))
        .route("/readyz", get(ready_handler))
        .route("/metrics", get(metrics_handler))
        .route("/_build", get(build_handler))
        .route("/_state", get(state_handler))
        .with_state(state);
    
    let listener = TcpListener::bind(&bind_addr).await?;
    
    axum::serve(listener, app).await?;
    
    Ok(())
}

async fn health_handler() -> &'static str {
    "OK"
}

async fn ready_handler(State(state): State<HealthState>) -> (StatusCode, &'static str) {
    let draining = state.draining.load(Ordering::SeqCst);
    if draining {
        (StatusCode::SERVICE_UNAVAILABLE, "DRAINING")
    } else if state.readiness.load(Ordering::SeqCst) {
        (StatusCode::OK, "READY")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "NOT_READY")
    }
}

async fn build_handler(State(state): State<HealthState>) -> String {
    state.version.clone()
}

async fn metrics_handler(State(state): State<HealthState>) -> (StatusCode, String) {
    let data = state.metrics.encode();
    (StatusCode::OK, String::from_utf8_lossy(&data).to_string())
}

async fn state_handler(State(state): State<HealthState>) -> (StatusCode, String) {
    let draining = state.draining.load(Ordering::SeqCst);
    let ready = state.readiness.load(Ordering::SeqCst) && !draining;
    let running = state.metrics.tasks_in_progress.get() as f64;
    let max = state.max_concurrency as f64;
    let load = if max == 0.0 { 0.0 } else { (running / max).clamp(0.0, 1.0) };
    let body = json!({
        "ready": ready,
        "draining": draining,
        "load": load,
    }).to_string();
    let code = if ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (code, body)
}

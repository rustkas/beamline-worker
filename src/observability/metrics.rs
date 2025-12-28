use prometheus::{
    Encoder, Histogram, IntCounter, IntGauge, Registry, TextEncoder,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct Metrics {
    pub registry: Arc<Registry>,
    pub nats_connect_attempts: IntCounter,
    pub nats_connected: IntGauge,
    pub subs_active: IntGauge,
    pub task_received: IntCounter,
    pub task_completed: IntCounter,
    pub task_failed: IntCounter,
    pub task_timeout: IntCounter,
    pub tasks_in_progress: IntGauge,
    pub dlq_published_total: IntCounter,
    pub task_duration_seconds: Histogram,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Arc::new(Registry::new());

        let nats_connect_attempts = IntCounter::new("nats_connect_attempts", "Total NATS connect attempts").unwrap();
        let nats_connected = IntGauge::new("nats_connected", "NATS connected gauge 1/0").unwrap();
        let subs_active = IntGauge::new("subs_active", "Active subscriptions count").unwrap();
        let task_received = IntCounter::new("task_received", "Tasks received").unwrap();
        let task_completed = IntCounter::new("task_completed", "Tasks completed").unwrap();
        let task_failed = IntCounter::new("task_failed", "Tasks failed").unwrap();
        let task_timeout = IntCounter::new("task_timeout", "Tasks timed out").unwrap();
        let tasks_in_progress = IntGauge::new("tasks_in_progress", "Currently running tasks").unwrap();
        let dlq_published_total = IntCounter::new("dlq_published_total", "Deadletters published total").unwrap();
        let task_duration_seconds = Histogram::with_opts(
            prometheus::HistogramOpts::new("task_duration_seconds", "Task execution duration in seconds")
        ).unwrap();

        registry.register(Box::new(nats_connect_attempts.clone())).unwrap();
        registry.register(Box::new(nats_connected.clone())).unwrap();
        registry.register(Box::new(subs_active.clone())).unwrap();
        registry.register(Box::new(task_received.clone())).unwrap();
        registry.register(Box::new(task_completed.clone())).unwrap();
        registry.register(Box::new(task_failed.clone())).unwrap();
        registry.register(Box::new(task_timeout.clone())).unwrap();
        registry.register(Box::new(tasks_in_progress.clone())).unwrap();
        registry.register(Box::new(dlq_published_total.clone())).unwrap();
        registry.register(Box::new(task_duration_seconds.clone())).unwrap();

        Self {
            registry,
            nats_connect_attempts,
            nats_connected,
            subs_active,
            task_received,
            task_completed,
            task_failed,
            task_timeout,
            tasks_in_progress,
            dlq_published_total,
            task_duration_seconds,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        encoder.encode(&metric_families, &mut buffer).unwrap_or(());
        buffer
    }
}

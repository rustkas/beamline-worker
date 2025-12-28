# Beamline Worker

A high-performance, modular, and secure Rust-based execution runtime for the Beamline Scheduler. This worker consumes job assignments from NATS, executes them using specialized handlers, and reports results back to the scheduler.

## Features

- **Protocol**: NATS-based async communication (Assign, Result, Heartbeat, Dead Letter Queue).
- **Modular Handlers**:
  - **Common**: `echo`, `sleep` (for testing and debugging).
  - **HTTP**: RESTful requests with exponential backoff retries and GraphQL support.
  - **Scripting**: Embedded JavaScript execution (via [Boa](https://github.com/boa-dev/boa)) and JMESPath JSON transformations.
  - **Database**: High-performance PostgreSQL query execution with connection pooling (`sqlx`).
  - **File System**: Secure Blob Get/Put operations with path traversal protection.
  - **Human Interaction**: Workflow hooks for human approval steps.
- **Observability**:
  - Prometheus metrics endpoint (`/metrics`).
  - Structured JSON logging.
  - Health and Readiness probes.
- **Reliability**:
  - Graceful shutdown handling (drains active tasks).
  - Local Dead Letter Queue (DLQ) with automatic rotation and retention policies.
  - Concurrency control via semaphores.

## Architecture

The worker follows an actor-like model where:
1.  It subscribes to a NATS subject (`CAF_ASSIGN_SUBJECT`).
2.  Incoming messages are validated and deserialized into `ExecAssignment`.
3.  The `Executor` dispatches the job to the appropriate handler module based on `job.type`.
4.  Results (Success/Failure) are published back to `CAF_RESULT_SUBJECT`.
5.  Heartbeats are periodically sent to `CAF_HEARTBEAT_SUBJECT` to report worker status and load.

## Configuration

The worker is configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NATS_URL` | `nats://localhost:4222` | NATS server URL. |
| `WORKER_ID` | `worker-<uuid>` | Unique identifier for this worker instance. |
| `WORKER_MAX_CONCURRENCY` | `8` | Maximum number of concurrent jobs processing. |
| `FS_BASE_DIR` | `/tmp/worker-storage` | Root directory for file system operations (sandboxed). |
| `CAF_ASSIGN_SUBJECT` | `caf.exec.assign.v1` | NATS subject to subscribe for new jobs. |
| `CAF_RESULT_SUBJECT` | `caf.exec.result.v1` | NATS subject to publish execution results. |
| `CAF_HEARTBEAT_SUBJECT` | `caf.status.heartbeat.v1` | NATS subject for heartbeat pulses. |
| `CAF_DLQ_SUBJECT` | `caf.deadletter.v1` | NATS subject for dead-letter notifications. |
| `CAF_HEARTBEAT_INTERVAL_MS` | `5000` | Interval between heartbeats in milliseconds. |
| `DEFAULT_JOB_TIMEOUT_MS` | `60000` | Default timeout for job execution. |
| `HEALTH_BIND` | `0.0.0.0:9091` | Address/Port for Health/Metrics HTTP server. |
| `DLQ_PATH` | `/tmp/worker-dlq.jsonl` | Path for local DLQ storage. |
| `DLQ_MAX_BYTES` | `100MB` | Max size of a single DLQ file before rotation. |
| `DLQ_TOTAL_MAX_BYTES` | `1GB` | Total max size of all DLQ files. |
| `DLQ_MAX_AGE_DAYS` | `None` | Max age of DLQ files in days. |
| `RESULT_PUBLISH_MAX_RETRIES`| `5` | Max retries for publishing results to NATS. |

## Development

### Prerequisites

- Rust (latest stable)
- NATS Server (for integration testing)
- PostgreSQL (optional, for SQL handler testing)

### Running Tests

```bash
cargo test
```

### Building

```bash
cargo build --release
```

### Running

```bash
export NATS_URL=nats://localhost:4222
cargo run
```

## Project Structure

- `src/main.rs`: Application entry point, NATS loop, and Health server.
- `src/executor.rs`: Job dispatch logic.
- `src/handlers/`: Modular job implementations.
  - `common.rs`, `http.rs`, `script.rs`, `sql.rs`, `fs.rs`, `human.rs`
- `src/protocol.rs`: Data structures for CAF protocol (Assign, Result, etc.).
- `src/config.rs`: Configuration loading and validation.
- `src/dlq.rs`: Dead Letter Queue management.
- `src/observability/`: Metrics and Logging.

## License

Apache License 2.0

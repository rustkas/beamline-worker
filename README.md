# Beamline Worker

> High-performance Rust-based execution runtime for the Beamline Scheduler

[![Rust](https://img.shields.io/badge/Rust-1.70+-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![NATS](https://img.shields.io/badge/NATS-powered-blue.svg)](https://nats.io/)

## 📋 Overview

Beamline Worker is a modular, secure, and highly performant job execution runtime written in Rust. It consumes job assignments from NATS, executes them using specialized handlers, and reports results back to the scheduler with comprehensive observability and reliability features.

## ✨ Features

### Core Capabilities
- 🚀 **High Performance**: Async Rust with Tokio runtime
- 📡 **NATS Protocol**: Async communication (Assign, Result, Heartbeat, DLQ)
- 🔄 **Concurrency Control**: Semaphore-based job throttling
- 📊 **Full Observability**: Prometheus metrics, structured JSON logging, health probes
- 🛡️ **Reliability**: Graceful shutdown, local DLQ with rotation, retry mechanisms

### Modular Handlers

#### Common Handlers
- **Echo** - Simple echo for testing
- **Sleep** - Delay execution for debugging

#### HTTP Handler
- RESTful requests with exponential backoff
- GraphQL support
- Automatic retries with configurable strategies
- Request/response transformation

#### Scripting Handler
- **JavaScript**: Embedded execution via [Boa Engine](https://github.com/boa-dev/boa)
- **JMESPath**: JSON transformations
- Sandboxed execution environment

#### Database Handler
- **PostgreSQL**: High-performance query execution
- Connection pooling with `sqlx`
- Prepared statements and parameter binding
- Transaction support

#### File System Handler
- Secure Blob Get/Put operations
- Path traversal protection
- Configurable base directory sandboxing
- Automatic cleanup mechanisms

#### Human Interaction Handler
- Workflow approval hooks
- Timeout handling
- Callback integration

## 🏗️ Architecture

The worker follows an actor-like model:

```
┌─────────────┐
│    NATS     │
│  Message    │
│   Broker    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Subscribe  │
│  CAF_ASSIGN │
│  _SUBJECT   │
└──────┬──────┘
       │
       ▼
┌─────────────┐      ┌──────────────┐
│  Validate & │─────▶│   Executor   │
│ Deserialize │      │   Dispatch   │
└─────────────┘      └───────┬──────┘
                            │
       ┌────────────────────┼────────────────────┐
       ▼                    ▼                    ▼
  ┌─────────┐         ┌─────────┐         ┌─────────┐
  │  HTTP   │         │ Script  │   ...   │   FS    │
  │ Handler │         │ Handler │         │ Handler │
  └────┬────┘         └────┬────┘         └────┬────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                          ▼
                   ┌─────────────┐
                   │   Publish   │
                   │  CAF_RESULT │
                   │  _SUBJECT   │
                   └─────────────┘
```

**Flow:**
1. Subscribe to `CAF_ASSIGN_SUBJECT` for new jobs
2. Validate and deserialize incoming `ExecAssignment`
3. Dispatch job to appropriate handler based on `job.type`
4. Execute job with timeout and retry mechanisms
5. Publish result (Success/Failure) to `CAF_RESULT_SUBJECT`
6. Send periodic heartbeats to `CAF_HEARTBEAT_SUBJECT`
7. Failed jobs go to Dead Letter Queue

## 🚀 Quick Start

### Prerequisites

- Rust 1.70 or higher
- NATS Server (for runtime)
- PostgreSQL (optional, for SQL handler testing)

### Installation

```bash
# Clone repository
git clone https://github.com/rustkas/beamline-worker.git
cd beamline-worker

# Build
cargo build --release

# Run tests
cargo test

# Run worker
export NATS_URL=nats://localhost:4222
cargo run --release
```

## ⚙️ Configuration

Configure via environment variables:

### Essential Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `WORKER_ID` | `worker-<uuid>` | Unique identifier for this worker instance |
| `WORKER_MAX_CONCURRENCY` | `8` | Maximum number of concurrent jobs |

### NATS Subjects

| Variable | Default | Description |
|----------|---------|-------------|
| `CAF_ASSIGN_SUBJECT` | `caf.exec.assign.v1` | Subject to subscribe for new jobs |
| `CAF_RESULT_SUBJECT` | `caf.exec.result.v1` | Subject to publish execution results |
| `CAF_HEARTBEAT_SUBJECT` | `caf.status.heartbeat.v1` | Subject for heartbeat pulses |
| `CAF_DLQ_SUBJECT` | `caf.deadletter.v1` | Subject for dead-letter notifications |

### Handler-Specific Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FS_BASE_DIR` | `/tmp/worker-storage` | Root directory for file system operations |
| `DEFAULT_JOB_TIMEOUT_MS` | `60000` | Default timeout for job execution |

### Observability

| Variable | Default | Description |
|----------|---------|-------------|
| `HEALTH_BIND` | `0.0.0.0:9091` | Address/Port for Health/Metrics HTTP server |
| `CAF_HEARTBEAT_INTERVAL_MS` | `5000` | Interval between heartbeats (ms) |

### Dead Letter Queue

| Variable | Default | Description |
|----------|---------|-------------|
| `DLQ_PATH` | `/tmp/worker-dlq.jsonl` | Path for local DLQ storage |
| `DLQ_MAX_BYTES` | `100MB` | Max size of single DLQ file before rotation |
| `DLQ_TOTAL_MAX_BYTES` | `1GB` | Total max size of all DLQ files |
| `DLQ_MAX_AGE_DAYS` | `None` | Max age of DLQ files in days |
| `RESULT_PUBLISH_MAX_RETRIES` | `5` | Max retries for publishing results to NATS |

## 📦 Project Structure

```
worker/
├── src/
│   ├── main.rs           # Application entry point, NATS loop, Health server
│   ├── executor.rs       # Job dispatch logic
│   ├── protocol.rs       # CAF protocol data structures
│   ├── config.rs         # Configuration loading and validation
│   ├── dlq.rs           # Dead Letter Queue management
│   ├── handlers/         # Modular job implementations
│   │   ├── common.rs    # Echo, Sleep handlers
│   │   ├── http.rs      # HTTP/GraphQL handler
│   │   ├── script.rs    # JavaScript/JMESPath handler
│   │   ├── sql.rs       # PostgreSQL handler
│   │   ├── fs.rs        # File System handler
│   │   └── human.rs     # Human interaction handler
│   └── observability/    # Metrics and logging
│       ├── metrics.rs   # Prometheus metrics
│       └── logging.rs   # Structured JSON logging
├── tests/               # Integration tests
├── Cargo.toml          # Dependencies
├── Cargo.lock          # Dependency lock file
└── README.md           # This file
```

## 🧪 Testing

### Unit Tests

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_echo_handler

# Run with output
cargo test -- --nocapture
```

### Integration Tests

```bash
# Run integration tests (requires NATS)
cargo test --test '*' -- --ignored
```

### Test Coverage

```bash
# Install tarpaulin
cargo install cargo-tarpaulin

# Generate coverage
cargo tarpaulin --out Html
```

## 📊 Observability

### Metrics (Prometheus)

**Endpoint:** `http://localhost:9091/metrics`

**Available Metrics:**
- `worker_jobs_total` - Total jobs processed by type and status
- `worker_job_duration_seconds` - Job execution duration histogram
- `worker_active_jobs` - Current number of active jobs
- `worker_dlq_writes_total` - Total writes to Dead Letter Queue
- `worker_heartbeats_sent_total` - Total heartbeats sent

### Health Probes

**Health Check:** `GET http://localhost:9091/health`
```json
{
  "status": "ok",
  "worker_id": "worker-123",
  "active_jobs": 3,
  "uptime_seconds": 12345
}
```

**Readiness Check:** `GET http://localhost:9091/ready`

### Logs (JSON)

Structured JSON logs with correlation IDs:

```json
{
  "timestamp": "2025-12-29T07:56:00Z",
  "level": "INFO",
  "worker_id": "worker-abc123",
  "job_id": "job-456",
  "message": "Job completed successfully",
  "duration_ms": 150,
  "job_type": "http.request"
}
```

## 🛡️ Security

- **Path Traversal Protection**: File system operations are sandboxed to `FS_BASE_DIR`
- **Input Validation**: All job parameters are validated before execution
- **Timeout Enforcement**: Prevents runaway jobs
- **Resource Limits**: Configurable concurrency limits
- **TLS Support**: NATS connections support TLS

## 🔄 Dead Letter Queue

Failed jobs are automatically written to the local DLQ with:
- **Rotation**: Automatic file rotation when size limits are reached
- **Retention**: Configurable max age and total size
- **Format**: JSONL for easy parsing
- **Recovery**: Manual or automated replay mechanisms

## 🚢 Deployment

### Docker

```dockerfile
FROM rust:1.70 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /app/target/release/worker /usr/local/bin/
CMD ["worker"]
```

### Systemd Service

```ini
[Unit]
Description=Beamline Worker
After=network.target

[Service]
Type=simple
User=worker
Environment="NATS_URL=nats://localhost:4222"
Environment="WORKER_ID=worker-prod-01"
ExecStart=/usr/local/bin/worker
Restart=always

[Install]
WantedBy=multi-user.target
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: beamline-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: beamline-worker
  template:
    metadata:
      labels:
        app: beamline-worker
    spec:
      containers:
      - name: worker
        image: beamline-worker:latest
        env:
        - name: NATS_URL
          value: "nats://nats-service:4222"
        - name: WORKER_MAX_CONCURRENCY
          value: "16"
        ports:
        - containerPort: 9091
          name: metrics
```

## 🛠️ Development

### Code Style

```bash
# Format code
cargo fmt

# Run clippy
cargo clippy -- -D warnings
```

### Building for Different Targets

```bash
# Linux
cargo build --release --target x86_64-unknown-linux-gnu

# macOS
cargo build --release --target x86_64-apple-darwin

# Windows
cargo build --release --target x86_64-pc-windows-msvc
```

## 🔗 Related Projects

- **[Beamline Platform](https://github.com/YOUR_ORG/beamline)** - Main platform repository
- **[Beamline Router](../otp/router)** - Erlang/OTP routing and orchestration
- **[C-Gateway](https://github.com/rustkas/beamline-c-gateway)** - High-performance C gateway
- **[CAF Components](https://github.com/rustkas/beamline-caf)** - C++ Actor Framework components

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/rustkas/beamline-worker/issues)
- **Documentation**: [docs/](docs/)

---

**Built with ⚡ Rust and NATS**

# Project: Worker (CP1 Compute Node)

## Goal
Provide a high-performance compute node for executing CP1 tasks (HTTP, FS, SQL, Human Approval).
This component acts as a bridge between the NATS transport layer and the internal execution logic (CAF/Rust).

## Boundaries
- **Input**: NATS messages (`caf.exec.assign.v1`)
- **Output**: NATS messages (`caf.exec.result.v1`)
- **State**: Stateless (except for current execution context)
- **Observability**: CP1 JSONL Logging, Health Endpoint

## Contracts
- **NATS Transport**: Follows `TECHSPEC_CAF_NATS_TRANSPORT.md`
- **Observability**: Follows `CP1_CORE_PROFILE_OBSERVABILITY.md`

## Architecture
See `architecture.md`.

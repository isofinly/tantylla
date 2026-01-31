# AGENTS.md

## Project Overview

This is **Tantylla**, a distributed search system built in Rust. It consists of:

- **node**: gRPC search nodes using Tantivy (Lucene-like search engine)
- **ingestor**: CDC (Change Data Capture) consumer from ScyllaDB
- **gateway**: HTTP API gateway aggregating search results
- **common**: Shared protobuf definitions and utilities

## Build Commands

Make sure your path is set up correctly and you are in the `rust/` directory from the root of the repository.

```bash
cargo build
cargo build --release

cargo build -p tantylla-node
cargo build -p tantylla-ingestor
cargo build -p tantylla-gateway
cargo build -p tantylla-common

cargo check
cargo check --all-targets
```

## Test Commands

```bash
cargo test

cargo test -p tantylla-node
cargo test -p tantylla-ingestor
cargo test -p tantylla-gateway
cargo test -p tantylla-common

cargo test test_name_here
cargo test -p tantylla-node test_name_here

cargo test -- --nocapture

cargo test -- --ignored
```

## Lint Commands

```bash
cargo clippy --all-targets --all-features -- -D warnings

cargo clippy -p tantylla-node -- -D warnings

cargo fmt -- --check

cargo fmt

cargo deny check
```

### Formatting

- Use `cargo fmt` with default settings
- Max line length: 100 characters (soft limit)
- Use 4 spaces for indentation
- Trailing commas in multi-line structs/enums

### Naming Conventions

- **Types**: PascalCase (`IndexService`, `BatchItem`)
- **Functions/Variables**: snake_case (`index_batch`, `processed_count`)
- **Constants**: SCREAMING_SNAKE_CASE (`MAX_BATCH_SIZE`)
- **Modules**: snake_case (`mod batch_service`)
- **Generic parameters**: Single uppercase letter (`T`, `K`, `V`)
- **Acronyms**: Treat as words (`HttpClient`, not `HTTPClient`)

### Types and Error Handling

- Use `anyhow::Result` for application-level errors
- Use `thiserror` for library error types (if needed)
- Propagate errors with `?` operator
- Use `expect()` only for unrecoverable programmer errors
- Do not `unwrap()`, prefer `?` or proper error handling

Example:

```rust
use anyhow::{Result, anyhow};

async fn process_data() -> Result<()> {
    let session = SessionBuilder::new()
        .known_nodes_addr(&uris)
        .build()
        .await?;  // Propagate error

    let value = data.get("key")
        .ok_or_else(|| anyhow!("Missing required key"))?;

    Ok(())
}
```

### Async/Await

- Use `tokio` as the async runtime
- Mark async functions with `async fn`
- Use `#[tokio::main]` for entry points
- Prefer `tokio::select!` for cancellation/shutdown
- Use `tokio::sync` primitives (Mutex, RwLock, channels) only when necessary

## Architecture Patterns

**A. The Ingestor (Stateless CDC Router)**

- **Role:** Connects to ScyllaDB, consumes the CDC log, parses the binary rows, and determines _where_ this data should live.
- **Logic:** It does **not** write to disk. It calculates `hash(PartitionKey) % N_Search_Nodes` and sends a gRPC `IndexRequest` to the calculated target.
- **Scaling:** CPU-bound. You can run 1 or 50 of these. They don't store state. If one crashes, Scylla CDC's native checkpointing handles the resume.

**B. The Search Node (Dumb Storage)**

- **Role:** A simple gRPC server wrapping Tantivy.
- **Logic:** Receives `IndexRequest`. Writes to local Tantivy index. Commits periodically.
- **State:** Holds the data on disk. It knows nothing about Scylla or CDC.

**C. The Gateway (Unified Search)**

- **Role:** Scatters queries to all Search Nodes and gathers results.

### Module Structure

```
crate/
├── src/
│   ├── main.rs          # Entry point with CLI args
│   ├── lib.rs           # Library exports (if applicable)
│   ├── service/         # Business logic
│   │   ├── mod.rs
│   │   └── core.rs
│   ├── engine/          # Core algorithms
│   └── batch/           # Data processing
└── Cargo.toml
```

### Key Dependencies

- **Async**: `tokio`, `async-trait`
- **RPC**: `tonic`, `prost` (gRPC)
- **Serialization**: `serde`, `serde_json`
- **CLI**: `clap`
- **Logging**: `tracing`, `tracing-subscriber`
- **Search**: `tantivy`
- **Database**: `scylla`, `scylla-cdc`
- **Collections**: `ahash` (fast HashMap)

## CI/Quality Checks

Before submitting code:

```bash
cargo fmt -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo deny check
```

## MSRV

Minimum Supported Rust Version: **1.90** (defined in `clippy.toml`)

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

## Rust guidelines

- When adding dependencies to Rust projects, use `cargo add`.
- In code that uses `eyre` or `anyhow` `Result`s, consistently use `.context()` prior to every error-propagation with `?`. Context messages in `.context` should be simple present tense, such as to complete the sentence "while attempting to ...".
- Prefer `expect()` over `unwrap()`. The `expect` message should be very concise, and should explain why that expect call cannot fail.
- When designing `pub` or crate-wide Rust APIs, consult the checklist in <https://rust-lang.github.io/api-guidelines/checklist.html>.

### Useful Rust frameworks for testing

- **`quickcheck`**: Property-based testing for when you have an obviously-correct comparison you can test against.
- **`insta`**: Snapshot testing for regression prevention. Use `cargo insta test` as a stand-in for `cargo test` to run the snapshot tests.

### Writing compile_fail Tests

Use `compile_fail` doctests to verify when certain code should _not_ compile, such as for type-state patterns or trait-based enforcement. Each `compile_fail` test should target a specific error condition since the doctest only has a binary output of whether it fails to compile, not the many reasons _why_. Make sure you clearly explain exactly WHY the code should fail to compile.

If there is no obvious item to add the doctest to, create a new private item with `#[allow(dead_code)]` that you add the compile-fail tests to. Document that that's its purpose.

Before committing, create a temporary example file for each compile-fail test and check the output of `cargo run --example <name>` to ensure it fails for the correct reason. Remove the temporary example after.

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

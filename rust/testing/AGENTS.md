## Project Overview (Testing)

This repository contains a CDC-based FTS system (Tantylla) with integration
tests living under `rust/testing`. The testing harness is designed to
spin up the Tantylla services as child processes and connect to a shared
ScyllaDB instance. Each test creates a unique keyspace to keep runs isolated
and deterministic.

Key directories and files:

- `rust/testing/src/cluster/mod.rs`: TestCluster orchestrator, topology
  configuration, keyspace setup, and process spawning.
- `rust/testing/src/process/mod.rs`: Process lifecycle wrapper
  (spawn/terminate) with captured logs for failure analysis. Services are run
  with `current_dir` set to the workspace root so checkpoint and index files land
  in consistent locations.
- `rust/testing/src/trace/mod.rs`: Trace collector skeleton for
  debug-only instrumentation events across services, plus ergonomic helpers for
  waiting on event names and sources.

Current test harness assumptions:

- ScyllaDB is a shared local instance (default `127.0.0.1:9042`).
- Each test creates and drops its own keyspace for isolation.
- Services are spawned as binaries (`tantylla-node`, `tantylla-ingestor`,
  `tantylla-gateway`) with dynamic ports.
- The harness is built to be extended to full infra-level topology tests later
  (e.g., multi-node Scylla in containers).
- Checkpoint files are stored in the workspace root as
  `{keyspace}-{table}.checkpoint` and are cleaned up when `TestCluster::shutdown`
  completes.
- Node index directories are stored in the workspace root as `index-<port>` and
  are cleaned up during shutdown.

Instrumentation expectations:

- Services emit structured events (JSON) to a test-only UDP port for
  full-stack failure assertions.
- The implementation uses `tracing` events with target `test_event` and a
  dedicated test-only `tracing` layer that forwards JSON over UDP.
- CLI flags like `--test-event-port` are wired into node/ingestor/gateway.

## Testing Conventions

- Prefer deterministic setup and teardown. Always use unique keyspaces.
- For CDC-related tests, keep polling intervals short in debug builds to speed
  feedback (the harness already uses short intervals for ingestor args).
- When adding failure tests, assert the full trace of events collected by
  `TraceCollector`.
- For trace assertions, prefer `TraceCollector::wait_for_event_name` and
  `TraceCollector::wait_for_event_from_source` to avoid custom predicate
  closures.
- When validating CDC ingestion, assert that a checkpoint is committed via
  `TestCluster::wait_for_checkpoint`.

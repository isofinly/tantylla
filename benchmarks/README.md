# Benchmarking

## Setup

```sh
uv sync
uv run bench --help
```

### Kafka Connect image (competitor stack only)

```sh
docker build -t kafka-connect-bench:competitor ./docker/kafka-connect
```

### Infrastructure

```sh
tofu init
```

## Running benchmarks

### Full lifecycle (recommended)

Runs N complete apply → seed → benchmark → destroy cycles:

```sh
uv run bench run competitor
uv run bench run tantylla-multi
uv run bench run tantylla-single

uv run bench run tantylla-multi --runs 5 --output-dir data/output
```

### Individual steps

```sh
uv run bench infra apply competitor
uv run bench infra outputs
uv run bench infra destroy

uv run bench seed --host localhost --port 9043 --count 100000

uv run bench ingest --tantylla-url http://localhost:8080
uv run bench ingest --elasticsearch-url http://localhost:9200

uv run bench search --tantylla-url http://localhost:8080
uv run bench search --elasticsearch-url http://localhost:9200 --queries 1000 --concurrency 10
```

## Notes

- CDC processing speed is governed by `tantylla_safety_interval_ms` and
  `tantylla_sleep_interval_ms` in the workspace tfvars (defaults: 30 s / 10 s).
- The `head-to-head` workspace can be managed via `bench infra apply head-to-head`
  but has no automated `bench run` scenario.
- DuckDB analysis queries live in `scripts/sql/` and are run from the
  `benchmarks/` directory:
  ```sh
  duckdb -c ".read scripts/sql/analyze-results.sql"
  duckdb -c ".read scripts/sql/search-latency.sql"
  duckdb -c ".read scripts/sql/cpu-mem.sql"
  ```

# Benchmarking

## Usage

### Working with Kafka

To run kafka benchmarks, you have to build kafka-connect: `docker build -t kafka-connect-bench:competitor ./benchmarks/docker/kafka-connect`

After that you have to set up the connectors: `bash benchmarks/scripts/setup-connectors.sh <connect_host:port> <scylla_container:port> <es_container:port>`

### Working with Tantylla

The complete stack will be automatically built locally using `Dockerfile.tantylla`.

**Important**: CDC will be processed based on `tantylla_safety_interval_ms` and `tantylla_sleep_interval_ms` defined in tfvars. If none specified, 30s and 10s values will be used as defaults.

### Infrastructure

1. `tofu init`
2. `export WORKSPACE_NAME=competitor | head-to-head | tantylla-multi | tantylla-single`
3. `tofu workspace new ${WORKSPACE_NAME}`
4. Override needed variables in `workspaces/${WORKSPACE_NAME}.tfvars`
5. `tofu apply -var-file=workspaces/${WORKSPACE_NAME}.tfvars`

### Running benchmarks

1. Create venv via `uv venv`
2. Install dependencies from `requirements.txt`
3. Seed benchmark data: `python3 scripts/seed-data.py --host localhost --port 9043 --count 100000`
4. Define urls: `export TANTYLLA_URL=` and `export ELASTICSEARCH_URL=`
5. Run benchmarks: `python3 scripts/run-benchmark.py --tantylla-url ${TANTYLLA_URL} --elasticsearch-url ${ELASTICSEARCH_URL}`. You can adjust these params: `queries`, `concurrency`, `throughput-duration`, `limit`, `warmup`, `seed`.

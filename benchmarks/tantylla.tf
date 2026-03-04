# =========================================================================
# Tantylla FTS Stack
# =========================================================================
#
# Conditional on var.enable_tantylla.
#
# Components:
#   1. Docker image — built locally from Dockerfile.tantylla
#   2. Search nodes — N gRPC Tantivy shards (hash-partitioned by PK)
#   3. Ingestor    — stateless CDC router, reads ScyllaDB CDC log
#   4. Gateway     — HTTP API, scatter-gather across all nodes
#
# The index data lives on Docker volumes so it persists across container
# restarts within the same workspace but can be cleaned with
# `tofu destroy`.

# =========================================================================
# Image: Build from local Rust workspace
# =========================================================================

resource "docker_image" "tantylla" {
  count = var.enable_tantylla ? 1 : 0
  name  = "tantylla:${terraform.workspace}"

  build {
    context    = "${path.module}/.."
    dockerfile = "benchmarks/Dockerfile.tantylla"
    tag        = ["tantylla:${terraform.workspace}"]
  }
}

# =========================================================================
# Node Volumes: One per search node for Tantivy index data
# =========================================================================

resource "docker_volume" "tantylla_node_data" {
  for_each = local.tantylla_nodes
  name     = "${each.value.name}-data"
}

# =========================================================================
# Search Nodes
# =========================================================================
# Each node runs tantylla-node with:
#   --address 0.0.0.0  (bind to all interfaces for inter-container comms)
#   --port 10000       (same internal port; host port varies by index)
#   --commit-interval-secs N
#
# The index is stored at /data/index-10000 inside the container.

resource "docker_container" "tantylla_node" {
  for_each = local.tantylla_nodes

  name  = each.value.name
  image = docker_image.tantylla[0].image_id

  entrypoint = ["tantylla-node"]
  command = [
    "--address", "0.0.0.0",
    "--port", tostring(local.tantylla_node_internal_port),
    "--commit-interval-secs", tostring(var.tantylla_commit_interval_secs),
  ]

  ports {
    internal = local.tantylla_node_internal_port
    external = each.value.host_port
  }

  volumes {
    volume_name    = docker_volume.tantylla_node_data[each.key].name
    container_path = "/data"
  }

  networks_advanced {
    name = docker_network.benchmark.id
  }

  env = [
    "RUST_LOG=info",
  ]

  memory = var.tantylla_node_memory_mb * 1024 * 1024

  restart = "unless-stopped"
}

# =========================================================================
# Ingestor Volume: Persists CDC checkpoint files
# =========================================================================

resource "docker_volume" "tantylla_ingestor_data" {
  count = var.enable_tantylla ? 1 : 0
  name  = "${local.tantylla_ingestor_container_name}-data"
}

# =========================================================================
# Ingestor
# =========================================================================
# Stateless CDC router. Connects to ScyllaDB, consumes CDC log for the
# benchmark.products table, hashes partition keys to determine target
# node, and sends gRPC IndexBatch requests.
#
# CLI flags:
#   --scylla-uri        ScyllaDB contact point inside the Docker network
#   --table-name        keyspace.table to consume CDC from
#   --search-nodes      comma-separated list of node addresses
#   --safety-interval   how far behind wall-clock CDC reader stays (ms)
#   --sleep-interval    poll interval when no new data (ms)

resource "docker_container" "tantylla_ingestor" {
  count = var.enable_tantylla ? 1 : 0

  name  = local.tantylla_ingestor_container_name
  image = docker_image.tantylla[0].image_id

  entrypoint = ["tantylla-ingestor"]
  command = [
    "--scylla-uri", "${local.scylla_container_name}:${local.scylla_internal_port}",
    "--table-name", "benchmark.products",
    "--search-nodes", local.tantylla_ingestor_node_addrs,
    "--safety-interval", tostring(var.tantylla_safety_interval_ms),
    "--sleep-interval", tostring(var.tantylla_sleep_interval_ms),
  ]

  volumes {
    volume_name    = docker_volume.tantylla_ingestor_data[0].name
    container_path = "/data"
  }

  networks_advanced {
    name = docker_network.benchmark.id
  }

  env = [
    "RUST_LOG=info",
  ]

  # Ingestor is lightweight: small memory footprint, mostly network I/O.
  memory = 256 * 1024 * 1024

  restart = "unless-stopped"

  # The ingestor needs ScyllaDB to be healthy and search nodes to be
  # running so it can forward CDC events via gRPC.
  depends_on = [
    docker_container.scylla,
    docker_container.tantylla_node,
  ]
}

# =========================================================================
# Gateway
# =========================================================================
# HTTP API frontend. Scatters search queries to all nodes via gRPC,
# merges results by BM25 score, and returns unified JSON responses.
#
# Endpoints:
#   GET  /api/health      -> 200 "OK"
#   POST /api/v1/search   -> SearchRequest JSON -> SearchResponse JSON

resource "docker_container" "tantylla_gateway" {
  count = var.enable_tantylla ? 1 : 0

  name  = local.tantylla_gateway_container_name
  image = docker_image.tantylla[0].image_id

  entrypoint = ["tantylla-gateway"]
  command = [
    "--address", "0.0.0.0",
    "--port", "8080",
    "--search-nodes", local.tantylla_gateway_node_addrs,
  ]

  ports {
    internal = 8080
    external = local.tantylla_gateway_host_port
  }

  networks_advanced {
    name = docker_network.benchmark.id
  }

  env = [
    "RUST_LOG=info",
  ]

  memory = 256 * 1024 * 1024

  restart = "unless-stopped"

  # The gateway needs search nodes running to scatter-gather queries.
  depends_on = [
    docker_container.tantylla_node,
  ]
}

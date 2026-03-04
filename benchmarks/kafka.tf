# =========================================================================
# Kafka + Kafka Connect (Competitor Stack)
# =========================================================================
#
# Conditional on var.enable_competitor.
#
# Pipeline:
#   ScyllaDB CDC log --> ScyllaDB CDC Source Connector --> Kafka topic
#                        --> Elasticsearch Sink Connector --> Elasticsearch
#
# We use Confluent Platform images for Kafka (KRaft mode, no ZooKeeper)
# and Kafka Connect. A custom Connect image adds the ScyllaDB CDC Source
# and Elasticsearch Sink connector plugins (see docker/kafka-connect/).
#
# NOTE on Apple Silicon: Confluent images may run under Rosetta 2 on
# M-series Macs. This adds ~10-15% overhead to the competitor stack.
# Tantylla runs natively (ARM64 Rust binary in Debian ARM64 image).
# The benchmark report should note this asymmetry.

# =========================================================================
# Kafka: Single-node KRaft broker
# =========================================================================

resource "docker_image" "kafka" {
  count        = var.enable_competitor ? 1 : 0
  name         = var.kafka_image
  keep_locally = true
}

resource "docker_volume" "kafka_data" {
  count = var.enable_competitor ? 1 : 0
  name  = "${local.prefix}-kafka-data"
}

resource "docker_container" "kafka" {
  count = var.enable_competitor ? 1 : 0

  name  = local.kafka_container_name
  image = docker_image.kafka[0].image_id

  ports {
    internal = var.kafka_host_port
    external = var.kafka_host_port
  }

  volumes {
    volume_name    = docker_volume.kafka_data[0].name
    container_path = "/var/lib/kafka/data"
  }

  networks_advanced {
    name = docker_network.benchmark.id
  }

  env = [
    # KRaft mode: combined broker + controller, no ZooKeeper.
    "KAFKA_NODE_ID=1",
    "KAFKA_PROCESS_ROLES=broker,controller",

    # Listeners: PLAINTEXT for inter-container, EXTERNAL for host access,
    # CONTROLLER for KRaft consensus.
    "KAFKA_LISTENERS=PLAINTEXT://${local.kafka_container_name}:${local.kafka_internal_port},CONTROLLER://${local.kafka_container_name}:${local.kafka_controller_port},EXTERNAL://0.0.0.0:${var.kafka_host_port}",
    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${local.kafka_container_name}:${local.kafka_internal_port},EXTERNAL://localhost:${var.kafka_host_port}",
    "KAFKA_CONTROLLER_QUORUM_VOTERS=1@${local.kafka_container_name}:${local.kafka_controller_port}",
    "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
    "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",

    # Single-node: replication factor 1 everywhere.
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",

    # Skip the initial rebalance delay — we have a single consumer group.
    "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",

    # Deterministic cluster ID for reproducible benchmarks.
    "CLUSTER_ID=benchmark-cluster-00001",

    # Log retention: keep 1 GB per topic, 1 hour max.
    # This bounds disk usage during long benchmark runs.
    "KAFKA_LOG_RETENTION_HOURS=1",
    "KAFKA_LOG_RETENTION_BYTES=1073741824",
  ]

  memory = var.kafka_memory_mb * 1024 * 1024

  healthcheck {
    test     = ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:${var.kafka_host_port} > /dev/null 2>&1 || exit 1"]
    interval = "10s"
    timeout  = "10s"
    retries  = 15
  }

  wait         = true
  wait_timeout = 120

  restart = "unless-stopped"
}

# =========================================================================
# Kafka Connect: Custom image with CDC + ES connectors
# =========================================================================

resource "docker_image" "kafka_connect" {
  count = var.enable_competitor ? 1 : 0
  name  = "kafka-connect-bench:${terraform.workspace}"

  build {
    context    = "${path.module}/docker/kafka-connect"
    dockerfile = "Dockerfile"
    tag        = ["kafka-connect-bench:${terraform.workspace}"]
  }
}

resource "docker_container" "kafka_connect" {
  count = var.enable_competitor ? 1 : 0

  name  = local.kafka_connect_container_name
  image = docker_image.kafka_connect[0].image_id

  ports {
    internal = local.kafka_connect_internal_port
    external = var.kafka_connect_host_port
  }

  networks_advanced {
    name = docker_network.benchmark.id
  }

  env = [
    # Bootstrap: use the internal listener so traffic stays on the
    # Docker network and avoids port-mapping overhead.
    "CONNECT_BOOTSTRAP_SERVERS=${local.kafka_container_name}:${local.kafka_internal_port}",
    "CONNECT_REST_ADVERTISED_HOST_NAME=${local.kafka_connect_container_name}",
    "CONNECT_REST_PORT=${local.kafka_connect_internal_port}",

    # Consumer group and internal topics for Connect.
    "CONNECT_GROUP_ID=${local.prefix}-connect-group",
    "CONNECT_CONFIG_STORAGE_TOPIC=${local.prefix}-connect-configs",
    "CONNECT_OFFSET_STORAGE_TOPIC=${local.prefix}-connect-offsets",
    "CONNECT_STATUS_STORAGE_TOPIC=${local.prefix}-connect-status",
    "CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1",
    "CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1",
    "CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1",

    # JSON converters without schemas — the CDC source produces plain
    # JSON that the ES sink can index directly.
    "CONNECT_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter",
    "CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE=false",
    "CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter",
    "CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=false",

    # Plugin path includes confluent-hub-installed connectors.
    "CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components",
  ]

  # Kafka Connect workers are JVM-heavy. 768 MB is the minimum for two
  # connector tasks (source + sink) plus framework overhead.
  memory = 768 * 1024 * 1024

  healthcheck {
    test     = ["CMD-SHELL", "curl -sf http://localhost:${local.kafka_connect_internal_port}/ > /dev/null || exit 1"]
    interval = "10s"
    timeout  = "10s"
    retries  = 20
  }

  wait         = true
  wait_timeout = 180

  restart = "unless-stopped"

  depends_on = [
    docker_container.kafka[0],
    docker_container.scylla,
  ]
}

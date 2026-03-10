# =========================================================================
# Elasticsearch (Competitor Stack)
# =========================================================================
#
# Conditional on var.enable_competitor.
#
# Single-node Elasticsearch with security disabled (xpack.security).
# This mirrors a typical development / staging setup. Production ES
# would have security, multiple nodes, and dedicated master nodes —
# none of which materially affect FTS latency in a single-machine
# benchmark.
#
# We configure the English analyzer (with stemming) to match tantylla's
# en_stem tokenizer in Tantivy, ensuring the same query terms produce
# comparable recall. The index template is applied by
# scripts/setup-connectors.sh after the container is healthy.

resource "docker_image" "elasticsearch" {
  count        = var.enable_competitor ? 1 : 0
  name         = var.elasticsearch_image
  keep_locally = true
}

resource "docker_volume" "es_data" {
  count = var.enable_competitor ? 1 : 0
  name  = "${local.prefix}-es-data"
}

resource "docker_container" "elasticsearch" {
  count = var.enable_competitor ? 1 : 0

  name  = local.es_container_name
  image = docker_image.elasticsearch[0].image_id

  ports {
    internal = local.es_internal_port
    external = var.es_host_port
  }

  volumes {
    volume_name    = docker_volume.es_data[0].name
    container_path = "/usr/share/elasticsearch/data"
  }

  networks_advanced {
    name = docker_network.benchmark.id
  }

  env = [
    # Single-node discovery — no cluster formation overhead.
    "discovery.type=single-node",

    # Disable security for benchmark simplicity. Production would use
    # TLS + authentication, adding ~1-2 ms per request.
    "xpack.security.enabled=false",

    # JVM heap: half the container memory, bounded to avoid OOM kills.
    "ES_JAVA_OPTS=-Xms${var.es_heap_mb}m -Xmx${var.es_heap_mb}m",

    # Reduce refresh interval from 1s default to match tantylla's commit
    # interval for a fairer index-lag comparison. Applied via index template
    # after the container becomes healthy (see scripts/setup-connectors.sh).

    # Disable machine learning to save ~200 MB memory.
    "xpack.ml.enabled=false",
  ]

  memory = var.es_memory_mb * 1024 * 1024

  healthcheck {
    test     = ["CMD-SHELL", "curl -sf http://localhost:${local.es_internal_port}/_cluster/health?wait_for_status=yellow&timeout=5s || exit 1"]
    interval = "10s"
    timeout  = "10s"
    retries  = 30
  }

  wait         = true
  wait_timeout = 180

  restart = "unless-stopped"
}

# =========================================================================
# Outputs
# =========================================================================
#
# After `tofu apply`, these outputs show endpoints and next-step
# commands for the deployed workspace.

# -------------------------------------------------------------------------
# Endpoints
# -------------------------------------------------------------------------

output "scylla_cql_endpoint" {
  description = "ScyllaDB CQL endpoint (host)"
  value       = "localhost:${var.scylla_host_port}"
}

output "tantylla_gateway_endpoint" {
  description = "Tantylla gateway HTTP endpoint"
  value       = var.enable_tantylla ? "http://localhost:${local.tantylla_gateway_host_port}" : null
}

output "tantylla_node_endpoints" {
  description = "Tantylla search node gRPC endpoints (host)"
  value = var.enable_tantylla ? {
    for k, v in local.tantylla_nodes : k => "localhost:${v.host_port}"
  } : {}
}

output "elasticsearch_endpoint" {
  description = "Elasticsearch HTTP endpoint"
  value       = var.enable_competitor ? "http://localhost:${var.es_host_port}" : null
}

output "kafka_endpoint" {
  description = "Kafka bootstrap server (host)"
  value       = var.enable_competitor ? "localhost:${var.kafka_host_port}" : null
}

output "kafka_connect_endpoint" {
  description = "Kafka Connect REST API endpoint"
  value       = var.enable_competitor ? "http://localhost:${var.kafka_connect_host_port}" : null
}

# -------------------------------------------------------------------------
# Resource budget
# -------------------------------------------------------------------------

output "estimated_memory_mb" {
  description = "Estimated total memory consumption across all containers (MB)"
  value       = local.estimated_memory_mb
}

# -------------------------------------------------------------------------
# Post-deployment commands
# -------------------------------------------------------------------------

output "next_steps" {
  description = "Commands to run after `tofu apply`"
  value = join("\n", compact([
    "# 1. Wait for all services to be healthy:",
    "bash scripts/wait-for-services.sh ${var.scylla_host_port} ${var.enable_tantylla ? local.tantylla_gateway_host_port : 0} ${var.enable_competitor ? var.es_host_port : 0}",
    "",
    "# 2. Create the benchmark schema:",
    "cqlsh localhost ${var.scylla_host_port} -f data/benchmark-schema.cql",
    "",
    var.enable_competitor ? "# 3. Register Kafka Connect connectors:" : null,
    var.enable_competitor ? "bash scripts/setup-connectors.sh localhost:${var.kafka_connect_host_port} ${local.scylla_container_name}:${local.scylla_internal_port} ${local.es_container_name}:${local.es_internal_port}" : null,
    var.enable_competitor ? "" : null,
    "# ${var.enable_competitor ? "4" : "3"}. Seed benchmark data:",
    "pip install -r scripts/requirements.txt",
    "python scripts/seed-data.py --host localhost --port ${var.scylla_host_port} --count ${local.dataset_doc_count}",
    "",
    "# ${var.enable_competitor ? "5" : "4"}. Run the benchmark:",
    "python scripts/run-benchmark.py \\",
    var.enable_tantylla ? "  --tantylla-url http://localhost:${local.tantylla_gateway_host_port} \\" : null,
    var.enable_competitor ? "  --elasticsearch-url http://localhost:${var.es_host_port} \\" : null,
    "  --queries 1000 --concurrency 10",
  ]))
}

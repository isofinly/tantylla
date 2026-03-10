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

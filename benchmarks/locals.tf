# =========================================================================
# Computed Values
# =========================================================================
#
# Derived from input variables and the current workspace name. These
# locals unify naming, port assignment, and resource budgets so the
# individual .tf files stay clean.

locals {
  # Unique prefix for all Docker resources in this workspace.
  prefix = "${var.benchmark_prefix}-${terraform.workspace}"

  # -------------------------------------------------------------------
  # Dataset sizing
  # -------------------------------------------------------------------

  dataset_sizes = {
    small  = 10000
    medium = 100000
    large  = 500000
  }
  dataset_doc_count = local.dataset_sizes[var.dataset_scale]

  # -------------------------------------------------------------------
  # ScyllaDB
  # -------------------------------------------------------------------

  scylla_container_name = "${local.prefix}-scylla"
  scylla_internal_port  = 9042

  # -------------------------------------------------------------------
  # Tantylla
  # -------------------------------------------------------------------
  # Every node listens on port 10000 *inside* its container.
  # Host ports are offset by index for debugging / direct access.

  tantylla_node_internal_port = 10000

  tantylla_nodes = var.enable_tantylla ? {
    for i in range(var.tantylla_node_count) : "node-${i}" => {
      index     = i
      name      = "${local.prefix}-tantylla-node-${i}"
      host_port = local.tantylla_node_internal_port + i
    }
  } : {}

  # Comma-separated address list consumed by ingestor --search-nodes.
  tantylla_ingestor_node_addrs = join(",", [
    for _, v in local.tantylla_nodes : "${v.name}:${local.tantylla_node_internal_port}"
  ])

  # Comma-separated address list consumed by gateway --search-nodes.
  # The gateway auto-prepends http:// when the scheme is absent.
  tantylla_gateway_node_addrs = join(",", [
    for _, v in local.tantylla_nodes : "${v.name}:${local.tantylla_node_internal_port}"
  ])

  tantylla_gateway_container_name  = "${local.prefix}-tantylla-gateway"
  tantylla_ingestor_container_name = "${local.prefix}-tantylla-ingestor"
  tantylla_gateway_host_port       = 8080

  # -------------------------------------------------------------------
  # Kafka (competitor stack)
  # -------------------------------------------------------------------

  kafka_container_name         = "${local.prefix}-kafka"
  kafka_internal_port          = 29092 # inter-container (PLAINTEXT)
  kafka_controller_port        = 29093
  kafka_connect_container_name = "${local.prefix}-kafka-connect"
  kafka_connect_internal_port  = 8083

  # -------------------------------------------------------------------
  # Elasticsearch (competitor stack)
  # -------------------------------------------------------------------

  es_container_name = "${local.prefix}-elasticsearch"
  es_internal_port  = 9200

  # -------------------------------------------------------------------
  # Resource budget sanity check (informational)
  # -------------------------------------------------------------------

  estimated_memory_mb = (
    var.scylla_memory_mb
    + (var.enable_tantylla ? (var.tantylla_node_count * var.tantylla_node_memory_mb + 768) : 0)
    + (var.enable_competitor ? (var.kafka_memory_mb + var.es_memory_mb + 768) : 0)
  )
}

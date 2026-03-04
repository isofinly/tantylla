# -------------------------------------------------------------------------
# Stack toggles
# -------------------------------------------------------------------------

variable "enable_tantylla" {
  description = "Deploy the Tantylla FTS stack (node + ingestor + gateway)"
  type        = bool
  default     = true
}

variable "enable_competitor" {
  description = "Deploy the competitor stack (Kafka CDC + Elasticsearch)"
  type        = bool
  default     = false
}

# -------------------------------------------------------------------------
# Tantylla configuration
# -------------------------------------------------------------------------

variable "tantylla_node_count" {
  description = "Number of Tantylla search nodes (hash-partitioned index shards)"
  type        = number
  default     = 1

  validation {
    condition     = var.tantylla_node_count >= 1 && var.tantylla_node_count <= 4
    error_message = "Node count must be between 1 and 4 for local benchmarks on 18 GB."
  }
}

variable "tantylla_node_memory_mb" {
  description = "Memory limit per Tantylla node container (MB)"
  type        = number
  default     = 512
}

variable "tantylla_commit_interval_secs" {
  description = "Tantivy commit interval (seconds). Lower = fresher results but higher I/O."
  type        = number
  default     = 5
}

variable "tantylla_safety_interval_ms" {
  description = "CDC safety interval for the ingestor (milliseconds). How far behind wall-clock the reader stays to avoid reading uncommitted CDC entries."
  type        = number
  default     = 5000
}

variable "tantylla_sleep_interval_ms" {
  description = "CDC poll interval for the ingestor (milliseconds). How long the reader sleeps when no new data is found."
  type        = number
  default     = 1000
}

# -------------------------------------------------------------------------
# ScyllaDB configuration
# -------------------------------------------------------------------------

variable "scylla_image" {
  description = "ScyllaDB Docker image"
  type        = string
  default     = "scylladb/scylla:2025.4"
}

variable "scylla_memory_mb" {
  description = "ScyllaDB memory limit (MB). Passed as --memory flag."
  type        = number
  default     = 1024
}

variable "scylla_smp" {
  description = "ScyllaDB CPU core count (--smp flag)"
  type        = number
  default     = 2
}

variable "scylla_host_port" {
  description = "Host port for ScyllaDB CQL. Use 9043 to avoid conflict with the dev compose.yaml on 9042."
  type        = number
  default     = 9043
}

# -------------------------------------------------------------------------
# Kafka configuration (competitor stack)
# -------------------------------------------------------------------------

variable "kafka_image" {
  description = "Confluent Kafka image (KRaft mode, no ZooKeeper)"
  type        = string
  default     = "confluentinc/cp-kafka:8.2.0"
}

variable "kafka_memory_mb" {
  description = "Kafka container memory limit (MB)"
  type        = number
  default     = 1024
}

variable "kafka_host_port" {
  description = "Host port for Kafka external listener"
  type        = number
  default     = 9092
}

variable "kafka_connect_host_port" {
  description = "Host port for Kafka Connect REST API"
  type        = number
  default     = 8083
}

# -------------------------------------------------------------------------
# Elasticsearch configuration (competitor stack)
# -------------------------------------------------------------------------

variable "elasticsearch_image" {
  description = "Elasticsearch Docker image"
  type        = string
  default     = "elasticsearch:8.17.0"
}

variable "es_memory_mb" {
  description = "Elasticsearch container memory limit (MB)"
  type        = number
  default     = 2048
}

variable "es_heap_mb" {
  description = "Elasticsearch JVM heap size (MB). Should be roughly half of es_memory_mb."
  type        = number
  default     = 1024
}

variable "es_host_port" {
  description = "Host port for Elasticsearch HTTP"
  type        = number
  default     = 9200
}

# -------------------------------------------------------------------------
# Benchmark dataset
# -------------------------------------------------------------------------

variable "dataset_scale" {
  description = "Benchmark dataset scale: small (10K docs), medium (100K), large (500K)"
  type        = string
  default     = "medium"

  validation {
    condition     = contains(["small", "medium", "large"], var.dataset_scale)
    error_message = "Must be one of: small, medium, large."
  }
}

# -------------------------------------------------------------------------
# Naming
# -------------------------------------------------------------------------

variable "benchmark_prefix" {
  description = "Name prefix for all Docker resources to avoid collisions"
  type        = string
  default     = "bench"
}

# =========================================================================
# Workspace: competitor
# =========================================================================
#
# Standard industry stack: ScyllaDB CDC → Kafka → Elasticsearch.
# No Tantylla components. Use this to establish the competitor's
# baseline for direct comparison.
#
# Pipeline:
#   1. ScyllaDB CDC log tables
#   2. ScyllaDB CDC Source Connector (Kafka Connect) → Kafka topic
#   3. Elasticsearch Sink Connector (Kafka Connect) → ES index
#   4. Benchmark queries against Elasticsearch directly
#
# Estimated memory: ScyllaDB 1 GB + Kafka 1 GB + Kafka Connect 768 MB
#                   + Elasticsearch 2 GB ≈ 4.8 GB total.
#
# NOTE: On Apple Silicon (M3), Confluent Kafka and Kafka Connect images
# may run under Rosetta 2 emulation, adding ~10-15% CPU overhead.
# Elasticsearch 8.16 has native ARM64 support.
#
# Usage:
#   tofu workspace new competitor
#   tofu apply -var-file=workspaces/competitor.tfvars

enable_tantylla  = false
enable_competitor = true

scylla_memory_mb = 1024
scylla_smp       = 2

kafka_memory_mb = 1024
es_memory_mb    = 2048
es_heap_mb      = 1024

dataset_scale    = "medium"
benchmark_prefix = "bench"

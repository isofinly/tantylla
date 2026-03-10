# Both stacks running simultaneously against a shared ScyllaDB instance.
# Both Tantylla's ingestor and the Kafka CDC connector consume the same
# CDC log independently, so both systems index identical data.
#
# Usage:
#   tofu workspace new head-to-head
#   tofu apply -var-file=workspaces/head-to-head.tfvars

enable_tantylla   = true
enable_competitor = true

# Tantylla: single node with reduced memory to share resources.
tantylla_node_count     = 1
tantylla_node_memory_mb = 384

# ScyllaDB: reduced to 768 MB since both stacks share it.
scylla_memory_mb = 768
scylla_smp       = 2

# Kafka: reduced from 1 GB to 768 MB.
kafka_memory_mb = 768

# Elasticsearch: reduced from 2 GB to 1.5 GB (heap 768 MB).
es_memory_mb = 1536
es_heap_mb   = 768

tantylla_commit_interval_secs = 5
tantylla_safety_interval_ms   = 30000
tantylla_sleep_interval_ms    = 10000

dataset_scale    = "medium"
benchmark_prefix = "bench"

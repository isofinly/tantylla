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
# Usage:
#   tofu workspace new competitor
#   tofu apply -var-file=workspaces/competitor.tfvars

enable_tantylla   = false
enable_competitor = true

scylla_memory_mb = 1024
scylla_smp       = 2

kafka_memory_mb = 1024
es_memory_mb    = 2048
es_heap_mb      = 1024

dataset_scale    = "medium"
benchmark_prefix = "bench"

# Pin to ES 8.x: the Confluent kafka-connect-elasticsearch connector v15.x
# uses the legacy RestHighLevelClient (ES 7 API). That client sends
# "Accept: application/vnd.elasticsearch+json;compatible-with=7" which ES 9.x
# rejects with 400. ES 8.x still accepts the compatibility header, so 8.x is
# the latest supported version for this connector.
elasticsearch_image = "elasticsearch:8.17.0"

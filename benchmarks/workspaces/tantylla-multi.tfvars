# Multi-node Tantylla benchmark: 2 search nodes to demonstrate hash-
# partitioned horizontal scaling.
# Index data is split across nodes via hash(PK) % N,
# and the gateway scatter-gathers search results.
#
# Usage:
#   tofu workspace new tantylla-multi
#   tofu apply -var-file=workspaces/tantylla-multi.tfvars

enable_tantylla   = true
enable_competitor = false

tantylla_node_count     = 2
tantylla_node_memory_mb = 512

scylla_memory_mb = 1024
scylla_smp       = 2

tantylla_commit_interval_secs = 5
tantylla_safety_interval_ms   = 5000
tantylla_sleep_interval_ms    = 1000

dataset_scale    = "medium"
benchmark_prefix = "bench"

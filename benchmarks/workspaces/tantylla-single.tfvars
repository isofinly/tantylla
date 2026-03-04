# =========================================================================
# Workspace: tantylla-single
# =========================================================================
#
# Baseline Tantylla benchmark: single search node, no competitor stack.
# Use this to establish Tantylla's baseline latency and throughput on
# constrained resources before comparing against the competitor.
#
# Estimated memory: ScyllaDB 1 GB + Node 512 MB + Ingestor 256 MB
#                   + Gateway 256 MB ≈ 2 GB total.
#
# Usage:
#   tofu workspace new tantylla-single
#   tofu apply -var-file=workspaces/tantylla-single.tfvars

enable_tantylla  = true
enable_competitor = false

tantylla_node_count     = 1
tantylla_node_memory_mb = 512

# ScyllaDB: moderate allocation for a single-node local benchmark.
scylla_memory_mb = 1024
scylla_smp       = 2

# Aggressive CDC polling for benchmark responsiveness.
# Safety interval 5s + sleep 1s → ~6s worst-case index lag.
tantylla_commit_interval_secs = 5
tantylla_safety_interval_ms   = 5000
tantylla_sleep_interval_ms    = 1000

dataset_scale    = "medium"
benchmark_prefix = "bench"

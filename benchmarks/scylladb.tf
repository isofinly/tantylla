# =========================================================================
# ScyllaDB: Shared Database
# =========================================================================
#
# Both the tantylla stack and the competitor stack read CDC events from
# this single ScyllaDB instance, ensuring the comparison is fair: both
# systems receive identical change streams.
#
# Developer mode is enabled to simplify filesystem requirements in
# Docker. This disables commitlog fsync and some other production
# guards. Since both stacks use the same ScyllaDB, this does not
# skew the comparison — it only affects the shared write path.
#
# TABLETS must be disabled for CDC to work (ScyllaDB requires
# vnodes for CDC log generation).

resource "docker_image" "scylla" {
  name         = var.scylla_image
  keep_locally = true
}

resource "docker_volume" "scylla_data" {
  name = "${local.prefix}-scylla-data"
}

resource "docker_container" "scylla" {
  name  = local.scylla_container_name
  image = docker_image.scylla.image_id

  command = [
    "--smp", tostring(var.scylla_smp),
    "--memory", "${var.scylla_memory_mb}M",
    "--developer-mode", "1",
    "--overprovisioned",
  ]

  ports {
    internal = local.scylla_internal_port
    external = var.scylla_host_port
  }

  volumes {
    volume_name    = docker_volume.scylla_data.name
    container_path = "/var/lib/scylla"
  }

  networks_advanced {
    name = docker_network.benchmark.id
  }

  # Memory limit at the container level. ScyllaDB's --memory flag handles
  # internal allocation; this is a hard ceiling enforced by Docker/cgroups.
  memory = var.scylla_memory_mb * 1024 * 1024

  healthcheck {
    test     = ["CMD-SHELL", "nodetool status | grep -w 'UN' || exit 1"]
    interval = "10s"
    timeout  = "5s"
    retries  = 30
  }

  # Block until the healthcheck passes. Downstream containers (ingestor,
  # Kafka Connect) depend on ScyllaDB being fully initialised.
  wait         = true
  wait_timeout = 180

  restart = "no"
}

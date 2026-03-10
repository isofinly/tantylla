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
    "--overprovisioned", "1",
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

  wait         = true
  wait_timeout = 180

  restart = "no"
}

# =========================================================================
# Schema Initialisation
# =========================================================================

resource "terraform_data" "benchmark_schema" {
  triggers_replace = [docker_container.scylla.id]

  provisioner "local-exec" {
    command = "cqlsh localhost ${var.scylla_host_port} --file '${path.module}/data/input/benchmark-schema.cql'"
  }

  depends_on = [docker_container.scylla]
}

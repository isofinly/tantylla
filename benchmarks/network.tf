# =========================================================================
# Docker Network
# =========================================================================
#
# A single bridge network shared by all benchmark containers in this
# workspace. Containers communicate by name (Docker DNS) over this
# network, so hardcoded IPs are unnecessary.

resource "docker_network" "benchmark" {
  name = "${local.prefix}-net"
}

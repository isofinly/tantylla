resource "docker_network" "benchmark" {
  name = "${local.prefix}-net"
}

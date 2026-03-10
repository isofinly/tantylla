# Usage:
#   tofu init
#   tofu workspace new tantylla-single
#   tofu apply -var-file=workspaces/tantylla-single.tfvars
#
# NOTE: If the Docker build context is slow, create a .dockerignore at
# the project root excluding scylladb/, rust/target/, and .git/.

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

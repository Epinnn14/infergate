# A three-node kind cluster with the whole platform:
#   terraform init && terraform apply
# Images come from GHCR by default; to use local builds run `make kind-images` first and
# apply with -var image_registry=infergate.local -var image_tag=dev.

terraform {
  required_version = ">= 1.6"
  required_providers {
    kind = { source = "tehcyx/kind", version = "~> 0.7" }
    helm = { source = "hashicorp/helm", version = "~> 2.17" }
  }
}

variable "cluster_name" {
  type    = string
  default = "infergate"
}

variable "image_registry" {
  type    = string
  default = "ghcr.io/epinnn14"
}

variable "image_tag" {
  type    = string
  default = "latest"
}

variable "install_monitoring" {
  type    = bool
  default = true
}

variable "install_istio" {
  type    = bool
  default = false
}

resource "kind_cluster" "this" {
  name           = var.cluster_name
  wait_for_ready = true

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"
    node { role = "control-plane" }
    node { role = "worker" }
    node { role = "worker" }
  }
}

provider "helm" {
  kubernetes {
    host                   = kind_cluster.this.endpoint
    client_certificate     = kind_cluster.this.client_certificate
    client_key             = kind_cluster.this.client_key
    cluster_ca_certificate = kind_cluster.this.cluster_ca_certificate
  }
}

module "platform" {
  source             = "../../modules/platform"
  chart_path         = "${path.module}/../../../helm/infergate"
  image_registry     = var.image_registry
  image_tag          = var.image_tag
  image_pull_policy  = var.image_registry == "infergate.local" ? "Never" : "IfNotPresent"
  install_monitoring = var.install_monitoring
  install_istio      = var.install_istio

  infergate_values = {
    gateway = { replicas = 1, autoscaling = { enabled = true, minReplicas = 1, maxReplicas = 3 } }
  }
}

output "kubeconfig" {
  value = kind_cluster.this.kubeconfig_path
}

output "next_steps" {
  value = module.platform.next_steps
}

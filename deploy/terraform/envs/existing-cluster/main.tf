# The platform on a cluster you already have (EKS, GKE, AKS, k3s …), via your kubeconfig:
#   terraform init && terraform apply -var kube_context=my-cluster
# Turn off add-ons the cluster already runs (install_keda=false etc.).

terraform {
  required_version = ">= 1.6"
  required_providers {
    helm = { source = "hashicorp/helm", version = "~> 2.17" }
  }
}

variable "kube_context" { type = string }
variable "kubeconfig" {
  type    = string
  default = "~/.kube/config"
}
variable "image_tag" {
  type    = string
  default = "latest"
}
variable "install_monitoring" {
  type    = bool
  default = true
}
variable "install_keda" {
  type    = bool
  default = true
}
variable "install_istio" {
  type    = bool
  default = false
}
variable "install_argocd" {
  type    = bool
  default = false
}

provider "helm" {
  kubernetes {
    config_path    = pathexpand(var.kubeconfig)
    config_context = var.kube_context
  }
}

module "platform" {
  source             = "../../modules/platform"
  chart_path         = "${path.module}/../../../helm/infergate"
  image_tag          = var.image_tag
  install_monitoring = var.install_monitoring
  install_keda       = var.install_keda
  install_istio      = var.install_istio
  install_argocd     = var.install_argocd

  infergate_values = {
    redis    = { persistence = { enabled = true } }
    postgres = { persistence = { enabled = true, size = "5Gi" } }
  }
}

output "next_steps" {
  value = module.platform.next_steps
}

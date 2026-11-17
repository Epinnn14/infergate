variable "namespace" {
  description = "Namespace for infergate itself."
  type        = string
  default     = "infergate"
}

variable "chart_path" {
  description = "Path to the infergate Helm chart."
  type        = string
}

variable "image_registry" {
  description = "Registry and owner the images come from, e.g. ghcr.io/epinnn14."
  type        = string
  default     = "ghcr.io/epinnn14"
}

variable "image_name_prefix" {
  description = "Images are <prefix>-gateway, -worker and -registry: the repository name that CI publishes them under."
  type        = string
  default     = "infergate" # image-prefix
}

variable "image_tag" {
  description = "Image tag for gateway, worker and registry. Empty = the chart's appVersion."
  type        = string
  default     = ""
}

variable "image_pull_policy" {
  type    = string
  default = "IfNotPresent"
}

variable "install_monitoring" {
  description = "Install kube-prometheus-stack (Prometheus Operator, Grafana) and wire infergate's ServiceMonitors, alerts and dashboard into it."
  type        = bool
  default     = true
}

variable "install_keda" {
  description = "Install KEDA; worker Deployments then scale on queue lag."
  type        = bool
  default     = true
}

variable "install_istio" {
  description = "Install Istio (base + istiod) and turn on mTLS for infergate. Off by default: it is heavy for a laptop."
  type        = bool
  default     = false
}

variable "install_argocd" {
  description = "Install Argo CD (then apply deploy/argocd/application.yaml to let it own the release)."
  type        = bool
  default     = false
}

variable "infergate_values" {
  description = "Extra values for the infergate chart, as a map merged on top of the module's."
  type        = any
  default     = {}
}

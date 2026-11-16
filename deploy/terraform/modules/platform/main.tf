# The cluster add-ons infergate relies on, then infergate itself. Every add-on is optional so
# the same module works on a laptop (kind) and on a managed cluster that already has some.

locals {
  image = { for c in ["gateway", "worker", "registry"] : c => {
    repository = "${var.image_registry}/infergate-${c}"
    tag        = var.image_tag
  } }

  base_values = {
    images = merge(local.image, { pullPolicy = var.image_pull_policy })
    keda   = { enabled = var.install_keda }
    monitoring = {
      serviceMonitor = { enabled = var.install_monitoring, labels = {} }
      prometheusRule = { enabled = var.install_monitoring, labels = {} }
      dashboard      = { enabled = true, labels = { grafana_dashboard = "1" } }
    }
    istio = { enabled = var.install_istio }
  }
}

resource "helm_release" "kube_prometheus_stack" {
  count            = var.install_monitoring ? 1 : 0
  name             = "kube-prometheus-stack"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  version          = "66.3.1"
  namespace        = "monitoring"
  create_namespace = true
  timeout          = 900

  values = [yamlencode({
    alertmanager = { enabled = false }
    prometheus = { prometheusSpec = {
      # pick up ServiceMonitors / PodMonitors / rules from every release, not only this one
      serviceMonitorSelectorNilUsesHelmValues = false
      podMonitorSelectorNilUsesHelmValues     = false
      ruleSelectorNilUsesHelmValues           = false
      retention                               = "2d"
      resources                               = { requests = { cpu = "100m", memory = "400Mi" } }
    } }
    grafana = {
      adminPassword = "infergate"
      sidecar       = { dashboards = { enabled = true, label = "grafana_dashboard", searchNamespace = "ALL" } }
    }
  })]
}

resource "helm_release" "keda" {
  count            = var.install_keda ? 1 : 0
  name             = "keda"
  repository       = "https://kedacore.github.io/charts"
  chart            = "keda"
  version          = "2.16.0"
  namespace        = "keda"
  create_namespace = true
}

resource "helm_release" "istio_base" {
  count            = var.install_istio ? 1 : 0
  name             = "istio-base"
  repository       = "https://istio-release.storage.googleapis.com/charts"
  chart            = "base"
  version          = "1.24.1"
  namespace        = "istio-system"
  create_namespace = true
}

resource "helm_release" "istiod" {
  count      = var.install_istio ? 1 : 0
  name       = "istiod"
  repository = "https://istio-release.storage.googleapis.com/charts"
  chart      = "istiod"
  version    = "1.24.1"
  namespace  = "istio-system"
  depends_on = [helm_release.istio_base]
}

resource "helm_release" "argocd" {
  count            = var.install_argocd ? 1 : 0
  name             = "argocd"
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  version          = "7.7.5"
  namespace        = "argocd"
  create_namespace = true
  values           = [yamlencode({ configs = { params = { "server.insecure" = true } } })]
}

resource "helm_release" "infergate" {
  name             = "infergate"
  chart            = var.chart_path
  namespace        = var.namespace
  create_namespace = true
  timeout          = 600
  wait             = true

  values = [yamlencode(local.base_values), yamlencode(var.infergate_values)]

  # CRDs (ScaledObject, ServiceMonitor, PeerAuthentication) must exist before the chart uses them
  depends_on = [helm_release.kube_prometheus_stack, helm_release.keda, helm_release.istiod]
}

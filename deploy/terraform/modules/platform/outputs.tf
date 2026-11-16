output "namespace" {
  value = helm_release.infergate.namespace
}

output "gateway_service" {
  value = "infergate-gateway.${helm_release.infergate.namespace}.svc"
}

output "next_steps" {
  value = <<-EOT
    kubectl -n ${helm_release.infergate.namespace} port-forward svc/infergate-gateway 8080:80
    curl localhost:8080/v1/chat/completions -H "Authorization: Bearer sk-demo-local" \
      -d '{"model":"chat","messages":[{"role":"user","content":"hello"}]}'
    %{if var.install_monitoring~}
    kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80   # admin / infergate
    %{endif~}
  EOT
}

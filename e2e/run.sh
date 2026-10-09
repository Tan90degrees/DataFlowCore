#!/usr/bin/env bash
set -euo pipefail
mkdir -p .e2e/data
chmod 777 .e2e/data
python3 - <<'PY'
from pathlib import Path
Path('.e2e/data/input.txt').write_text(('业务文档 DataFlow reference paragraph.\n' * 500)[:8192])
PY
data_path="$(pwd)/.e2e/data"
cat > .e2e/kind.yaml <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraMounts:
      - hostPath: ${data_path}
        containerPath: /dataflow-shared
  - role: worker
    extraMounts:
      - hostPath: ${data_path}
        containerPath: /dataflow-shared
  - role: worker
    extraMounts:
      - hostPath: ${data_path}
        containerPath: /dataflow-shared
EOF
kind create cluster --name dataflowcore --config .e2e/kind.yaml --wait 120s
kind load docker-image dataflowcore:e2e dataflowcore-console:e2e --name dataflowcore
kubectl apply -f e2e/infrastructure.yaml
kubectl rollout status deployment/postgres --timeout=180s
kubectl rollout status deployment/embedding-fixture --timeout=180s
# Credentials are fixture values for a disposable isolated cluster.
kubectl create secret generic dataflowcore-secrets \
  --from-literal=database-url=postgresql://dataflow:dataflow-test@postgres:5432/dataflow \
  --from-literal=admin-token=e2e-admin-token-00000000000000000 \
  --from-literal=worker-token=e2e-worker-token-0000000000000000
helm upgrade --install dataflowcore charts/dataflowcore \
  --set image.repository=dataflowcore --set image.tag=e2e \
  --set console.enabled=true --set console.image.repository=dataflowcore-console --set console.image.tag=e2e \
  --set leaseSeconds=10 --set worker.interval=1 --set worker.stopGrace=1 \
  --set worker.resources.requests.cpu=200m --set worker.resources.requests.memory=256Mi \
  --wait --timeout=240s
kubectl port-forward service/dataflowcore-control 18080:8080 > .e2e/port-forward.log 2>&1 &
forward_pid=$!
kubectl port-forward service/dataflowcore-console 18081:8080 > .e2e/console-forward.log 2>&1 &
console_forward_pid=$!
trap 'kill "$forward_pid" "$console_forward_pid" 2>/dev/null || true' EXIT
export DATAFLOW_ADMIN_TOKEN=e2e-admin-token-00000000000000000
PYTHONPATH=src python e2e/verify.py

PYTHONPATH=src python e2e/verify_console.py

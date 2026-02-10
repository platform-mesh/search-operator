#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default configuration
PM_HELM_CHARTS="${PM_HELM_CHARTS:-/Users/C5306990/code/hackthon/helm-charts}"
# Use KCP_KUBECONFIG to avoid conflict with global KUBECONFIG
KCP_KUBECONFIG="${KCP_KUBECONFIG:-${PM_HELM_CHARTS}/local-setup/.secret/kcp/admin.kubeconfig}"
WS_SERVER="${WS_SERVER:-https://localhost:8443/clusters/root:platform-mesh-system}"
ORGS_SERVER="${ORGS_SERVER:-https://localhost:8443/clusters/root:orgs}"

# External file paths (from local setup)
CHARTS_DIR="${PM_HELM_CHARTS}/charts/search-operator-crds/templates"
SCHEMA_FILE="${CHARTS_DIR}/apiresourceschema-searchindices.core.platform-mesh.io.yaml"
EXPORT_FILE="${CHARTS_DIR}/apiexport-core.platform-mesh.io.yaml"

API_EXPORT_NAME="core.platform-mesh.io"

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required binary: $1" >&2
    exit 1
  fi
}

require_bin kubectl

if [[ ! -f "${KCP_KUBECONFIG}" ]]; then
  echo "Kubeconfig not found: ${KCP_KUBECONFIG}" >&2
  exit 1
fi

export KUBECONFIG="${KCP_KUBECONFIG}"

echo "Using KUBECONFIG: $KUBECONFIG"
echo "Service Provider Workspace: $WS_SERVER"
echo "Consumer Workspace: $ORGS_SERVER"

# 0) Verify workspaces exist (we do not create them)
echo "Verifying access to workspaces..."
if ! kubectl --server "$WS_SERVER" get --raw / >/dev/null 2>&1; then
    echo "Error: Cannot access Service Provider Workspace at $WS_SERVER"
    echo "Please ensure the local setup is running and workspaces are created."
    exit 1
fi

if ! kubectl --server "$ORGS_SERVER" get --raw / >/dev/null 2>&1; then
    echo "Error: Cannot access Consumer Workspace at $ORGS_SERVER"
    echo "Please ensure the local setup is running and workspaces are created."
    exit 1
fi

# 1) Apply APIResourceSchema for SearchIndex
echo "Applying APIResourceSchema for SearchIndex in $WS_SERVER..."
if [ -f "$SCHEMA_FILE" ]; then
    kubectl --server "$WS_SERVER" apply -f "$SCHEMA_FILE"
else
    echo "Warning: Schema file not found at $SCHEMA_FILE. Skipping..."
fi

# 2) Apply APIExport (if available)
echo "Applying APIExport in $WS_SERVER..."
if [ -f "$EXPORT_FILE" ]; then
    kubectl --server "$WS_SERVER" apply -f "$EXPORT_FILE"
else
    echo "Warning: APIExport file not found at $EXPORT_FILE. Assuming it exists..."
fi

# 3) Bind export in root:orgs
echo "Creating APIBinding in $ORGS_SERVER..."
cat <<EOF | kubectl --server "$ORGS_SERVER" apply -f -
apiVersion: apis.kcp.io/v1alpha1
kind: APIBinding
metadata:
  name: searchindices-binding
spec:
  reference:
    export:
      path: root:platform-mesh-system
      name: $API_EXPORT_NAME
EOF

# 4) Wait until bound
echo "Waiting for APIBinding to be ready..."
kubectl --server "$ORGS_SERVER" wait --for=condition=Ready apibinding/searchindices-binding --timeout=60s

echo "Preparation complete."

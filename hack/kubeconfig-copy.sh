#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load .env from repo root if present
ENV_FILE="$SCRIPT_DIR/../.env"
if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +a
fi

KCP_KUBECONFIG="$PORTAL_DIRECTORY/upstream/.secret/kcp/admin.kubeconfig"
KCP_SERVER="https://localhost:8443/clusters/root:providers:search"

OUTPUT_KUBECONFIG="$SCRIPT_DIR/../admin.kubeconfig"

echo "Checking for Kind cluster 'platform-mesh'..."
if [ -n "${KIND_EXPERIMENTAL_PROVIDER:-}" ]; then
  export KIND_EXPERIMENTAL_PROVIDER
fi
if ! kind get clusters 2>/dev/null | grep -q "^platform-mesh$"; then
  echo "Error: Kind cluster 'platform-mesh' not found"
  echo "Available clusters:"
  kind get clusters 2>/dev/null || echo "(none)"
  exit 1
fi

if [ ! -f "$KCP_KUBECONFIG" ]; then
    echo "Error: KCP kubeconfig not found at $KCP_KUBECONFIG"
    exit 1
fi

echo "Copying kubeconfig and setting server to $KCP_SERVER..."

cp "$KCP_KUBECONFIG" "$OUTPUT_KUBECONFIG"
yq eval -i '(.clusters[] | select(.name == "workspace.kcp.io/current") | .cluster.server) = "'"$KCP_SERVER"'"' "$OUTPUT_KUBECONFIG"

echo ""
echo "Successfully wrote kubeconfig to $OUTPUT_KUBECONFIG"

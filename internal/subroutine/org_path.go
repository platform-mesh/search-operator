package subroutine

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	kcpcore "github.com/kcp-dev/sdk/apis/core"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	kcptenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
)

// stripPathFromConfig returns a copy of cfg with the URL path cleared,
// leaving a clean base URL for workspace routing.
func stripPathFromConfig(cfg *rest.Config) (*rest.Config, error) {
	out := rest.CopyConfig(cfg)
	parsed, err := url.Parse(out.Host)
	if err != nil {
		return nil, fmt.Errorf("parse KCP host URL: %w", err)
	}
	parsed.Path = ""
	out.Host = parsed.String()
	return out, nil
}

// getWorkspaceClusterAndPath reads the LogicalCluster singleton from the
// current workspace and returns its cluster ID and path annotation
// (e.g. "root:orgs:acme").
func getWorkspaceClusterAndPath(ctx context.Context, mgr mcmanager.Manager) (clusterID string, workspacePath string, err error) {
	id, ok := mccontext.ClusterFrom(ctx)
	if !ok {
		return "", "", fmt.Errorf("cluster not found in context")
	}

	cluster, err := mgr.GetCluster(ctx, id)
	if err != nil {
		return "", "", fmt.Errorf("failed to get cluster %q: %w", id, err)
	}

	// Use client.New directly — cluster.GetClient() is scoped to the APIExport
	// virtual workspace and cannot reach core.kcp.io resources.
	cl, err := client.New(cluster.GetConfig(), client.Options{Scheme: cluster.GetScheme()})
	if err != nil {
		return "", "", fmt.Errorf("failed to create client for cluster %q: %w", id, err)
	}

	lc := &kcpcorev1alpha1.LogicalCluster{}
	if err := cl.Get(ctx, client.ObjectKey{Name: kcpcorev1alpha1.LogicalClusterName}, lc); err != nil {
		return "", "", fmt.Errorf("failed to get LogicalCluster for %q: %w", id, err)
	}

	path, ok := lc.Annotations[kcpcore.LogicalClusterPathAnnotationKey]
	if !ok {
		return "", "", fmt.Errorf("LogicalCluster %q missing %s annotation", id, kcpcore.LogicalClusterPathAnnotationKey)
	}

	return id, path, nil
}

// extractOrgFromPath parses "root:orgs:acme[:...]" and returns "acme".
// Returns an error when the path is not under root:orgs.
func extractOrgFromPath(path string) (string, error) {
	parts := strings.Split(path, ":")
	if len(parts) < 3 || parts[0] != "root" || parts[1] != "orgs" {
		return "", fmt.Errorf("path %q is not under root:orgs", path)
	}
	return parts[2], nil
}

// getOrgClusterID returns the logical cluster ID for the named org workspace
// by looking up its Workspace object via orgsClient (scoped to root:orgs).
func getOrgClusterID(ctx context.Context, orgsClient client.Client, orgName string) (string, error) {
	ws := &kcptenancyv1alpha1.Workspace{}
	if err := orgsClient.Get(ctx, types.NamespacedName{Name: orgName}, ws); err != nil {
		return "", fmt.Errorf("get Workspace %q in root:orgs: %w", orgName, err)
	}
	return ws.Spec.Cluster, nil
}

// buildWorkspaceScopedClient constructs a client targeting the given KCP
// workspace path (e.g. "root:orgs:acme") using rootCfg as the base URL.
func buildWorkspaceScopedClient(rootCfg *rest.Config, scheme *runtime.Scheme, workspacePath string) (client.Client, error) {
	cfg := rest.CopyConfig(rootCfg)
	cfg.Host = fmt.Sprintf("%s/clusters/%s", cfg.Host, workspacePath)
	return client.New(cfg, client.Options{Scheme: scheme})
}

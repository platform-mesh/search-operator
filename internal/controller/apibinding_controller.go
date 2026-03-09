package controller

import (
	"context"
	"net/url"
	"strings"

	"github.com/kcp-dev/logicalcluster/v3"
	kcpv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/builder"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/multicluster"
	lifecyclesubroutine "github.com/platform-mesh/golang-commons/controller/lifecycle/subroutine"
	"github.com/platform-mesh/golang-commons/logger"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	"github.com/platform-mesh/search-operator/internal/opensearch"
	"github.com/platform-mesh/search-operator/internal/subroutine"
)

// APIBindingReconciler watches APIBinding resources across all workspaces
type APIBindingReconciler struct {
	log         *logger.Logger
	mclifecycle *multicluster.LifecycleManager
	allClient   client.Client
}

// NewAPIBindingReconciler creates a new APIBinding reconciler
// If osClient is nil, only the APIBindingWatcher subroutine is used (no indexing)
func NewAPIBindingReconciler(log *logger.Logger, mcMgr mcmanager.Manager, osClient *opensearch.Client, apiExportName string) (*APIBindingReconciler, error) {
	// Create a wildcard client for cross-workspace queries
	allClient, err := GetAllClient(mcMgr.GetLocalManager().GetConfig(), mcMgr.GetLocalManager().GetScheme())
	if err != nil {
		return nil, err
	}

	// Build subroutines list
	subroutines := []lifecyclesubroutine.Subroutine{
		subroutine.NewAPIBindingWatcherSubroutine(mcMgr, allClient, apiExportName),
	}

	return &APIBindingReconciler{
		log:       log,
		allClient: allClient,
		mclifecycle: builder.NewBuilder("apibinding", "APIBindingReconciler", subroutines, log).
			BuildMultiCluster(mcMgr),
	}, nil
}

// +kubebuilder:rbac:groups=apis.kcp.io,resources=apibindings,verbs=get;list;watch
// +kubebuilder:rbac:groups=apis.kcp.io,resources=apiexports,verbs=get;list;watch
// +kubebuilder:rbac:groups=apis.kcp.io,resources=apiresourceschemas,verbs=get;list;watch
// +kubebuilder:rbac:groups=core.platform-mesh.io,resources=accountinfos,verbs=get;list;watch

// Reconcile handles APIBinding reconciliation
func (r *APIBindingReconciler) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
	ctxWithCluster := mccontext.WithCluster(ctx, req.ClusterName)
	return r.mclifecycle.Reconcile(ctxWithCluster, req, &kcpv1alpha1.APIBinding{})
}

// SetupWithManager sets up the controller with the multicluster Manager.
func (r *APIBindingReconciler) SetupWithManager(mgr mcmanager.Manager, maxConcurrentReconciles int, evp ...predicate.Predicate) error {
	return r.mclifecycle.SetupWithManager(mgr, maxConcurrentReconciles, "apibinding", &kcpv1alpha1.APIBinding{}, "", r, r.log, evp...)
}

// GetScopedClient creates a client scoped to a specific logical cluster path (e.g. "root:orgs")
func GetScopedClient(cfg *rest.Config, scheme *runtime.Scheme, clusterPath string) (client.Client, error) {
	scopedCfg := rest.CopyConfig(cfg)
	parsed, err := url.Parse(scopedCfg.Host)
	if err != nil {
		return nil, err
	}
	requestPath := logicalcluster.NewPath(clusterPath).RequestPath()
	parts := strings.Split(parsed.Path, "clusters")
	if len(parts) > 0 {
		parsed.Path, err = url.JoinPath(parts[0], requestPath)
	} else {
		parsed.Path, err = url.JoinPath("/", requestPath)
	}
	if err != nil {
		return nil, err
	}
	scopedCfg.Host = parsed.String()
	return client.New(scopedCfg, client.Options{Scheme: scheme})
}

// GetAllClient creates a client that can query across all workspaces using the wildcard cluster
func GetAllClient(config *rest.Config, scheme *runtime.Scheme) (client.Client, error) {
	allCfg := rest.CopyConfig(config)

	parsed, err := url.Parse(allCfg.Host)
	if err != nil {
		return nil, err
	}

	// Extract the base path before "clusters" and append wildcard
	parts := strings.Split(parsed.Path, "clusters")
	if len(parts) > 0 {
		parsed.Path, err = url.JoinPath(parts[0], "clusters", logicalcluster.Wildcard.String())
		if err != nil {
			return nil, err
		}
	} else {
		parsed.Path, err = url.JoinPath("/", "clusters", logicalcluster.Wildcard.String())
		if err != nil {
			return nil, err
		}
	}

	allCfg.Host = parsed.String()

	return client.New(allCfg, client.Options{Scheme: scheme})
}

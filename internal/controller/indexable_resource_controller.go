package controller

import (
	"context"

	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/builder"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/multicluster"
	lifecyclesubroutine "github.com/platform-mesh/golang-commons/controller/lifecycle/subroutine"
	"github.com/platform-mesh/golang-commons/logger"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	"github.com/platform-mesh/search-operator/internal/opensearch"
	"github.com/platform-mesh/search-operator/internal/subroutine"
)

type IndexableResourceReconciler struct {
	log         *logger.Logger
	mclifecycle *multicluster.LifecycleManager
	allClient   client.Client
}

// NewIndexableResourceReconciler creates a new IndexableResource reconciler
// If osClient is nil, only the IndexableResourceWatcher subroutine is used (no indexing)
func NewIndexableResource(log *logger.Logger, mcMgr mcmanager.Manager, osClient *opensearch.Client, apiExportName string) (*IndexableResourceReconciler, error) {
	// Create a wildcard client for cross-workspace queries
	allClient, err := GetAllClient(mcMgr.GetLocalManager().GetConfig(), mcMgr.GetLocalManager().GetScheme())
	if err != nil {
		return nil, err
	}

	// Build subroutines list
	subroutines := []lifecyclesubroutine.Subroutine{
		subroutine.NewIndexableResourceWatcherSubroutine(mcMgr, allClient, apiExportName),
	}

	return &IndexableResourceReconciler{
		log:       log,
		allClient: allClient,
		mclifecycle: builder.NewBuilder("search-operator", "IndexableResourceReconciler", subroutines, log).
			BuildMultiCluster(mcMgr),
	}, nil
}

// +kubebuilder:rbac:groups=tenancy.kcp.io,resources=workspaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=core.platform-mesh.io,resources=accountinfos,verbs=get;list;watch

// Reconcile handles IndexableResource reconciliation
func (r *IndexableResourceReconciler) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
	ctxWithCluster := mccontext.WithCluster(ctx, req.ClusterName)
	return r.mclifecycle.Reconcile(ctxWithCluster, req, &tenancyv1alpha1.Workspace{})
}

// SetupWithManager sets up the controller with the multicluster Manager.
func (r *IndexableResourceReconciler) SetupWithManager(mgr mcmanager.Manager, maxConcurrentReconciles int, evp ...predicate.Predicate) error {
	return r.mclifecycle.SetupWithManager(mgr, maxConcurrentReconciles, "indexableResourceReconciler", &tenancyv1alpha1.Workspace{}, "", r, r.log, evp...)
}

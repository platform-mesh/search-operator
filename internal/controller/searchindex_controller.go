package controller

import (
	"context"

	"github.com/platform-mesh/golang-commons/controller/lifecycle/builder"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/multicluster"
	lifecyclesubroutine "github.com/platform-mesh/golang-commons/controller/lifecycle/subroutine"
	"github.com/platform-mesh/golang-commons/logger"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	"github.com/platform-mesh/search-operator/api/v1alpha1"
	"github.com/platform-mesh/search-operator/internal/opensearch"
	"github.com/platform-mesh/search-operator/internal/subroutine"
)

// SearchIndexReconciler reconciles a SearchIndex object
type SearchIndexReconciler struct {
	log         *logger.Logger
	mclifecycle *multicluster.LifecycleManager
}

// NewSearchIndexReconciler creates a new SearchIndex reconciler
func NewSearchIndexReconciler(
	log *logger.Logger,
	mcMgr mcmanager.Manager,
	osClient *opensearch.Client,
	staticIndexPrefix string,
	semanticModelID string,
) *SearchIndexReconciler {
	return &SearchIndexReconciler{
		log: log,
		mclifecycle: builder.NewBuilder("searchindex", "SearchIndexReconciler", []lifecyclesubroutine.Subroutine{
			subroutine.NewIndexLifecycleSubroutine(mcMgr, osClient, staticIndexPrefix, semanticModelID),
		}, log).BuildMultiCluster(mcMgr),
	}
}

// +kubebuilder:rbac:groups=search.platform-mesh.io,resources=searchindexes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=search.platform-mesh.io,resources=searchindexes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=search.platform-mesh.io,resources=searchindexes/finalizers,verbs=update
// +kubebuilder:rbac:groups=apis.kcp.io,resources=apibindings,verbs=get;list;watch
// +kubebuilder:rbac:groups=apis.kcp.io,resources=apiexports,verbs=get;list;watch
// +kubebuilder:rbac:groups=core.kcp.io,resources=logicalclusters,verbs=get;list;watch

// Reconcile handles SearchIndex reconciliation
func (r *SearchIndexReconciler) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
	ctxWithCluster := mccontext.WithCluster(ctx, req.ClusterName)
	return r.mclifecycle.Reconcile(ctxWithCluster, req, &v1alpha1.SearchIndex{})
}

// SetupWithManager sets up the controller with the multicluster Manager.
func (r *SearchIndexReconciler) SetupWithManager(mgr mcmanager.Manager, maxConcurrentReconciles int, evp ...predicate.Predicate) error {
	return r.mclifecycle.SetupWithManager(mgr, maxConcurrentReconciles, "searchindex", &v1alpha1.SearchIndex{}, "", r, r.log, evp...)
}

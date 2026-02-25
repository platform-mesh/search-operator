package subroutine

import (
	"context"

	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/runtimeobject"
	lifecyclesubroutine "github.com/platform-mesh/golang-commons/controller/lifecycle/subroutine"
	"github.com/platform-mesh/golang-commons/errors"
	"github.com/platform-mesh/golang-commons/logger"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
)

// IndexableResourceWatcherSubroutine watches IndexableResource resources across workspaces
type IndexableResourceWatcherSubroutine struct {
	mgr           mcmanager.Manager
	allClient     client.Client
	apiExportName string
}

// NewIndexableResourceWatcherSubroutine creates a new IndexableResource watcher subroutine
func NewIndexableResourceWatcherSubroutine(mgr mcmanager.Manager, allClient client.Client, apiExportName string) *IndexableResourceWatcherSubroutine {
	return &IndexableResourceWatcherSubroutine{
		mgr:           mgr,
		allClient:     allClient,
		apiExportName: apiExportName,
	}
}

var _ lifecyclesubroutine.Subroutine = &IndexableResourceWatcherSubroutine{}

// GetName returns the subroutine name
func (s *IndexableResourceWatcherSubroutine) GetName() string {
	return "IndexableResourceWatcher"
}

// Finalizers returns the finalizers this subroutine manages
func (s *IndexableResourceWatcherSubroutine) Finalizers(_ runtimeobject.RuntimeObject) []string {
	// Phase 1: No finalizers - read-only observer
	return nil
}

// Process handles the reconciliation logic
func (s *IndexableResourceWatcherSubroutine) Process(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	_ = logger.LoadLoggerFromContext(ctx)
	_ = instance.(*unstructured.Unstructured) // This must be one of the preconfigured GVKs

	// Assuming everything we reconcile here is in a workspace under :root:orgs:some-org

	// Get searchindex for current org

	// Check if current resource is enabled in this index i.e.: the resource is contained in SearchIndex.Spec.IndexableResources

	// Index current APIResource in OpenSearch to index retrieved

	return ctrl.Result{}, nil
}

// Finalize handles cleanup when the resource is being deleted
func (s *IndexableResourceWatcherSubroutine) Finalize(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	log := logger.LoadLoggerFromContext(ctx)
	binding := instance.(*tenancyv1alpha1.Workspace)

	// Get workspace from context
	workspace, ok := mccontext.ClusterFrom(ctx)
	if !ok {
		workspace = "unknown"
	}

	log.Info().
		Str("name", binding.Name).
		Str("workspace", workspace).
		Msg("IndexableResource being deleted")

	return ctrl.Result{}, nil
}

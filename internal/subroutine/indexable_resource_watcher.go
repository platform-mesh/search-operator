package subroutine

import (
	"context"

	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/runtimeobject"
	lifecyclesubroutine "github.com/platform-mesh/golang-commons/controller/lifecycle/subroutine"
	"github.com/platform-mesh/golang-commons/errors"
	"github.com/platform-mesh/golang-commons/logger"
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
	log := logger.LoadLoggerFromContext(ctx)
	ws := instance.(*tenancyv1alpha1.Workspace)

	// Get index resource for current organization

	// Write Workspace to this index

	log.Info().
		Str("name", ws.Name).
		Msg("observed IndexableResource")

	// TODO Create SearchIndex or update tracked resources based on bindings

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

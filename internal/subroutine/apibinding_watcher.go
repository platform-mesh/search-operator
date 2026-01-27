package subroutine

import (
	"context"

	kcpv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/runtimeobject"
	lifecyclesubroutine "github.com/platform-mesh/golang-commons/controller/lifecycle/subroutine"
	"github.com/platform-mesh/golang-commons/errors"
	"github.com/platform-mesh/golang-commons/logger"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
)

// apiBindingWatcherSubroutine watches APIBinding resources across workspaces
type apiBindingWatcherSubroutine struct {
	mgr       mcmanager.Manager
	allClient client.Client
}

// NewAPIBindingWatcherSubroutine creates a new APIBinding watcher subroutine
func NewAPIBindingWatcherSubroutine(mgr mcmanager.Manager, allClient client.Client) *apiBindingWatcherSubroutine {
	return &apiBindingWatcherSubroutine{
		mgr:       mgr,
		allClient: allClient,
	}
}

var _ lifecyclesubroutine.Subroutine = &apiBindingWatcherSubroutine{}

// GetName returns the subroutine name
func (s *apiBindingWatcherSubroutine) GetName() string {
	return "APIBindingWatcher"
}

// Finalizers returns the finalizers this subroutine manages
func (s *apiBindingWatcherSubroutine) Finalizers(_ runtimeobject.RuntimeObject) []string {
	// Phase 1: No finalizers - read-only observer
	return nil
}

// Process handles the reconciliation logic
func (s *apiBindingWatcherSubroutine) Process(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	log := logger.LoadLoggerFromContext(ctx)
	binding := instance.(*kcpv1alpha1.APIBinding)

	// Get workspace from context
	workspace, ok := mccontext.ClusterFrom(ctx)
	if !ok {
		workspace = "unknown"
	}

	log.Info().
		Str("name", binding.Name).
		Str("workspace", workspace).
		Str("exportName", binding.Spec.Reference.Export.Name).
		Str("exportPath", binding.Spec.Reference.Export.Path).
		Str("phase", string(binding.Status.Phase)).
		Str("apiExportCluster", binding.Status.APIExportClusterName).
		Msg("observed APIBinding")

	if binding.Status.Phase == kcpv1alpha1.APIBindingPhaseBound {
		for _, resource := range binding.Status.BoundResources {
			log.Info().
				Str("binding", binding.Name).
				Str("workspace", workspace).
				Str("group", resource.Group).
				Str("resource", resource.Resource).
				Msg("bound resource available")
		}
	}

	// TODO Create SearchIndex or update tracked resources based on bindings

	return ctrl.Result{}, nil
}

// Finalize handles cleanup when the resource is being deleted
func (s *apiBindingWatcherSubroutine) Finalize(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	log := logger.LoadLoggerFromContext(ctx)
	binding := instance.(*kcpv1alpha1.APIBinding)

	// Get workspace from context
	workspace, ok := mccontext.ClusterFrom(ctx)
	if !ok {
		workspace = "unknown"
	}

	log.Info().
		Str("name", binding.Name).
		Str("workspace", workspace).
		Msg("APIBinding being deleted")

	return ctrl.Result{}, nil
}

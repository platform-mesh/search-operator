package subroutine

import (
	"context"

	"github.com/platform-mesh/golang-commons/controller/lifecycle/runtimeobject"
	lifecyclesubroutine "github.com/platform-mesh/golang-commons/controller/lifecycle/subroutine"
	"github.com/platform-mesh/golang-commons/errors"
	"github.com/platform-mesh/golang-commons/logger"
	ctrl "sigs.k8s.io/controller-runtime"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"

	corev1alpha1 "github.com/platform-mesh/search-operator/api/v1alpha1"
)

// IndexLifecycleSubroutine manages the lifecycle of OpenSearch indices
type IndexLifecycleSubroutine struct {
	mgr mcmanager.Manager
}

// NewIndexLifecycleSubroutine creates a new index lifecycle subroutine
func NewIndexLifecycleSubroutine(mgr mcmanager.Manager) *IndexLifecycleSubroutine {
	return &IndexLifecycleSubroutine{
		mgr: mgr,
	}
}

var _ lifecyclesubroutine.Subroutine = &IndexLifecycleSubroutine{}

// GetName returns the subroutine name
func (s *IndexLifecycleSubroutine) GetName() string {
	return "IndexLifecycle"
}

// Finalizers returns the finalizers this subroutine manages
func (s *IndexLifecycleSubroutine) Finalizers(_ runtimeobject.RuntimeObject) []string {
	// TODO: handle "search.platform-mesh.io/index" finalizer
	return nil
}

// Process handles the reconciliation logic
func (s *IndexLifecycleSubroutine) Process(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	log := logger.LoadLoggerFromContext(ctx)
	searchIndex := instance.(*corev1alpha1.SearchIndex)

	log.Info().
		Str("name", searchIndex.Name).
		Str("indexPrefix", searchIndex.Spec.IndexPrefix).
		Bool("paused", searchIndex.Spec.Paused).
		Int("trackedResources", len(searchIndex.Spec.TrackedResources)).
		Msg("processing SearchIndex")

	// TODO: create/update OpenSearch index based, index documents

	return ctrl.Result{}, nil
}

// Finalize handles cleanup when the resource is being deleted
func (s *IndexLifecycleSubroutine) Finalize(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	log := logger.LoadLoggerFromContext(ctx)
	searchIndex := instance.(*corev1alpha1.SearchIndex)

	log.Info().
		Str("name", searchIndex.Name).
		Msg("finalizing SearchIndex")

	// TODO: delete OpenSearch index

	return ctrl.Result{}, nil
}

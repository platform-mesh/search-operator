package subroutine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/platform-mesh/golang-commons/controller/lifecycle/runtimeobject"
	lifecyclesubroutine "github.com/platform-mesh/golang-commons/controller/lifecycle/subroutine"
	"github.com/platform-mesh/golang-commons/errors"
	"github.com/platform-mesh/golang-commons/logger"
	"github.com/platform-mesh/search-operator/api/v1alpha1"
	"github.com/platform-mesh/search-operator/internal/opensearch"
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
	osClient      *opensearch.Client
	apiExportName string
}

// NewIndexableResourceWatcherSubroutine creates a new IndexableResource watcher subroutine
func NewIndexableResourceWatcherSubroutine(mgr mcmanager.Manager, allClient client.Client, osClient *opensearch.Client, apiExportName string) *IndexableResourceWatcherSubroutine {
	return &IndexableResourceWatcherSubroutine{
		mgr:           mgr,
		allClient:     allClient,
		osClient:      osClient,
		apiExportName: apiExportName,
	}
}

var _ lifecyclesubroutine.Subroutine = &IndexableResourceWatcherSubroutine{}

const indexableResourceFinalizer = "search.platform-mesh.io/indexable-resource"

// GetName returns the subroutine name
func (s *IndexableResourceWatcherSubroutine) GetName() string {
	return "IndexableResourceWatcher"
}

// Finalizers returns the finalizers this subroutine manages
func (s *IndexableResourceWatcherSubroutine) Finalizers(_ runtimeobject.RuntimeObject) []string {
	return []string{indexableResourceFinalizer}
}

// Process handles the reconciliation logic
func (s *IndexableResourceWatcherSubroutine) Process(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	log := logger.LoadLoggerFromContext(ctx)
	resource := instance.(*unstructured.Unstructured)

	clusterName, err := s.getClusterFromContext(ctx)
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(err, true, false)
	}

	orgName, err := s.extractOrgFromKCPPath(clusterName)
	if err != nil {
		log.Debug().Msg("Not in an org workspace, skipping")
		return ctrl.Result{}, nil
	}

	searchIndex, err := s.getSearchIndexForOrg(ctx, orgName)
	if err != nil {
		// SearchIndex might not exist yet. TODO: requeue or exp. backoff or ignore?
		log.Debug().Err(err).Msg("SearchIndex not found, will retry")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Maybe not the best idea to first calculate index and only then decide whether to reconcile. We should handle this statically if possible.
	if !s.isResourceTracked(resource, searchIndex) {
		log.Debug().
			Str("kind", resource.GetKind()).
			Msg("resource type not tracked, skipping")
		return ctrl.Result{}, nil
	}

	// TODO: handle updates to SearchIndex (e.g. tracked resources changed, or paused)
	return ctrl.Result{}, nil
}

func (s *IndexableResourceWatcherSubroutine) getClusterFromContext(ctx context.Context) (string, error) {
	cluster, ok := mccontext.ClusterFrom(ctx)
	if !ok {
		return "", fmt.Errorf("cluster not found in context")
	}
	return cluster, nil
}

func (s *IndexableResourceWatcherSubroutine) extractOrgFromKCPPath(clusterName string) (string, error) {
	parts := strings.Split(clusterName, ":")
	if len(parts) < 3 || parts[0] != "root" || parts[1] != "orgs" {
		return "", fmt.Errorf("not an org workspace")
	}
	return parts[2], nil
}

func (s *IndexableResourceWatcherSubroutine) getSearchIndexForOrg(ctx context.Context, orgName string) (*v1alpha1.SearchIndex, error) {
	// TODO
	return nil, fmt.Errorf("getSearchIndexForOrg not implemented yet")
}

func (s *IndexableResourceWatcherSubroutine) isResourceTracked(
	resource *unstructured.Unstructured,
	searchIndex *v1alpha1.SearchIndex,
) bool {
	gvk := resource.GroupVersionKind()

	for _, tracked := range searchIndex.Spec.TrackedResources {
		if tracked.Group == gvk.Group &&
			tracked.Version == gvk.Version &&
			tracked.Kind == gvk.Kind {
			return true
		}
	}

	return false
}

func (s *IndexableResourceWatcherSubroutine) generateDocumentID(
	resource *unstructured.Unstructured,
	clusterName string,
) string {
	namespace := resource.GetNamespace()
	if namespace == "" {
		namespace = "_cluster"
	}
	return fmt.Sprintf("%s/%s/%s/%s",
		clusterName,
		namespace,
		resource.GetKind(),
		resource.GetName(),
	)
}

func (s *IndexableResourceWatcherSubroutine) Finalize(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	log := logger.LoadLoggerFromContext(ctx)
	resource := instance.(*unstructured.Unstructured)

	clusterName, err := s.getClusterFromContext(ctx)
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(err, true, false)
	}

	orgName, err := s.extractOrgFromKCPPath(clusterName)
	if err != nil {
		return ctrl.Result{}, nil
	}

	searchIndex, err := s.getSearchIndexForOrg(ctx, orgName)
	if err != nil {
		log.Debug().Msg("SearchIndex not found during finalization")
		return ctrl.Result{}, nil
	}

	docID := s.generateDocumentID(resource, clusterName)
	indexName := searchIndex.Status.IndexName
	if indexName == "" {
		log.Warn().Msg("SearchIndex has no IndexName, cannot delete document")
		return ctrl.Result{}, nil
	}

	if err := s.osClient.DeleteDocument(ctx, indexName, docID); err != nil {
		log.Error().Err(err).Msg("failed to delete document from OpenSearch")
		return ctrl.Result{}, errors.NewOperatorError(err, true, false)
	}

	log.Info().
		Str("docID", docID).
		Str("index", indexName).
		Msg("deleted document from index")

	return ctrl.Result{}, nil
}

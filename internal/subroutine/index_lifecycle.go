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
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"

	"github.com/platform-mesh/search-operator/internal/opensearch"
)

// IndexLifecycleSubroutine manages the lifecycle of OpenSearch indices
type IndexLifecycleSubroutine struct {
	osClient *opensearch.Client
}

// NewIndexLifecycleSubroutine creates a new index lifecycle subroutine
func NewIndexLifecycleSubroutine(mgr mcmanager.Manager, osClient *opensearch.Client) *IndexLifecycleSubroutine {
	return &IndexLifecycleSubroutine{
		osClient: osClient,
	}
}

var _ lifecyclesubroutine.Subroutine = &IndexLifecycleSubroutine{}

const (
	searchIndexFinalizer = "search.platform-mesh.io/index"
	orgsWorkspacePath    = "root:orgs"
)

// GetName returns the subroutine name
func (s *IndexLifecycleSubroutine) GetName() string {
	return "IndexLifecycle"
}

// Finalizers returns the finalizers this subroutine manages
func (s *IndexLifecycleSubroutine) Finalizers(instance runtimeobject.RuntimeObject) []string {
	if !isSearchIndexResource(instance) {
		return nil
	}
	return []string{searchIndexFinalizer}
}

// Process handles the reconciliation logic
func (s *IndexLifecycleSubroutine) Process(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	log := logger.LoadLoggerFromContext(ctx)
	searchIndex, ok := instance.(*v1alpha1.SearchIndex)
	if !ok {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("expected *v1alpha1.SearchIndex, got %T", instance), false, false)
	}
	if !isSearchIndexResource(searchIndex) {
		return ctrl.Result{}, nil
	}

	organizationClusterID := searchIndex.Spec.OrganizationClusterID
	if organizationClusterID == "" {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("missing required spec.organizationClusterID"), false, false)
	}
	targetWorkspace := resolveIndexWorkspace(organizationClusterID, searchIndex.GetName())

	indexPrefix := searchIndex.Spec.IndexPrefix
	if indexPrefix == "" {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("missing required spec.indexPrefix"), false, false)
	}

	paused := searchIndex.Spec.Paused

	trackedResources := searchIndex.Spec.TrackedResources
	if len(trackedResources) == 0 {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("this organization specifies no resources to add to index"), false, false)
	}

	indexName := buildIndexName(indexPrefix, targetWorkspace)
	numberShards := searchIndex.Spec.NumberOfShards
	if numberShards <= 0 {
		numberShards = 1
	}

	numReplicas := searchIndex.Spec.NumberOfReplicas
	if numReplicas < 0 {
		numReplicas = 0
	}

	log.Info().
		Str("name", searchIndex.GetName()).
		Str("reconcileWorkspace", organizationClusterID).
		Str("targetWorkspace", targetWorkspace).
		Str("indexPrefix", indexPrefix).
		Str("indexName", indexName).
		Bool("paused", paused).
		Int("trackedResources", len(trackedResources)).
		Int32("numberOfShards", numberShards).
		Int32("numberOfReplicas", numReplicas).
		Msg("processing SearchIndex")

	if paused {
		return ctrl.Result{}, nil
	}

	if s.osClient == nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("OpenSearch client not configured"), true, false)
	}

	exists, err := s.osClient.IndexExists(ctx, indexName)
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to check index existence: %w", err), true, true)
	}

	created := false
	if !exists {
		if err := s.osClient.CreateIndex(ctx, indexName, ""); err != nil {
			return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to create index %q: %w", indexName, err), true, true)
		}
		created = true
	}

	if created {
		searchIndex.Status.LastSyncTime = &v1.Time{Time: time.Now()}
		log.Info().
			Str("name", searchIndex.GetName()).
			Str("reconcileWorkspace", organizationClusterID).
			Str("targetWorkspace", targetWorkspace).
			Str("indexName", indexName).
			Bool("created", created).
			Int32("numberOfShards", numberShards).
			Int32("numberOfReplicas", numReplicas).
			Msg("updated SearchIndex status")
	}

	return ctrl.Result{}, nil
}

// Finalize handles cleanup when the resource is being deleted
func (s *IndexLifecycleSubroutine) Finalize(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	log := logger.LoadLoggerFromContext(ctx)
	searchIndex, ok := instance.(*unstructured.Unstructured)
	if !ok {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("expected *unstructured.Unstructured, got %T", instance), false, false)
	}
	if !isSearchIndexResource(searchIndex) {
		return ctrl.Result{}, nil
	}
	if s.osClient == nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("OpenSearch client not configured during SearchIndex finalization"), true, false)
	}

	workspaceName, ok := mccontext.ClusterFrom(ctx)
	if !ok {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("missing cluster in multicluster context during finalization"), true, false)
	}
	targetWorkspace := resolveIndexWorkspace(workspaceName, searchIndex.GetName())

	indexName, _, err := unstructured.NestedString(searchIndex.Object, "status", "indexName")
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to read status.indexName: %w", err), false, false)
	}
	if indexName == "" {
		indexPrefix, _, specErr := unstructured.NestedString(searchIndex.Object, "spec", "indexPrefix")
		if specErr != nil {
			return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to read spec.indexPrefix during finalization: %w", specErr), false, false)
		}
		indexName = buildIndexName(indexPrefix, targetWorkspace)
	}

	log.Info().
		Str("name", searchIndex.GetName()).
		Str("reconcileWorkspace", workspaceName).
		Str("targetWorkspace", targetWorkspace).
		Str("indexName", indexName).
		Msg("finalizing SearchIndex")

	if err := s.osClient.DeleteIndex(ctx, indexName); err != nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to delete index %q: %w", indexName, err), true, true)
	}

	return ctrl.Result{}, nil
}

func isSearchIndexResource(instance runtimeobject.RuntimeObject) bool {
	uns, ok := instance.(*unstructured.Unstructured)
	if !ok {
		return false
	}
	return uns.GetKind() == "SearchIndex" &&
		uns.GetAPIVersion() == "core.platform-mesh.io/v1alpha1"
}

func buildIndexName(indexPrefix, workspaceName string) string {
	prefix := sanitizeIndexNamePart(indexPrefix)
	if prefix == "" {
		prefix = "search"
	}
	workspace := sanitizeIndexNamePart(workspaceName)
	if workspace == "" {
		workspace = "workspace"
	}
	indexName := fmt.Sprintf("%s-%s", prefix, workspace)
	if len(indexName) > 255 {
		indexName = indexName[:255]
	}
	return strings.Trim(indexName, "-")
}

// resolveIndexWorkspace derives the virtual workspace that should own the index.
// In root:orgs, each SearchIndex resource name represents one onboarded org workspace.
func resolveIndexWorkspace(reconcileWorkspace, resourceName string) string {
	if reconcileWorkspace == orgsWorkspacePath && resourceName != "" {
		return fmt.Sprintf("%s:%s", orgsWorkspacePath, resourceName)
	}
	return reconcileWorkspace
}

func sanitizeIndexNamePart(value string) string {
	value = strings.ToLower(value)

	var b strings.Builder
	b.Grow(len(value))
	lastWasDash := false

	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastWasDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastWasDash = false
		default:
			if !lastWasDash {
				b.WriteByte('-')
				lastWasDash = true
			}
		}
	}

	return strings.Trim(b.String(), "-")
}

package subroutine

import (
	"context"
	"fmt"
	"strings"
	"time"

	kcpcore "github.com/kcp-dev/sdk/apis/core"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	kcptenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/runtimeobject"
	lifecyclesubroutine "github.com/platform-mesh/golang-commons/controller/lifecycle/subroutine"
	"github.com/platform-mesh/golang-commons/errors"
	"github.com/platform-mesh/golang-commons/logger"
	"github.com/platform-mesh/search-operator/api/v1alpha1"
	"github.com/platform-mesh/search-operator/internal/opensearch"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
)

// IndexableResourceWatcherSubroutine watches IndexableResource resources across workspaces
type IndexableResourceWatcherSubroutine struct {
	mgr           mcmanager.Manager
	allClient     client.Client
	orgsClient    client.Client // scoped to root:orgs for Workspace lookups
	osClient      *opensearch.Client
	apiExportName string
}

// NewIndexableResourceWatcherSubroutine creates a new IndexableResource watcher subroutine
func NewIndexableResourceWatcherSubroutine(mgr mcmanager.Manager, allClient client.Client, orgsClient client.Client, osClient *opensearch.Client, apiExportName string) *IndexableResourceWatcherSubroutine {
	return &IndexableResourceWatcherSubroutine{
		mgr:           mgr,
		allClient:     allClient,
		orgsClient:    orgsClient,
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

	clusterID, workspacePath, err := s.getWorkspacePath(ctx)
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(err, true, false)
	}

	orgName, err := s.extractOrgFromKCPPath(workspacePath)
	if err != nil {
		log.Debug().Msg("Not in an org workspace, skipping")
		return ctrl.Result{}, nil
	}

	orgID, err := s.getOrgID(ctx, orgName)
	if err != nil {
		log.Debug().Err(err).Msg("SearchIndex not found, will retry")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	indexName, err := getSearchIndexForOrg(ctx, s.orgsClient, orgID)
	if err != nil {
		log.Debug().Msg("SearchIndex has no IndexName yet, requeuing")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	docID := s.generateDocumentID(resource, clusterID)
	gvk := resource.GroupVersionKind()

	doc := opensearch.NewResourceDocument(
		docID,
		resource.GetKind(),
		resource.GetName(),
		resource.GetNamespace(),
		clusterID,
		workspacePath,
	)
	doc.APIGroup = gvk.Group
	doc.APIVersion = gvk.Version
	doc.OrganizationName = orgName
	doc.OrganizationID = orgID
	doc.Labels = resource.GetLabels()
	doc.Annotations = resource.GetAnnotations()

	if accountName, err := extractAccountFromKCPPath(workspacePath); err == nil {
		doc.AccountName = accountName
	}

	if spec, ok, _ := unstructured.NestedMap(resource.Object, "spec"); ok {
		doc.Spec = spec
	}
	if status, ok, _ := unstructured.NestedMap(resource.Object, "status"); ok {
		doc.Status = status
	}

	if err := s.osClient.IndexDocument(ctx, indexName, docID, doc); err != nil {
		return ctrl.Result{}, errors.NewOperatorError(
			fmt.Errorf("failed to index document %s: %w", docID, err), true, false,
		)
	}

	log.Info().
		Str("docID", docID).
		Str("index", indexName).
		Str("kind", resource.GetKind()).
		Msg("indexed document")

	return ctrl.Result{}, nil
}

func getSearchIndexForOrg(ctx context.Context, orgsClient client.Client, orgID string) (string, error) {
	searchIndex := v1alpha1.SearchIndex{}
	err := orgsClient.Get(ctx, types.NamespacedName{Name: orgID}, &searchIndex)
	if err != nil {
		return "", fmt.Errorf("failed to get cluster %q: %w", orgID, err)
	}
	return searchIndex.Status.IndexName, nil
}

func (s *IndexableResourceWatcherSubroutine) getWorkspacePath(ctx context.Context) (clusterID string, workspacePath string, err error) {
	id, ok := mccontext.ClusterFrom(ctx)
	if !ok {
		return "", "", fmt.Errorf("cluster not found in context")
	}

	cluster, err := s.mgr.GetCluster(ctx, id)
	if err != nil {
		return "", "", fmt.Errorf("failed to get cluster %q: %w", id, err)
	}
	cl, err := client.New(cluster.GetConfig(), client.Options{Scheme: cluster.GetScheme()})
	if err != nil {
		return "", "", fmt.Errorf("failed to create client for cluster %q: %w", id, err)
	}
	lc := &kcpcorev1alpha1.LogicalCluster{}
	err = cl.Get(ctx, client.ObjectKey{Name: kcpcorev1alpha1.LogicalClusterName}, lc)
	if err != nil {
		return "", "", fmt.Errorf("failed to get LogicalCluster for %q: %w", id, err)
	}

	path, ok := lc.Annotations[kcpcore.LogicalClusterPathAnnotationKey]
	if !ok {
		return "", "", fmt.Errorf("LogicalCluster %q missing %s annotation", id, kcpcore.LogicalClusterPathAnnotationKey)
	}

	return id, path, nil
}

func (s *IndexableResourceWatcherSubroutine) extractOrgFromKCPPath(clusterName string) (string, error) {
	parts := strings.Split(clusterName, ":")
	if len(parts) < 3 || parts[0] != "root" || parts[1] != "orgs" {
		return "", fmt.Errorf("not an org workspace")
	}
	return parts[2], nil
}

// extractAccountFromKCPPath extracts the account name from a KCP path like "root:orgs:acme:account-1"
func extractAccountFromKCPPath(clusterName string) (string, error) {
	parts := strings.Split(clusterName, ":")
	if len(parts) < 4 {
		return "", fmt.Errorf("path %q does not contain an account segment", clusterName)
	}
	return parts[3], nil
}

func (s *IndexableResourceWatcherSubroutine) getOrgID(ctx context.Context, orgName string) (string, error) {
	workspace := &kcptenancyv1alpha1.Workspace{}
	if err := s.orgsClient.Get(ctx, types.NamespacedName{Name: orgName}, workspace); err != nil {
		return "", fmt.Errorf("failed to get Workspace %q: %w", orgName, err)
	}

	return workspace.Spec.Cluster, nil
}

func (s *IndexableResourceWatcherSubroutine) generateDocumentID(
	resource *unstructured.Unstructured,
	clusterName string,
) string {
	namespace := resource.GetNamespace()
	if namespace == "" {
		namespace = "_cluster"
	}
	return fmt.Sprintf("%s-%s-%s-%s",
		clusterName,
		namespace,
		resource.GetKind(),
		resource.GetName(),
	)
}

func (s *IndexableResourceWatcherSubroutine) Finalize(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	log := logger.LoadLoggerFromContext(ctx)
	resource := instance.(*unstructured.Unstructured)

	clusterID, workspacePath, err := s.getWorkspacePath(ctx)
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(err, true, false)
	}

	orgName, err := s.extractOrgFromKCPPath(workspacePath)
	if err != nil {
		log.Debug().Msg("Not in an org workspace, skipping")
		return ctrl.Result{}, nil
	}

	orgID, err := s.getOrgID(ctx, orgName)
	if err != nil {
		log.Debug().Err(err).Msg("SearchIndex not found, will retry")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	indexName := fmt.Sprintf("pm-orgs-%s", orgID)
	if indexName == "" {
		log.Debug().Msg("SearchIndex has no IndexName yet, requeuing")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if indexName == "" {
		log.Warn().Msg("SearchIndex has no IndexName, cannot delete document")
		return ctrl.Result{}, nil
	}

	docID := s.generateDocumentID(resource, clusterID)
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

package subroutine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"sigs.k8s.io/yaml"

	kcpcore "github.com/kcp-dev/sdk/apis/core"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	kcptenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/runtimeobject"
	lifecyclesubroutine "github.com/platform-mesh/golang-commons/controller/lifecycle/subroutine"
	"github.com/platform-mesh/golang-commons/errors"
	fgamodel "github.com/platform-mesh/golang-commons/fga/model"
	"github.com/platform-mesh/golang-commons/logger"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"

	accountv1alpha1 "github.com/platform-mesh/account-operator/api/v1alpha1"

	"github.com/platform-mesh/search-operator/api/v1alpha1"
	"github.com/platform-mesh/search-operator/internal/opensearch"
)

// IndexableResourceWatcherSubroutine watches IndexableResource resources across workspaces
type IndexableResourceWatcherSubroutine struct {
	mgr           mcmanager.Manager
	allClient     client.Client
	orgsClient    client.Client // scoped to root:orgs for Workspace lookups
	osClient      *opensearch.Client
	apiExportName string
	rootCfg       *rest.Config // base KCP REST config (path stripped) for workspace-scoped clients
}

// NewIndexableResourceWatcherSubroutine creates a new IndexableResource watcher subroutine.
// localCfg must be the admin KCP REST config
func NewIndexableResourceWatcherSubroutine(mgr mcmanager.Manager, allClient client.Client, orgsClient client.Client, osClient *opensearch.Client, apiExportName string, localCfg *rest.Config) (*IndexableResourceWatcherSubroutine, error) {
	// Strip any existing path so we have a clean base URL for workspace routing.
	rootCfg := rest.CopyConfig(localCfg)
	parsed, err := url.Parse(rootCfg.Host)
	if err != nil {
		return nil, fmt.Errorf("parse KCP host URL: %w", err)
	}
	parsed.Path = ""
	rootCfg.Host = parsed.String()

	return &IndexableResourceWatcherSubroutine{
		mgr:           mgr,
		allClient:     allClient,
		orgsClient:    orgsClient,
		osClient:      osClient,
		apiExportName: apiExportName,
		rootCfg:       rootCfg,
	}, nil
}

var _ lifecyclesubroutine.Subroutine = &IndexableResourceWatcherSubroutine{}

const (
	indexableResourceFinalizer = "search.platform-mesh.io/indexable-resource"
	kcpClusterAnnotation       = "kcp.io/cluster"
)

func (s *IndexableResourceWatcherSubroutine) GetName() string {
	return "IndexableResourceWatcher"
}

// Finalizers return the finalizers this subroutine manages
func (s *IndexableResourceWatcherSubroutine) Finalizers(_ runtimeobject.RuntimeObject) []string {
	return []string{indexableResourceFinalizer}
}

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
		log.Debug().Err(err).Msg("org ID not found, will retry")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	indexName, err := getSearchIndexForOrg(ctx, s.orgsClient, orgID)
	if err != nil {
		log.Debug().Err(err).Msg("could not get SearchIndex, requeuing")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if indexName == "" {
		log.Debug().Str("orgID", orgID).Msg("search index not ready yet, requeuing")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	resourceClusterID := resolveResourceClusterID(resource, clusterID)
	docID := s.generateDocumentID(resource, resourceClusterID)
	gvk := resource.GroupVersionKind()

	doc := opensearch.NewResourceDocument(
		docID,
		resource.GetKind(),
		resource.GetName(),
		resource.GetNamespace(),
		resourceClusterID,
		workspacePath,
	)
	doc.APIGroup = gvk.Group
	doc.APIVersion = gvk.Version
	doc.OrganizationName = orgName
	doc.OrganizationID = orgID
	doc.Labels = resource.GetLabels()
	doc.Annotations = resource.GetAnnotations()

	accountInfo, err := s.getAccountInfo(ctx, workspacePath, gvk, resource)
	if err != nil {
		log.Warn().Err(err).
			Str("workspacePath", workspacePath).
			Msg("AccountInfo path-based lookup failed, requeuing")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Not all APIResources have an AccountInfo directly associated with them, but there is always a Parent Account or Org that has an AccountInfo
	if accountInfo == nil {
		accountInfo = s.getParentAccountInfo(ctx, log, resource, clusterID, resourceClusterID)
	}

	if accountInfo.Spec.Account.Name == "" || accountInfo.Spec.Account.OriginClusterId == "" ||
		accountInfo.Spec.Organization.Name == "" || accountInfo.Spec.Organization.OriginClusterId == "" {
		log.Warn().Msg("AccountInfo is missing required account/organization origin metadata, requeuing")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	fgaGroup, fgaKind, fgaClusterID := mapResourceToFGAObject(gvk.Group, gvk.Kind, resourceClusterID, accountInfo)
	doc.FGAObject = buildFGAObjectName(fgaGroup, fgaKind, fgaClusterID, resource.GetName(), resource.GetNamespace())

	// Contextual Tuples (Permissions field), build parent hierarchy from AccountInfo
	orgObject := buildFGAObjectName(v1alpha1.GroupName, v1alpha1.AccountKind, accountInfo.Spec.Organization.OriginClusterId, accountInfo.Spec.Organization.Name, "")
	accountObject := buildFGAObjectName(v1alpha1.GroupName, v1alpha1.AccountKind, accountInfo.Spec.Account.OriginClusterId, accountInfo.Spec.Account.Name, "")
	doc.AccountName = accountInfo.Spec.Account.Name
	doc.AccountID = accountInfo.Spec.Account.OriginClusterId

	isOrganization := gvk.Group == v1alpha1.GroupName && gvk.Kind == v1alpha1.OrganizationKind
	parentObject := accountObject
	if isOrganization {
		parentObject = orgObject
	}

	namespaceClusterID := resourceClusterID
	if generatedClusterID := strings.TrimSpace(accountInfo.Spec.Account.GeneratedClusterId); generatedClusterID != "" {
		namespaceClusterID = generatedClusterID
	}

	if ns := resource.GetNamespace(); ns != "" {
		// Namespaced resource: Resource -> Namespace -> Parent
		nsObject := buildFGAObjectName("", "Namespace", namespaceClusterID, ns, "")
		addParentPermissions(doc, fgamodel.BuildParentTuples(parentObject, doc.FGAObject, &nsObject))
	} else if doc.FGAObject != parentObject {
		// Cluster-scoped resource: direct link to its logical parent (Account or Org)
		addParentPermissions(doc, fgamodel.BuildParentTuples(parentObject, doc.FGAObject, nil))
	}

	payloadRawJSON, payloadText, payloadErr := buildPayload(resource)
	if payloadErr != nil {
		return ctrl.Result{}, errors.NewOperatorError(
			fmt.Errorf("failed to build payload for %s/%s: %w", resource.GetKind(), resource.GetName(), payloadErr),
			true,
			false,
		)
	}
	doc.PayloadRawJSON = payloadRawJSON
	doc.PayloadText = payloadText

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

func (s *IndexableResourceWatcherSubroutine) getParentAccountInfo(ctx context.Context, log *logger.Logger, resource *unstructured.Unstructured, clusterID, resourceClusterID string) *accountv1alpha1.AccountInfo {
	accountInfo := accountv1alpha1.AccountInfo{}
	accountInfoLookupClusters := resolveAccountInfoLookupClusters(resource, clusterID, resourceClusterID)
	for _, candidateClusterID := range accountInfoLookupClusters {
		cluster, getClusterErr := s.mgr.GetCluster(ctx, candidateClusterID)
		if getClusterErr != nil {
			log.Warn().
				Err(getClusterErr).
				Str("candidateClusterID", candidateClusterID).
				Msg("failed to get candidate cluster for AccountInfo lookup")
			continue
		}

		clusterClient := cluster.GetClient()
		lookupCtx := mccontext.WithCluster(ctx, candidateClusterID)
		getAccountInfoErr := clusterClient.Get(lookupCtx, client.ObjectKey{Name: "account"}, &accountInfo)
		if getAccountInfoErr == nil {
			break
		}
		if apierrors.IsNotFound(getAccountInfoErr) {
			retryErr := clusterClient.Get(ctx, client.ObjectKey{Name: "account"}, &accountInfo)
			if retryErr == nil {
				break
			}
			if apierrors.IsNotFound(retryErr) {
				log.Debug().
					Str("candidateClusterID", candidateClusterID).
					Msg("AccountInfo not found in candidate cluster")
				continue
			}
			log.Warn().
				Err(retryErr).
				Str("candidateClusterID", candidateClusterID).
				Msg("failed to get AccountInfo from candidate cluster on retry")
			continue
		}

		log.Warn().
			Err(getAccountInfoErr).
			Str("candidateClusterID", candidateClusterID).
			Msg("failed to get AccountInfo from candidate cluster")
	}

	return &accountInfo
}

// Returns the AccountInfo for the given resource if it is an Account or Organization, otherwise returns nil.
func (s *IndexableResourceWatcherSubroutine) getAccountInfo(ctx context.Context, workspacePath string, gvk schema.GroupVersionKind, resource *unstructured.Unstructured) (*accountv1alpha1.AccountInfo, error) {
	if gvk.Group == v1alpha1.GroupName && (gvk.Kind == v1alpha1.AccountKind || gvk.Kind == v1alpha1.OrganizationKind) {
		// account and organization are both FGA account objects with the AccountInfo
		// in their own child workspace, use a direct lookup based on the current workspace path
		accountWorkspacePath := workspacePath + ":" + resource.GetName()
		ai, pathErr := s.getAccountInfoFromWorkspacePath(ctx, accountWorkspacePath)
		if pathErr != nil {
			return nil, fmt.Errorf("account info not found at path %q: %w", accountWorkspacePath, pathErr)
		}
		return ai, nil
	}
	return nil, nil
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

	// Use client.New with the cluster's config directly — cluster.GetClient() is scoped
	// to the APIExport's exported APIs and cannot reach core.kcp.io resources.
	cl, err := client.New(cluster.GetConfig(), client.Options{Scheme: cluster.GetScheme()})
	if err != nil {
		return "", "", fmt.Errorf("failed to create client for cluster %q: %w", id, err)
	}
	lc := &kcpcorev1alpha1.LogicalCluster{}
	if err := cl.Get(ctx, client.ObjectKey{Name: kcpcorev1alpha1.LogicalClusterName}, lc); err != nil {
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

func buildPayload(resource *unstructured.Unstructured) (string, string, error) {
	raw := resource.DeepCopy().Object
	if metadata, ok := raw["metadata"].(map[string]any); ok {
		delete(metadata, "managedFields")
	}

	jsonBytes, err := json.Marshal(raw)
	if err != nil {
		return "", "", err
	}

	yamlBytes, err := yaml.Marshal(raw)
	if err != nil {
		yamlBytes = jsonBytes
	}

	return string(jsonBytes), string(yamlBytes), nil
}

func mapResourceToFGAObject(group, kind, clusterID string, accountInfo *accountv1alpha1.AccountInfo) (fgaGroup, fgaKind, fgaClusterID string) {
	fgaGroup = group
	fgaKind = kind
	fgaClusterID = clusterID

	isAccount := group == v1alpha1.GroupName && kind == v1alpha1.AccountKind
	isOrganization := group == v1alpha1.GroupName && kind == v1alpha1.OrganizationKind
	isWorkspace := group == "tenancy.kcp.io" && kind == "Workspace"
	if isAccount || isWorkspace || isOrganization {
		fgaGroup = v1alpha1.GroupName
		fgaKind = v1alpha1.AccountKind
		if accountInfo != nil {
			switch {
			case isOrganization:
				if accountInfo.Spec.Organization.OriginClusterId != "" {
					fgaClusterID = accountInfo.Spec.Organization.OriginClusterId
				}
			case isAccount, isWorkspace:
				if accountInfo.Spec.Account.OriginClusterId != "" {
					fgaClusterID = accountInfo.Spec.Account.OriginClusterId
				}
			}
		}
	}

	return fgaGroup, fgaKind, fgaClusterID
}

func resolveAccountInfoLookupClusters(resource *unstructured.Unstructured, contextClusterID, resourceClusterID string) []string {
	candidates := []string{resourceClusterID, contextClusterID, resolveSpecClusterID(resource)}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if _, exists := seen[c]; exists {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out
}

func resolveSpecClusterID(resource *unstructured.Unstructured) string {
	if resource == nil {
		return ""
	}
	spec, ok := resource.Object["spec"].(map[string]any)
	if !ok {
		return ""
	}
	raw, ok := spec["cluster"]
	if !ok || raw == nil {
		return ""
	}
	clusterID, ok := raw.(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(clusterID)
}

// getAccountInfoFromWorkspacePath builds a workspace-scoped REST client from the base KCP
// config and fetches the singleton AccountInfo named "account" from that workspace.
func (s *IndexableResourceWatcherSubroutine) getAccountInfoFromWorkspacePath(ctx context.Context, accountWorkspacePath string) (*accountv1alpha1.AccountInfo, error) {
	scopedCfg := rest.CopyConfig(s.rootCfg)
	parsed, err := url.Parse(scopedCfg.Host)
	if err != nil {
		return nil, fmt.Errorf("parse KCP host URL: %w", err)
	}
	parsed.Path = fmt.Sprintf("/clusters/%s", accountWorkspacePath)
	scopedCfg.Host = parsed.String()

	cl, err := client.New(scopedCfg, client.Options{Scheme: s.mgr.GetLocalManager().GetScheme()})
	if err != nil {
		return nil, fmt.Errorf("create scoped client for %q: %w", accountWorkspacePath, err)
	}

	accountInfo := &accountv1alpha1.AccountInfo{}
	if err := cl.Get(ctx, client.ObjectKey{Name: "account"}, accountInfo); err != nil {
		return nil, fmt.Errorf("get AccountInfo from %q: %w", accountWorkspacePath, err)
	}

	return accountInfo, nil
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
		log.Debug().Err(err).Msg("Workspace not found, will retry")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	indexName, err := getSearchIndexForOrg(ctx, s.orgsClient, orgID)
	if err != nil {
		log.Debug().Err(err).Msg("could not get SearchIndex, requeuing")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if indexName == "" {
		log.Warn().Str("orgID", orgID).Msg("SearchIndex has no IndexName, cannot delete document")
		return ctrl.Result{}, nil
	}

	resourceClusterID := resolveResourceClusterID(resource, clusterID)
	docID := s.generateDocumentID(resource, resourceClusterID)
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

func buildFGAObjectName(group, kind, clusterID, name, namespace string) string {
	var namespacePtr *string
	if namespace != "" {
		namespacePtr = &namespace
	}

	// TODO rebac-auth-webhook uses singular resource names as the canonical basis for
	// OpenFGA object types. For our current resources, lowercase Kind matches the
	// singular form while keeping output stable.
	return fgamodel.BuildObjectName(group, strings.ToLower(kind), clusterID, name, namespacePtr)
}

func resolveResourceClusterID(resource *unstructured.Unstructured, fallbackClusterID string) string {
	if v := resource.GetAnnotations()[kcpClusterAnnotation]; strings.TrimSpace(v) != "" {
		return v
	}

	return fallbackClusterID
}

func addParentPermissions(doc *opensearch.ResourceDocument, tuples []*openfgav1.TupleKey) {
	for _, tuple := range tuples {
		if tuple == nil {
			continue
		}

		doc.AddPermission(tuple.User, tuple.Relation, tuple.Object)
	}
}

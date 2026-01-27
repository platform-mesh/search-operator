package subroutine

import (
	"context"
	"fmt"
	"time"

	kcpv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	accountv1alpha1 "github.com/platform-mesh/account-operator/api/v1alpha1"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/runtimeobject"
	lifecyclesubroutine "github.com/platform-mesh/golang-commons/controller/lifecycle/subroutine"
	"github.com/platform-mesh/golang-commons/errors"
	"github.com/platform-mesh/golang-commons/logger"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"

	"github.com/platform-mesh/search-operator/internal/opensearch"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// WorkspacesIndexName is the default index name for workspace documents
	WorkspacesIndexName = "platform-mesh-workspaces"
	// SearchIndexFinalizer is the finalizer for workspace indexing
	SearchIndexFinalizer = "search.platform-mesh.io/indexed"
)

// WorkspaceIndexingSubroutine indexes workspace/account data into OpenSearch
// when APIBindings are observed. It uses AccountInfo to get organization and
// account context for proper permission scoping.
type WorkspaceIndexingSubroutine struct {
	mgr       mcmanager.Manager
	allClient client.Client
	osClient  *opensearch.Client
}

// NewWorkspaceIndexingSubroutine creates a new workspace indexing subroutine
func NewWorkspaceIndexingSubroutine(mgr mcmanager.Manager, allClient client.Client, osClient *opensearch.Client) *WorkspaceIndexingSubroutine {
	return &WorkspaceIndexingSubroutine{
		mgr:       mgr,
		allClient: allClient,
		osClient:  osClient,
	}
}

var _ lifecyclesubroutine.Subroutine = &WorkspaceIndexingSubroutine{}

// GetName returns the subroutine name
func (s *WorkspaceIndexingSubroutine) GetName() string {
	return "WorkspaceIndexing"
}

// Finalizers returns the finalizers this subroutine manages
func (s *WorkspaceIndexingSubroutine) Finalizers(_ runtimeobject.RuntimeObject) []string {
	return []string{SearchIndexFinalizer}
}

// Process handles the indexing of workspace data when an APIBinding is reconciled
func (s *WorkspaceIndexingSubroutine) Process(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	log := logger.LoadLoggerFromContext(ctx)
	binding := instance.(*kcpv1alpha1.APIBinding)

	// Get workspace cluster name from context
	clusterName, ok := mccontext.ClusterFrom(ctx)
	if !ok {
		log.Warn().Msg("unable to get cluster from context, skipping indexing")
		return ctrl.Result{}, nil
	}

	// Get the cluster client for this workspace
	cluster, err := s.mgr.ClusterFromContext(ctx)
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(
			fmt.Errorf("unable to get cluster from context: %w", err), true, false)
	}

	// Try to get AccountInfo from the workspace to understand org/account context
	var accountInfo accountv1alpha1.AccountInfo
	err = cluster.GetClient().Get(ctx, types.NamespacedName{Name: "account"}, &accountInfo)

	// If AccountInfo doesn't exist (e.g., system workspaces), skip indexing
	if kerrors.IsNotFound(err) || meta.IsNoMatchError(err) {
		log.Debug().
			Str("workspace", clusterName).
			Str("binding", binding.Name).
			Msg("no AccountInfo found, skipping workspace indexing")
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(err, true, true)
	}

	// Only index when binding is in Bound phase
	if binding.Status.Phase != kcpv1alpha1.APIBindingPhaseBound {
		log.Debug().
			Str("workspace", clusterName).
			Str("binding", binding.Name).
			Str("phase", string(binding.Status.Phase)).
			Msg("binding not in Bound phase, skipping indexing")
		return ctrl.Result{}, nil
	}

	// Create workspace document from AccountInfo
	doc := s.createWorkspaceDocument(clusterName, &accountInfo, binding)

	// Ensure the index exists
	if err := s.osClient.CreateIndex(ctx, WorkspacesIndexName, workspacesIndexMapping); err != nil {
		return ctrl.Result{}, errors.NewOperatorError(
			fmt.Errorf("failed to create index: %w", err), true, true)
	}

	// Index the document
	docID := fmt.Sprintf("workspace-%s", clusterName)
	if err := s.osClient.IndexDocument(ctx, WorkspacesIndexName, docID, doc); err != nil {
		return ctrl.Result{}, errors.NewOperatorError(
			fmt.Errorf("failed to index workspace: %w", err), true, true)
	}

	log.Info().
		Str("workspace", clusterName).
		Str("docID", docID).
		Str("orgName", doc.OrganizationName).
		Str("accountName", doc.AccountName).
		Str("type", doc.Type).
		Int("permissions", len(doc.Permissions)).
		Msg("indexed workspace document")

	return ctrl.Result{}, nil
}

// Finalize removes the workspace document from OpenSearch when the APIBinding is deleted
func (s *WorkspaceIndexingSubroutine) Finalize(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	log := logger.LoadLoggerFromContext(ctx)
	binding := instance.(*kcpv1alpha1.APIBinding)

	// Get workspace cluster name from context
	clusterName, ok := mccontext.ClusterFrom(ctx)
	if !ok {
		log.Warn().Msg("unable to get cluster from context during finalize")
		return ctrl.Result{}, nil
	}

	// Delete the workspace document
	docID := fmt.Sprintf("workspace-%s", clusterName)
	if err := s.osClient.DeleteDocument(ctx, WorkspacesIndexName, docID); err != nil {
		log.Warn().
			Err(err).
			Str("workspace", clusterName).
			Str("docID", docID).
			Msg("failed to delete workspace document")
	} else {
		log.Info().
			Str("workspace", clusterName).
			Str("binding", binding.Name).
			Str("docID", docID).
			Msg("deleted workspace document from index")
	}

	return ctrl.Result{}, nil
}

// createWorkspaceDocument builds a WorkspaceDocument from AccountInfo
func (s *WorkspaceIndexingSubroutine) createWorkspaceDocument(
	clusterName string,
	accountInfo *accountv1alpha1.AccountInfo,
	binding *kcpv1alpha1.APIBinding,
) *opensearch.WorkspaceDocument {
	doc := opensearch.NewWorkspaceDocument(
		clusterName,
		accountInfo.Spec.Account.Name,
		string(accountInfo.Spec.Account.Type),
		clusterName,
		accountInfo.Spec.Account.Path,
	)

	// Set organization context
	doc.OrganizationID = accountInfo.Spec.Organization.GeneratedClusterId
	doc.OrganizationName = accountInfo.Spec.Organization.Name

	// Set account context (if different from org)
	if accountInfo.Spec.Account.Type == accountv1alpha1.AccountTypeAccount {
		doc.AccountID = accountInfo.Spec.Account.GeneratedClusterId
		doc.AccountName = accountInfo.Spec.Account.Name
	}

	// Add creation time if available
	if !accountInfo.CreationTimestamp.IsZero() {
		doc.CreatedAt = accountInfo.CreationTimestamp.Time
	}
	doc.UpdatedAt = time.Now()

	// Copy labels and annotations from AccountInfo
	if accountInfo.Labels != nil {
		doc.Labels = make(map[string]string)
		for k, v := range accountInfo.Labels {
			doc.Labels[k] = v
		}
	}
	if accountInfo.Annotations != nil {
		doc.Annotations = make(map[string]string)
		for k, v := range accountInfo.Annotations {
			doc.Annotations[k] = v
		}
	}

	// Add OpenFGA permission tuples
	// These tuples represent the basic permission structure for the workspace
	s.addPermissionTuples(doc, accountInfo)

	return doc
}

// addPermissionTuples adds OpenFGA tuples to the document for permission-aware search
func (s *WorkspaceIndexingSubroutine) addPermissionTuples(doc *opensearch.WorkspaceDocument, accountInfo *accountv1alpha1.AccountInfo) {
	accountType := fmt.Sprintf("core_platform-mesh_io_%s", accountInfo.Spec.Account.Type)
	objectID := fmt.Sprintf("%s:%s", accountType, doc.ClusterName)

	// Add organization-level access tuple
	// Members of the organization can access this workspace
	orgObject := fmt.Sprintf("core_platform-mesh_io_org:%s", accountInfo.Spec.Organization.GeneratedClusterId)
	doc.AddPermission(orgObject+"#member", "member", objectID)
	doc.AddPermission(orgObject+"#owner", "owner", objectID)

	// If this is an account (not an org), add account-level tuples
	if accountInfo.Spec.Account.Type == accountv1alpha1.AccountTypeAccount {
		accountObject := fmt.Sprintf("core_platform-mesh_io_account:%s", accountInfo.Spec.Account.GeneratedClusterId)
		doc.AddPermission(accountObject+"#member", "member", objectID)
		doc.AddPermission(accountObject+"#owner", "owner", objectID)
	}
}

// workspacesIndexMapping defines the OpenSearch mapping for the workspaces index
const workspacesIndexMapping = `{
	"settings": {
		"index": {
			"number_of_shards": 1,
			"number_of_replicas": 0
		}
	},
	"mappings": {
		"properties": {
			"id": { "type": "keyword" },
			"name": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
			"type": { "type": "keyword" },
			"cluster_name": { "type": "keyword" },
			"path": { "type": "keyword" },
			"organization_id": { "type": "keyword" },
			"organization_name": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
			"account_id": { "type": "keyword" },
			"account_name": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
			"permissions": {
				"type": "nested",
				"properties": {
					"user": { "type": "keyword" },
					"relation": { "type": "keyword" },
					"object": { "type": "keyword" }
				}
			},
			"created_at": { "type": "date" },
			"updated_at": { "type": "date" },
			"labels": { "type": "object", "enabled": true },
			"annotations": { "type": "object", "enabled": true }
		}
	}
}`

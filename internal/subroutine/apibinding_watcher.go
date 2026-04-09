package subroutine

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"

	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	kcpcore "github.com/kcp-dev/sdk/apis/core"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	kcptenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/runtimeobject"
	"github.com/platform-mesh/golang-commons/errors"
	"github.com/platform-mesh/golang-commons/logger"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"

	lifecyclesubroutine "github.com/platform-mesh/golang-commons/controller/lifecycle/subroutine"

	"github.com/platform-mesh/search-operator/api/v1alpha1"
)

// apiBindingWatcherSubroutine watches APIBinding resources across workspaces.
// When a binding takes place in an org then all indexes are updated for the
// fields contained in the bound APIResourceSchemas.
type apiBindingWatcherSubroutine struct {
	mgr        mcmanager.Manager
	orgsClient client.Client // scoped to root:orgs for Workspace lookups
	rootCfg    *rest.Config  // clean base KCP REST config (no path) for building workspace clients
}

// NewAPIBindingWatcherSubroutine creates a new APIBinding watcher subroutine.
// orgsClient must be scoped to the root:orgs workspace.
// localCfg must be the admin KCP REST config.
func NewAPIBindingWatcherSubroutine(mgr mcmanager.Manager, orgsClient client.Client, localCfg *rest.Config) (*apiBindingWatcherSubroutine, error) {
	rootCfg := rest.CopyConfig(localCfg)
	parsed, err := url.Parse(rootCfg.Host)
	if err != nil {
		return nil, fmt.Errorf("parse KCP host URL: %w", err)
	}
	parsed.Path = ""
	rootCfg.Host = parsed.String()

	return &apiBindingWatcherSubroutine{
		mgr:        mgr,
		orgsClient: orgsClient,
		rootCfg:    rootCfg,
	}, nil
}

var _ lifecyclesubroutine.Subroutine = &apiBindingWatcherSubroutine{}

func (s *apiBindingWatcherSubroutine) GetName() string {
	return "APIBindingWatcher"
}

func (s *apiBindingWatcherSubroutine) Finalizers(_ runtimeobject.RuntimeObject) []string {
	return nil
}

// Process ensures that a SearchIndex exists in the org workspace for each bound
// APIBinding, with DefaultFields populated from the top-level fields of all bound
// APIResourceSchemas.
func (s *apiBindingWatcherSubroutine) Process(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	log := logger.LoadLoggerFromContext(ctx)
	binding := instance.(*kcpapisv1alpha1.APIBinding)

	if binding.Status.Phase != kcpapisv1alpha1.APIBindingPhaseBound {
		log.Debug().
			Str("name", binding.Name).
			Str("phase", string(binding.Status.Phase)).
			Msg("APIBinding not yet bound, skipping")
		return ctrl.Result{}, nil
	}

	workspacePath, err := s.getWorkspacePath(ctx)
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("get workspace path: %w", err), true, false)
	}

	orgName, err := extractOrgNameFromPath(workspacePath)
	if err != nil {
		log.Debug().Str("workspacePath", workspacePath).Msg("APIBinding is not in an org workspace, skipping")
		return ctrl.Result{}, nil
	}

	orgClusterID, err := s.getOrgClusterID(ctx, orgName)
	if err != nil {
		log.Debug().Err(err).Str("orgName", orgName).Msg("org Workspace not found, requeuing")
		return ctrl.Result{Requeue: true}, nil
	}

	defaultFields, err := s.resolveDefaultFields(ctx, binding)
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("resolve default fields for binding %q: %w", binding.Name, err), true, false)
	}

	orgWorkspacePath := fmt.Sprintf("root:orgs:%s", orgName)
	if err := s.ensureSearchIndex(ctx, log, orgWorkspacePath, orgClusterID, binding.Name, defaultFields); err != nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("ensure SearchIndex for binding %q: %w", binding.Name, err), true, false)
	}

	return ctrl.Result{}, nil
}

// Finalize is a no-op: we do not remove the SearchIndex when a binding is deleted
// because the index may still hold indexed data that should persist.
// TODO: there should still be some strategy for cleanup of old SearchIndexes
func (s *apiBindingWatcherSubroutine) Finalize(_ context.Context, _ runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	return ctrl.Result{}, nil
}

// getWorkspacePath reads the LogicalCluster from the current workspace
// and returns its path annotation (e.g. "root:orgs:acme").
func (s *apiBindingWatcherSubroutine) getWorkspacePath(ctx context.Context) (string, error) {
	cluster, err := s.mgr.ClusterFromContext(ctx)
	if err != nil {
		return "", fmt.Errorf("get cluster from context: %w", err)
	}

	// cluster.GetClient() is scoped to the APIExport virtual workspace and cannot
	// reach core.kcp.io resources; use a direct client from the cluster config.
	cl, err := client.New(cluster.GetConfig(), client.Options{Scheme: cluster.GetScheme()})
	if err != nil {
		return "", fmt.Errorf("build cluster client: %w", err)
	}

	lc := &kcpcorev1alpha1.LogicalCluster{}
	if err := cl.Get(ctx, client.ObjectKey{Name: kcpcorev1alpha1.LogicalClusterName}, lc); err != nil {
		return "", fmt.Errorf("get LogicalCluster: %w", err)
	}

	path, ok := lc.Annotations[kcpcore.LogicalClusterPathAnnotationKey]
	if !ok {
		return "", fmt.Errorf("LogicalCluster missing %s annotation", kcpcore.LogicalClusterPathAnnotationKey)
	}

	return path, nil
}

// extractOrgNameFromPath parses "root:orgs:acme[:...]" and returns "acme".
func extractOrgNameFromPath(path string) (string, error) {
	parts := strings.Split(path, ":")
	if len(parts) < 3 || parts[0] != "root" || parts[1] != "orgs" {
		return "", fmt.Errorf("path %q is not under root:orgs", path)
	}
	return parts[2], nil
}

// getOrgClusterID returns the logical cluster ID of the org workspace by looking
// up the Workspace object in root:orgs.
func (s *apiBindingWatcherSubroutine) getOrgClusterID(ctx context.Context, orgName string) (string, error) {
	ws := &kcptenancyv1alpha1.Workspace{}
	if err := s.orgsClient.Get(ctx, types.NamespacedName{Name: orgName}, ws); err != nil {
		return "", fmt.Errorf("get Workspace %q in root:orgs: %w", orgName, err)
	}
	return ws.Spec.Cluster, nil
}

// resolveDefaultFields collects the top-level field names from every APIResourceSchema
// referenced by the binding. Fields are returned unique in sorted order.
func (s *apiBindingWatcherSubroutine) resolveDefaultFields(ctx context.Context, binding *kcpapisv1alpha1.APIBinding) ([]string, error) {
	if len(binding.Status.BoundResources) == 0 {
		return nil, nil
	}

	exportCluster, err := s.mgr.GetCluster(ctx, binding.Status.APIExportClusterName)
	if err != nil {
		return nil, fmt.Errorf("get export cluster %q: %w", binding.Status.APIExportClusterName, err)
	}
	exportClient := exportCluster.GetClient()

	seen := make(map[string]struct{})
	for _, br := range binding.Status.BoundResources {
		schema := &kcpapisv1alpha1.APIResourceSchema{}
		if err := exportClient.Get(ctx, types.NamespacedName{Name: br.Schema.Name}, schema); err != nil {
			return nil, fmt.Errorf("get APIResourceSchema %q: %w", br.Schema.Name, err)
		}

		for _, version := range schema.Spec.Versions {
			if !version.Served {
				continue
			}
			props, err := version.GetSchema()
			if err != nil {
				return nil, fmt.Errorf("parse schema for %q version %q: %w", br.Schema.Name, version.Name, err)
			}
			if props == nil {
				continue
			}
			for fieldName := range props.Properties {
				seen[fieldName] = struct{}{}
			}
		}
	}

	fields := make([]string, 0, len(seen))
	for f := range seen {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	return fields, nil
}

// ensureSearchIndex creates or updates the SearchIndex in the org workspace.
// The resource is named after the binding so each binding gets its own SearchIndex.
// TODO: maybe add a timestamp to avoid multiple edits of the SearchIndex if the
// APIResourceSchemas change and updates all bindings in an org
func (s *apiBindingWatcherSubroutine) ensureSearchIndex(
	ctx context.Context,
	log *logger.Logger,
	orgWorkspacePath string,
	orgClusterID string,
	bindingName string,
	defaultFields []string,
) error {
	orgClient, err := s.buildOrgClient(orgWorkspacePath)
	if err != nil {
		return fmt.Errorf("build org client for %q: %w", orgWorkspacePath, err)
	}

	searchIndexName := sanitizeResourceName(bindingName)
	existing := &v1alpha1.SearchIndex{}
	err = orgClient.Get(ctx, types.NamespacedName{Name: searchIndexName}, existing)

	switch {
	case apierrors.IsNotFound(err):
		desired := &v1alpha1.SearchIndex{
			ObjectMeta: metav1.ObjectMeta{
				Name: searchIndexName,
			},
			Spec: v1alpha1.SearchIndexSpec{
				IndexPrefix:           sanitizeIndexNamePart(bindingName),
				OrganizationClusterID: orgClusterID,
				NumberOfShards:        1,
				NumberOfReplicas:      1,
				DefaultFields:         defaultFields,
			},
		}
		if createErr := orgClient.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("create SearchIndex %q in %q: %w", searchIndexName, orgWorkspacePath, createErr)
		}
		log.Info().
			Str("searchIndex", searchIndexName).
			Str("orgWorkspace", orgWorkspacePath).
			Int("defaultFields", len(defaultFields)).
			Msg("created SearchIndex")

	case err != nil:
		return fmt.Errorf("get SearchIndex %q: %w", searchIndexName, err)

	default:
		if stringSlicesEqual(existing.Spec.DefaultFields, defaultFields) {
			return nil
		}
		updated := existing.DeepCopy()
		updated.Spec.DefaultFields = defaultFields
		if updateErr := orgClient.Update(ctx, updated); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return fmt.Errorf("conflict updating SearchIndex %q, will requeue: %w", searchIndexName, updateErr)
			}
			return fmt.Errorf("update SearchIndex %q in %q: %w", searchIndexName, orgWorkspacePath, updateErr)
		}
		log.Info().
			Str("searchIndex", searchIndexName).
			Str("orgWorkspace", orgWorkspacePath).
			Int("defaultFields", len(defaultFields)).
			Msg("updated SearchIndex default fields")
	}

	return nil
}

func (s *apiBindingWatcherSubroutine) buildOrgClient(workspacePath string) (client.Client, error) {
	cfg := rest.CopyConfig(s.rootCfg)
	cfg.Host = fmt.Sprintf("%s/clusters/%s", cfg.Host, workspacePath)
	return client.New(cfg, client.Options{Scheme: s.mgr.GetLocalManager().GetScheme()})
}

// stringSlicesEqual returns true when a and b contain the same elements in the same order.
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// sanitizeResourceName produces a valid lowercase Kubernetes name.
func sanitizeResourceName(name string) string {
	s := strings.ToLower(name)
	var b strings.Builder
	lastWasDash := false
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
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

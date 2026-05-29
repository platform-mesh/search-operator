package subroutine

import (
	"context"
	"fmt"
	"sort"
	"strings"

	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/runtimeobject"
	"github.com/platform-mesh/golang-commons/errors"
	"github.com/platform-mesh/golang-commons/logger"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
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
	mgr               mcmanager.Manager
	orgsClient        client.Client // scoped to root:orgs for Workspace lookups
	searchIndexClient client.Client // scoped to the provider workspace where SearchIndex config resources live
	rootCfg           *rest.Config  // clean base KCP REST config (no path) for building workspace clients
	indexPrefix       string
}

// NewAPIBindingWatcherSubroutine creates a new APIBinding watcher subroutine.
// orgsClient must be scoped to the root:orgs workspace.
// searchIndexClient must be scoped to the provider workspace.
// localCfg must be the admin KCP REST config.
func NewAPIBindingWatcherSubroutine(mgr mcmanager.Manager, orgsClient client.Client, searchIndexClient client.Client, localCfg *rest.Config, indexPrefix string) (*apiBindingWatcherSubroutine, error) {
	rootCfg, err := stripPathFromConfig(localCfg)
	if err != nil {
		return nil, err
	}

	return &apiBindingWatcherSubroutine{
		mgr:               mgr,
		orgsClient:        orgsClient,
		searchIndexClient: searchIndexClient,
		rootCfg:           rootCfg,
		indexPrefix:       indexPrefix,
	}, nil
}

var _ lifecyclesubroutine.Subroutine = &apiBindingWatcherSubroutine{}

func (s *apiBindingWatcherSubroutine) GetName() string {
	return "APIBindingWatcher"
}

func (s *apiBindingWatcherSubroutine) Finalizers(_ runtimeobject.RuntimeObject) []string {
	return nil
}

// Process ensures that a SearchIndex exists in the provider workspace for each bound
// APIBinding. Field lists are derived from the provider workspace schemas and
// optional SearchConfig resources.
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

	_, workspacePath, err := getWorkspaceClusterAndPath(ctx, s.mgr)
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("get workspace path: %w", err), true, false)
	}

	orgName, err := extractOrgFromPath(workspacePath)
	if err != nil {
		log.Debug().Str("workspacePath", workspacePath).Msg("APIBinding is not in an org workspace, skipping")
		return ctrl.Result{}, nil
	}

	orgClusterID, err := getOrgClusterID(ctx, s.orgsClient, orgName)
	if err != nil {
		log.Debug().Err(err).Str("orgName", orgName).Msg("org Workspace not found, requeuing")
		return ctrl.Result{Requeue: true}, nil
	}

	fields, err := s.resolveSearchIndexFields(ctx, binding)
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("resolve SearchIndex fields for binding %q: %w", binding.Name, err), true, false)
	}

	for _, br := range binding.Status.AppliedPermissionClaims {
		if err := s.ensureSearchIndex(ctx, log, orgName, orgClusterID, br.Resource, fields); err != nil {
			return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("ensure SearchIndex for binding %q resource %q: %w", binding.Name, br.Resource, err), true, false)
		}
	}

	return ctrl.Result{}, nil
}

// Finalize is a no-op: we do not remove the SearchIndex when a binding is deleted
// because the index may still hold indexed data that should persist.
// TODO: there should still be some strategy for cleanup of old SearchIndexes
func (s *apiBindingWatcherSubroutine) Finalize(_ context.Context, _ runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	return ctrl.Result{}, nil
}

type searchIndexFields struct {
	DefaultFields    []string
	SemanticFields   []string
	FilterableFields []string
}

var excludedSearchIndexFieldNames = []string{"password", "certificate", "crt", "cert"}

// resolveSearchIndexFields collects indexed field metadata from every APIResourceSchema
// referenced by the binding. If a matching SearchConfig exists in the provider
// workspace, it classifies fields explicitly; otherwise schema types drive the
// fallback field classification.
func (s *apiBindingWatcherSubroutine) resolveSearchIndexFields(ctx context.Context, binding *kcpapisv1alpha1.APIBinding) (searchIndexFields, error) {
	if len(binding.Status.BoundResources) == 0 {
		return searchIndexFields{}, nil
	}

	// The export cluster is the provider workspace that owns the APIExport.
	// It is not a consumer of the export, so it does not appear in the multicluster
	// manager's cluster list. Build a direct client using the cluster ID via the
	// clusters API instead of going through GetCluster.
	exportClient, err := buildClusterIDScopedClient(s.rootCfg, s.mgr.GetLocalManager().GetScheme(), binding.Status.APIExportClusterName)
	if err != nil {
		return searchIndexFields{}, fmt.Errorf("get export cluster client %q: %w", binding.Status.APIExportClusterName, err)
	}

	collector := newSearchIndexFieldCollector()
	for _, br := range binding.Status.BoundResources {
		schema := &kcpapisv1alpha1.APIResourceSchema{}
		if err := exportClient.Get(ctx, types.NamespacedName{Name: br.Schema.Name}, schema); err != nil {
			return searchIndexFields{}, fmt.Errorf("get APIResourceSchema %q: %w", br.Schema.Name, err)
		}

		schemaFields, err := schemaSearchIndexFields(schema)
		if err != nil {
			return searchIndexFields{}, err
		}

		searchConfig := s.fetchSearchConfig(ctx, exportClient, br.Schema.Name)
		if searchConfig == nil {
			collector.addFields(schemaFields)
			continue
		}

		collector.addSearchConfig(schemaFields, searchConfig)
	}

	return collector.fields(), nil
}

func (s *apiBindingWatcherSubroutine) fetchSearchConfig(ctx context.Context, exportClient client.Client, schemaName string) *v1alpha1.SearchConfig {
	log := logger.LoadLoggerFromContext(ctx)
	cfg := &v1alpha1.SearchConfig{}
	err := exportClient.Get(ctx, types.NamespacedName{Name: schemaName}, cfg)
	if err == nil {
		log.Debug().
			Str("searchConfig", cfg.Name).
			Str("schema", schemaName).
			Msg("found SearchConfig in provider workspace")
		return cfg
	}
	if !apierrors.IsNotFound(err) {
		log.Warn().Err(err).
			Str("schema", schemaName).
			Msg("error fetching SearchConfig from provider workspace, falling back to schema-derived fields")
	}
	return nil
}

func schemaSearchIndexFields(schema *kcpapisv1alpha1.APIResourceSchema) ([]schemaSearchIndexField, error) {
	fieldsByPath := make(map[string]schemaSearchIndexField)
	for _, version := range schema.Spec.Versions {
		if !version.Served {
			continue
		}
		props, err := version.GetSchema()
		if err != nil {
			return nil, fmt.Errorf("parse schema for %q version %q: %w", schema.Name, version.Name, err)
		}
		collectSchemaSearchIndexFields("", props, fieldsByPath)
	}

	fields := make([]schemaSearchIndexField, 0, len(fieldsByPath))
	for _, field := range fieldsByPath {
		fields = append(fields, field)
	}
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Path < fields[j].Path
	})
	return fields, nil
}

type schemaSearchIndexField struct {
	Path string
	Type string
}

func collectSchemaSearchIndexFields(prefix string, schema *apiextensionsv1.JSONSchemaProps, fields map[string]schemaSearchIndexField) {
	if schema == nil {
		return
	}

	if len(schema.Properties) == 0 {
		if prefix != "" && !isExcludedSearchIndexField(prefix) {
			fields[prefix] = schemaSearchIndexField{Path: prefix, Type: schema.Type}
		}
		return
	}

	for fieldName, fieldSchema := range schema.Properties {
		fieldPath := fieldName
		if prefix != "" {
			fieldPath = prefix + "." + fieldName
		}
		if isExcludedSearchIndexField(fieldPath) {
			continue
		}
		if fieldSchema.Type == "object" && len(fieldSchema.Properties) > 0 {
			collectSchemaSearchIndexFields(fieldPath, &fieldSchema, fields)
			continue
		}
		fields[fieldPath] = schemaSearchIndexField{Path: fieldPath, Type: fieldSchema.Type}
	}
}

// ensureSearchIndex creates or updates the SearchIndex in the provider workspace.
// The resource is named after the derived index prefix so each binding gets its own SearchIndex.
// TODO: maybe add a timestamp to avoid multiple edits of the SearchIndex if the
// APIResourceSchemas change and updates all bindings in an org
func (s *apiBindingWatcherSubroutine) ensureSearchIndex(
	ctx context.Context,
	log *logger.Logger,
	orgName string,
	orgClusterID string,
	resource string,
	fields searchIndexFields,
) error {
	searchIndexName := buildCanonicalIndexName(s.indexPrefix, orgClusterID, resource)
	existing := &v1alpha1.SearchIndex{}
	err := s.searchIndexClient.Get(ctx, types.NamespacedName{Name: searchIndexName}, existing)

	switch {
	case apierrors.IsNotFound(err):
		desired := &v1alpha1.SearchIndex{
			ObjectMeta: metav1.ObjectMeta{
				Name: searchIndexName,
			},
			Spec: v1alpha1.SearchIndexSpec{
				IndexPrefix:           sanitizeIndexNamePart(s.indexPrefix),
				OrganizationClusterID: orgClusterID,
				NumberOfShards:        1,
				NumberOfReplicas:      1,
				DefaultFields:         fields.DefaultFields,
				SemanticFields:        fields.SemanticFields,
				FilterableFields:      fields.FilterableFields,
			},
		}
		applySearchIndexOrgMetadata(desired, orgClusterID)
		if createErr := s.searchIndexClient.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("create SearchIndex %q for org %q: %w", searchIndexName, orgName, createErr)
		}
		log.Info().
			Str("searchIndex", searchIndexName).
			Str("orgName", orgName).
			Str("organizationClusterID", orgClusterID).
			Int("defaultFields", len(fields.DefaultFields)).
			Int("semanticFields", len(fields.SemanticFields)).
			Int("filterableFields", len(fields.FilterableFields)).
			Msg("created SearchIndex")

	case err != nil:
		return fmt.Errorf("get SearchIndex %q: %w", searchIndexName, err)

	default:
		updated := existing.DeepCopy()
		metadataChanged := applySearchIndexOrgMetadata(updated, orgClusterID)
		if stringSlicesEqual(existing.Spec.DefaultFields, fields.DefaultFields) &&
			stringSlicesEqual(existing.Spec.SemanticFields, fields.SemanticFields) &&
			stringSlicesEqual(existing.Spec.FilterableFields, fields.FilterableFields) &&
			!metadataChanged {
			return nil
		}
		updated.Spec.DefaultFields = fields.DefaultFields
		updated.Spec.SemanticFields = fields.SemanticFields
		updated.Spec.FilterableFields = fields.FilterableFields
		if updateErr := s.searchIndexClient.Update(ctx, updated); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return fmt.Errorf("conflict updating SearchIndex %q, will requeue: %w", searchIndexName, updateErr)
			}
			return fmt.Errorf("update SearchIndex %q for org %q: %w", searchIndexName, orgName, updateErr)
		}
		log.Info().
			Str("searchIndex", searchIndexName).
			Str("orgName", orgName).
			Str("organizationClusterID", orgClusterID).
			Int("defaultFields", len(fields.DefaultFields)).
			Int("semanticFields", len(fields.SemanticFields)).
			Int("filterableFields", len(fields.FilterableFields)).
			Msg("updated SearchIndex fields")
	}

	return nil
}

type searchIndexFieldCollector struct {
	defaultFields    map[string]struct{}
	semanticFields   map[string]struct{}
	filterableFields map[string]struct{}
}

func newSearchIndexFieldCollector() *searchIndexFieldCollector {
	return &searchIndexFieldCollector{
		defaultFields:    make(map[string]struct{}),
		semanticFields:   make(map[string]struct{}),
		filterableFields: make(map[string]struct{}),
	}
}

func (c *searchIndexFieldCollector) addFields(fields []schemaSearchIndexField) {
	for _, field := range fields {
		if isExcludedSearchIndexField(field.Path) {
			continue
		}

		c.defaultFields[field.Path] = struct{}{}
		if isFilterableSchemaType(field.Type) {
			c.filterableFields[field.Path] = struct{}{}
		}
		if field.Type == "string" {
			c.semanticFields[field.Path] = struct{}{}
		}
	}
}

func (c *searchIndexFieldCollector) addSearchConfig(fields []schemaSearchIndexField, cfg *v1alpha1.SearchConfig) {
	excluded := normalizedFieldSet(cfg.Spec.ExcludedFields)
	semantic := normalizedFieldSet(cfg.Spec.SemanticFields)
	exact := normalizedFieldSet(cfg.Spec.ExactFields)

	for _, field := range fields {
		switch {
		case isConfiguredFieldExcluded(field.Path, excluded):
			continue
		case exact[field.Path]:
			c.filterableFields[field.Path] = struct{}{}
		case semantic[field.Path]:
			c.semanticFields[field.Path] = struct{}{}
		default:
			c.defaultFields[field.Path] = struct{}{}
		}
	}

	for field := range semantic {
		if !isConfiguredFieldExcluded(field, excluded) {
			c.semanticFields[field] = struct{}{}
		}
	}
	for field := range exact {
		if !isConfiguredFieldExcluded(field, excluded) {
			c.filterableFields[field] = struct{}{}
		}
	}
}

func (c *searchIndexFieldCollector) fields() searchIndexFields {
	return searchIndexFields{
		DefaultFields:    sortedFieldNames(c.defaultFields),
		SemanticFields:   sortedFieldNames(c.semanticFields),
		FilterableFields: sortedFieldNames(c.filterableFields),
	}
}

func sortedFieldNames(fields map[string]struct{}) []string {
	out := make([]string, 0, len(fields))
	for f := range fields {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func normalizedFieldSet(fields []string) map[string]bool {
	out := make(map[string]bool, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		out[field] = true
	}
	return out
}

func isConfiguredFieldExcluded(field string, excluded map[string]bool) bool {
	if excluded[field] {
		return true
	}
	for excludedField := range excluded {
		if strings.HasPrefix(field, excludedField+".") {
			return true
		}
	}
	return false
}

func isFilterableSchemaType(schemaType string) bool {
	switch schemaType {
	case "string", "boolean", "integer", "number", "array":
		return true
	default:
		return false
	}
}

func isExcludedSearchIndexField(fieldName string) bool {
	normalized := normalizeSearchIndexFieldName(fieldName)
	for _, excluded := range excludedSearchIndexFieldNames {
		if strings.Contains(normalized, excluded) {
			return true
		}
	}
	return false
}

func normalizeSearchIndexFieldName(fieldName string) string {
	return strings.ToLower(strings.NewReplacer("-", "", "_", "").Replace(fieldName))
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

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
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"

	"github.com/platform-mesh/search-operator/api/v1alpha1"

	"github.com/platform-mesh/search-operator/internal/opensearch"
)

// IndexLifecycleSubroutine manages the lifecycle of OpenSearch indices
type IndexLifecycleSubroutine struct {
	osClient          *opensearch.Client
	staticIndexPrefix string
}

// NewIndexLifecycleSubroutine creates a new index lifecycle subroutine
func NewIndexLifecycleSubroutine(mgr mcmanager.Manager, osClient *opensearch.Client, staticIndexPrefix string) *IndexLifecycleSubroutine {
	return &IndexLifecycleSubroutine{
		osClient:          osClient,
		staticIndexPrefix: normalizePrefix(staticIndexPrefix),
	}
}

var _ lifecyclesubroutine.Subroutine = &IndexLifecycleSubroutine{}

const (
	searchIndexFinalizer = "search.platform-mesh.io/index"
)

// GetName returns the subroutine name
func (s *IndexLifecycleSubroutine) GetName() string {
	return "IndexLifecycle"
}

// Finalizers returns the finalizers this subroutine manages
func (s *IndexLifecycleSubroutine) Finalizers(instance runtimeobject.RuntimeObject) []string {
	_, ok := instance.(*v1alpha1.SearchIndex)
	if !ok {
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

	organizationClusterID := searchIndex.Spec.OrganizationClusterID
	if organizationClusterID == "" {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("missing required spec.organizationClusterID"), false, false)
	}
	specPrefix := searchIndex.Spec.IndexPrefix
	if specPrefix == "" {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("missing required spec.indexPrefix"), false, false)
	}

	paused := searchIndex.Spec.Paused

	trackedResources := searchIndex.Spec.TrackedResources
	if len(trackedResources) == 0 {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("this organization specifies no resources to add to index"), false, false)
	}

	numberShards := searchIndex.Spec.NumberOfShards
	if numberShards <= 0 {
		numberShards = 1
	}

	numReplicas := searchIndex.Spec.NumberOfReplicas
	if numReplicas < 0 {
		numReplicas = 0
	}
	desiredIndexName := buildCanonicalIndexName(s.staticIndexPrefix, specPrefix, organizationClusterID)

	log.Info().
		Str("name", searchIndex.GetName()).
		Str("organizationClusterID", organizationClusterID).
		Str("desiredIndexName", desiredIndexName).
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

	legacyIndexName := organizationClusterID
	useIndexName := desiredIndexName

	desiredExists, err := s.osClient.IndexExists(ctx, desiredIndexName)
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to check index existence for %q: %w", desiredIndexName, err), true, true)
	}
	legacyExists := false
	if !desiredExists {
		legacyExists, err = s.osClient.IndexExists(ctx, legacyIndexName)
		if err != nil {
			return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to check legacy index existence for %q: %w", legacyIndexName, err), true, true)
		}
		if legacyExists {
			useIndexName = legacyIndexName
		}
	}

	created := false
	replicasUpdated := false
	if !desiredExists && !legacyExists {
		if err := s.osClient.CreateIndex(ctx, desiredIndexName, numberShards, numReplicas, ""); err != nil {
			return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to create index %q: %w", desiredIndexName, err), true, true)
		}
		created = true
		useIndexName = desiredIndexName
	} else {
		currentSettings, settingsErr := s.osClient.GetIndexSettings(ctx, useIndexName)
		if settingsErr != nil {
			return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to read index settings for %q: %w", useIndexName, settingsErr), true, true)
		}

		if currentSettings.NumberOfShards != numberShards {
			return ctrl.Result{}, errors.NewOperatorError(
				fmt.Errorf(
					"cannot change number_of_shards for existing index %q (current=%d desired=%d); create a new index and reindex data",
					useIndexName,
					currentSettings.NumberOfShards,
					numberShards,
				),
				false,
				false,
			)
		}

		if currentSettings.NumberOfReplicas != numReplicas {
			if err := s.osClient.UpdateIndexReplicas(ctx, useIndexName, numReplicas); err != nil {
				return ctrl.Result{}, errors.NewOperatorError(
					fmt.Errorf("failed to update number_of_replicas for index %q to %d: %w", useIndexName, numReplicas, err),
					true,
					true,
				)
			}

			log.Info().
				Str("name", searchIndex.GetName()).
				Str("indexName", useIndexName).
				Int32("previousNumberOfReplicas", currentSettings.NumberOfReplicas).
				Int32("numberOfReplicas", numReplicas).
				Msg("updated existing index replicas")
			replicasUpdated = true
		}
	}

	aliases := buildIndexAliases(s.staticIndexPrefix, specPrefix, organizationClusterID, desiredIndexName)
	if err := s.osClient.EnsureAliases(ctx, useIndexName, aliases); err != nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to ensure aliases for index %q: %w", useIndexName, err), true, true)
	}

	statusChanged := false
	if searchIndex.Status.IndexName != useIndexName {
		searchIndex.Status.IndexName = useIndexName
		statusChanged = true
	}

	if created || replicasUpdated || statusChanged {
		searchIndex.Status.LastSyncTime = &v1.Time{Time: time.Now()}
		log.Info().
			Str("name", searchIndex.GetName()).
			Str("organizationClusterID", organizationClusterID).
			Str("indexName", useIndexName).
			Str("desiredIndexName", desiredIndexName).
			Bool("created", created).
			Bool("legacyIndexInUse", useIndexName == legacyIndexName && useIndexName != desiredIndexName).
			Bool("replicasUpdated", replicasUpdated).
			Bool("statusChanged", statusChanged).
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

	indexName, _, err := unstructured.NestedString(searchIndex.Object, "status", "indexName")
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to read status.indexName: %w", err), false, false)
	}
	if indexName == "" {
		organizationClusterID, _, specErr := unstructured.NestedString(searchIndex.Object, "spec", "organizationClusterID")
		if specErr != nil {
			return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to read spec.organizationClusterID during finalization: %w", specErr), false, false)
		}
		if organizationClusterID == "" {
			return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("missing spec.organizationClusterID during finalization"), false, false)
		}
		indexPrefix, _, prefixErr := unstructured.NestedString(searchIndex.Object, "spec", "indexPrefix")
		if prefixErr != nil {
			return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to read spec.indexPrefix during finalization: %w", prefixErr), false, false)
		}
		desiredIndexName := buildCanonicalIndexName(s.staticIndexPrefix, indexPrefix, organizationClusterID)
		desiredExists, existsErr := s.osClient.IndexExists(ctx, desiredIndexName)
		if existsErr != nil {
			return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to check desired index existence during finalization for %q: %w", desiredIndexName, existsErr), true, true)
		}
		if desiredExists {
			indexName = desiredIndexName
		} else {
			indexName = organizationClusterID
		}
	}

	log.Info().
		Str("name", searchIndex.GetName()).
		Str("reconcileWorkspace", workspaceName).
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

func buildCanonicalIndexName(staticPrefix, specPrefix, organizationClusterID string) string {
	parts := make([]string, 0, 3)

	if p := sanitizeIndexNamePart(staticPrefix); p != "" {
		parts = append(parts, p)
	}
	if p := sanitizeIndexNamePart(specPrefix); p != "" {
		parts = append(parts, p)
	}
	if p := sanitizeIndexNamePart(organizationClusterID); p != "" {
		parts = append(parts, p)
	}

	indexName := strings.Join(parts, "-")
	if len(indexName) > 255 {
		indexName = indexName[:255]
	}
	return strings.Trim(indexName, "-")
}

func buildIndexAliases(staticPrefix, specPrefix, organizationClusterID, canonicalIndexName string) []string {
	static := sanitizeIndexNamePart(staticPrefix)
	spec := sanitizeIndexNamePart(specPrefix)
	orgID := sanitizeIndexNamePart(organizationClusterID)
	canonical := sanitizeIndexNamePart(canonicalIndexName)

	aliases := make([]string, 0, 3)
	if static != "" {
		aliases = append(aliases, fmt.Sprintf("%s-all", static))
	}
	if static != "" && spec != "" {
		aliases = append(aliases, fmt.Sprintf("%s-%s-all", static, spec))
	}
	if canonical != "" && canonical != orgID {
		aliases = append(aliases, canonical)
	}

	return aliases
}

func normalizePrefix(value string) string {
	if sanitized := sanitizeIndexNamePart(value); sanitized != "" {
		return sanitized
	}
	return "pm"
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

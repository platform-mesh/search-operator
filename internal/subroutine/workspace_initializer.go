package subroutine

import (
	"context"
	"fmt"
	"strings"

	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/runtimeobject"
	"github.com/platform-mesh/golang-commons/errors"
	"github.com/platform-mesh/search-operator/internal/config"
	"github.com/platform-mesh/search-operator/internal/opensearch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
)

type workspaceInitializer struct {
	orgsClient      client.Client
	mgr             mcmanager.Manager
	osClient        *opensearch.Client
	cfg             config.Config
	coreModule      string
	initializerName string
}

func NewWorkspaceInitializer(orgsClient client.Client, cfg config.Config, mgr mcmanager.Manager, osClient *opensearch.Client) *workspaceInitializer {
	return &workspaceInitializer{
		orgsClient:      orgsClient,
		coreModule:      "",
		initializerName: cfg.InitializerName(),
		mgr:             mgr,
		osClient:        osClient,
		cfg:             cfg,
	}
}

func (w *workspaceInitializer) Process(ctx context.Context, instance runtimeobject.RuntimeObject) (ctrl.Result, errors.OperatorError) {
	lc := instance.(*kcpcorev1alpha1.LogicalCluster)

	wsName := getWorkspaceName(lc)
	if wsName == "" {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("failed to get workspace name"), true, false)
	}

	// Idempotent (does not fail if index already exists)
	err := w.osClient.CreateIndex(ctx, wsName, "")
	if err != nil {
		return ctrl.Result{}, errors.NewOperatorError(fmt.Errorf("unable to create index: %w", err), true, true)
	}

	return ctrl.Result{}, nil
}

func getWorkspaceName(lc *kcpcorev1alpha1.LogicalCluster) string {
	if path, ok := lc.Annotations["kcp.io/path"]; ok {
		pathElements := strings.Split(path, ":")
		return pathElements[len(pathElements)-1]
	}
	return ""
}

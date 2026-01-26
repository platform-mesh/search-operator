package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"os/signal"
	"syscall"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/platform-mesh/golang-commons/logger"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	// KCP imports
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	kcpcorev1alpha1 "github.com/kcp-dev/kcp/sdk/apis/core/v1alpha1"
	kcptenancyv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/tenancy/v1alpha1"
	"github.com/kcp-dev/multicluster-provider/apiexport"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"

	corev1alpha1 "github.com/platform-mesh/search-operator/api/v1alpha1"
	"github.com/platform-mesh/search-operator/internal/controller"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	// Add KCP types to scheme
	utilruntime.Must(apisv1alpha1.AddToScheme(scheme))
	utilruntime.Must(kcpcorev1alpha1.AddToScheme(scheme))
	utilruntime.Must(kcptenancyv1alpha1.AddToScheme(scheme))

	// Add our types
	utilruntime.Must(corev1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var kcpKubeconfig string
	var enableHTTP2 bool
	var maxConcurrentReconciles int
	var tlsOpts []func(*tls.Config)

	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&kcpKubeconfig, "kcp-kubeconfig", "/etc/kcp/kubeconfig",
		"Path to the KCP kubeconfig file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.IntVar(&maxConcurrentReconciles, "max-concurrent-reconciles", 1,
		"Maximum number of concurrent reconciles per controller")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Create logger for golang-commons
	logCfg := logger.DefaultConfig()
	log, err := logger.New(logCfg)
	if err != nil {
		setupLog.Error(err, "unable to create logger")
		os.Exit(1)
	}

	// Disable HTTP/2 due to vulnerabilities
	if !enableHTTP2 {
		disableHTTP2 := func(c *tls.Config) {
			setupLog.Info("disabling http/2")
			c.NextProtos = []string{"http/1.1"}
		}
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Get KCP config
	kcpCfg, err := getKCPConfig(kcpKubeconfig)
	if err != nil {
		setupLog.Error(err, "unable to get KCP config")
		os.Exit(1)
	}

	// Create KCP multicluster provider using APIExport
	provider, err := apiexport.New(kcpCfg, apiexport.Options{
		Scheme: scheme,
	})
	if err != nil {
		setupLog.Error(err, "unable to create cluster provider")
		os.Exit(1)
	}

	// Manager options
	mgrOpts := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
			TLSOpts:     tlsOpts,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "search-operator.platform-mesh.io",
	}

	// Create the smulticluster manager
	mgr, err := mcmanager.New(kcpCfg, provider, mgrOpts)
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	// Setup SearchIndex controller using lifecycle manager pattern
	if err := controller.NewSearchIndexReconciler(log, mgr).
		SetupWithManager(mgr, maxConcurrentReconciles); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "SearchIndex")
		os.Exit(1)
	}

	// Setup APIBinding controller for watching bindings across workspaces
	apiBindingReconciler, err := controller.NewAPIBindingReconciler(log, mgr)
	if err != nil {
		setupLog.Error(err, "unable to create APIBinding reconciler")
		os.Exit(1)
	}
	if err := apiBindingReconciler.SetupWithManager(mgr, maxConcurrentReconciles); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "APIBinding")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	defer cancel()

	go func() {
		setupLog.Info("starting cluster provider")
		if err := provider.Run(ctx, mgr); err != nil {
			setupLog.Error(err, "cluster provider failed")
			os.Exit(1)
		}
	}()

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// getKCPConfig loads the KCP kubeconfig from the specified path
func getKCPConfig(kubeconfigPath string) (*rest.Config, error) {
	// First check if KCP_KUBECONFIG env var is set
	if envPath := os.Getenv("KCP_KUBECONFIG"); envPath != "" {
		kubeconfigPath = envPath
	}

	return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
}

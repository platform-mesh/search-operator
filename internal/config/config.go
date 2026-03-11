package config

import (
	"github.com/vrischmann/envconfig"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Config holds the configuration for the search-operator
type Config struct {
	KCP struct {
		// Kubeconfig is the path to the KCP kubeconfig file
		Kubeconfig string `mapstructure:"kcp-kubeconfig" envconfig:"default=/api-kubeconfig/kubeconfig"`
	} `mapstructure:",squash"`

	SearchableResource struct {
		Resources []schema.GroupVersionKind `mapstructure:"resources" envconfig:"default={core.platform-mesh.io;v1alpha1;Account}"`
	} `mapstructure:",squash"`

	OpenSearch struct {
		// URL is the OpenSearch endpoint URL
		URL string `mapstructure:"opensearch-url"  envconfig:"default=https://opensearch.portal.localhost:8443"`
		// Username for OpenSearch authentication
		Username string `mapstructure:"opensearch-username" envconfig:"default=admin"`
		// Password for OpenSearch authentication
		Password string `mapstructure:"opensearch-password" envconfig:"default=admin"`
		// IndexNamePrefix is a static prefix for all operator-managed index names and aliases.
		IndexNamePrefix string `mapstructure:"opensearch-index-name-prefix" envconfig:"default=pm"`
	} `mapstructure:",squash"`
}

func (c Config) InitializerName() string {
	return "search"
}

// NewFromEnv creates a Config from environment values
func NewFromEnv() (*Config, error) {
	appConfig := Config{}
	err := envconfig.Init(&appConfig)
	return &appConfig, err
}

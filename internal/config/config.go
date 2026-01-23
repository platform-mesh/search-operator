package config

// Config holds the configuration for the search-operator
type Config struct {
	KCP struct {
		// Kubeconfig is the path to the KCP kubeconfig file
		Kubeconfig string `mapstructure:"kcp-kubeconfig" default:"/api-kubeconfig/kubeconfig"`
	} `mapstructure:",squash"`

	OpenSearch struct {
		// URL is the OpenSearch endpoint URL
		URL string `mapstructure:"opensearch-url"`
		// Username for OpenSearch authentication
		Username string `mapstructure:"opensearch-username"`
		// Password for OpenSearch authentication
		Password string `mapstructure:"opensearch-password"`
	} `mapstructure:",squash"`
}

package opensearch

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
	"github.com/platform-mesh/golang-commons/logger"
)

// Client wraps the OpenSearch client with convenience methods
type Client struct {
	api *opensearchapi.Client
}

type Config struct {
	// URL is the OpenSearch server URL (e.g., https://localhost:9200)
	URL string
	// Username for basic auth
	Username string
	// Password for basic auth
	Password string
	// InsecureSkipVerify skips TLS certificate verification (for development)
	InsecureSkipVerify bool
}

// NewClient creates a new OpenSearch client
func NewClient(cfg Config) (*Client, error) {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.InsecureSkipVerify,
		},
	}

	client, err := opensearchapi.NewClient(
		opensearchapi.Config{
			Client: opensearch.Config{
				Transport: transport,
				Addresses: []string{cfg.URL},
				Username:  cfg.Username,
				Password:  cfg.Password,
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenSearch client: %w", err)
	}

	return &Client{api: client}, nil
}

// NewClientFromEnv creates a new OpenSearch client using environment variables
// OPENSEARCH_URL, OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD
func NewClientFromEnv() (*Client, error) {
	url := os.Getenv("OPENSEARCH_URL")
	if url == "" {
		url = "https://localhost:9200"
	}

	insecure := os.Getenv("OPENSEARCH_INSECURE") == "true"

	return NewClient(Config{
		URL:                url,
		Username:           os.Getenv("OPENSEARCH_USERNAME"),
		Password:           os.Getenv("OPENSEARCH_PASSWORD"),
		InsecureSkipVerify: insecure,
	})
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.api.Info(ctx, nil)

	return err
}

// CreateIndex creates an index if it doesn't exist
func (c *Client) CreateIndex(ctx context.Context, indexName string, mapping string) error {
	log := logger.LoadLoggerFromContext(ctx)

	exists, err := c.IndexExists(ctx, indexName)
	if err != nil {
		return err
	}

	if exists {
		log.Debug().Str("index", indexName).Msg("index already exists")
		return nil
	}

	var body io.Reader
	if mapping != "" {
		body = strings.NewReader(mapping)
	}

	_, err = c.api.Indices.Create(
		ctx,
		opensearchapi.IndicesCreateReq{
			Index: indexName,
			Body:  body,
		},
	)
	if err != nil {
		var opensearchError *opensearch.StructError
		if errors.As(err, &opensearchError) {
			if opensearchError.Err.Type == "resource_already_exists_exception" {
				log.Debug().Str("index", indexName).Msg("index already exists (concurrent creation)")
				return nil
			}
		}
		return fmt.Errorf("failed to create index %s: %w", indexName, err)
	}

	log.Info().Str("index", indexName).Msg("created index")
	return nil
}

// IndexExists checks if an index exists
func (c *Client) IndexExists(ctx context.Context, indexName string) (bool, error) {
	resp, err := c.api.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{
		Indices: []string{indexName},
	})
	if err != nil {
		var opensearchError *opensearch.StructError
		if errors.As(err, &opensearchError) {
			if opensearchError.Err.Type == "index_not_found_exception" {
				return false, nil
			}
		}

		if resp.StatusCode == http.StatusNotFound {
			return false, nil
		}

		return false, fmt.Errorf("failed to check index existence: %w", err)
	}
	if resp == nil {
		return false, fmt.Errorf("failed to check index existence: empty response")
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return false, fmt.Errorf("failed to check index existence: status %s", resp.Status())
	}
	return true, nil
}

// DeleteIndex deletes an index
func (c *Client) DeleteIndex(ctx context.Context, indexName string) error {
	log := logger.LoadLoggerFromContext(ctx)

	exists, err := c.IndexExists(ctx, indexName)
	if err != nil {
		return err
	}

	if !exists {
		log.Debug().Str("index", indexName).Msg("index does not exist")
		return nil
	}

	_, err = c.api.Indices.Delete(ctx, opensearchapi.IndicesDeleteReq{
		Indices: []string{indexName},
	})
	if err != nil {
		return fmt.Errorf("failed to delete index %s: %w", indexName, err)
	}

	log.Info().Str("index", indexName).Msg("deleted index")
	return nil
}

// IndexDocument indexes a document
func (c *Client) IndexDocument(ctx context.Context, indexName, docID string, document interface{}) error {
	log := logger.LoadLoggerFromContext(ctx)

	resp, err := c.api.Index(
		ctx,
		opensearchapi.IndexReq{
			Index:      indexName,
			DocumentID: docID,
			Body:       opensearchutil.NewJSONReader(document),
			Params: opensearchapi.IndexParams{
				Refresh: "true", // Make document immediately searchable
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to index document %s in %s: %w", docID, indexName, err)
	}

	log.Debug().
		Str("index", resp.Index).
		Str("docID", resp.ID).
		Str("result", resp.Result).
		Msg("indexed document")

	return nil
}

// DeleteDocument deletes a document
func (c *Client) DeleteDocument(ctx context.Context, indexName, docID string) error {
	log := logger.LoadLoggerFromContext(ctx)

	resp, err := c.api.Document.Delete(ctx, opensearchapi.DocumentDeleteReq{
		Index:      indexName,
		DocumentID: docID,
	})
	if err != nil {
		return fmt.Errorf("failed to delete document %s from %s: %w", docID, indexName, err)
	}

	log.Debug().
		Str("index", resp.Index).
		Str("docID", docID).
		Str("result", resp.Result).
		Msg("deleted document")

	return nil
}

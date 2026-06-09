package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/elevran/charon/internal/config"
	"github.com/elevran/charon/internal/storage"
)

var _ storage.PayloadStore = (*PayloadStore)(nil)

// PayloadStore stores blobs in an S3-compatible object store.
// The key passed to Put/Get/Delete is used directly as the S3 object key.
type PayloadStore struct {
	client *awss3.Client
	bucket string
}

// Open creates an S3 PayloadStore from the given StorageConfig.
// If cfg.S3.EndpointURL is set, requests are directed there (MinIO / COS).
// If cfg.S3.AccessKeyID is non-empty, static credentials override the default chain.
func Open(cfg config.StorageConfig) (*PayloadStore, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(regionOrDefault(cfg.S3.Region)),
	}

	if cfg.S3.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.S3.AccessKeyID,
				cfg.S3.SecretAccessKey,
				"",
			),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("s3 load config: %w", err)
	}

	clientOpts := []func(*awss3.Options){}
	if cfg.S3.EndpointURL != "" {
		epURL := cfg.S3.EndpointURL
		clientOpts = append(clientOpts,
			func(o *awss3.Options) {
				o.BaseEndpoint = aws.String(epURL)
				o.UsePathStyle = cfg.S3.PathStyle
			},
		)
	}

	client := awss3.NewFromConfig(awsCfg, clientOpts...)
	return &PayloadStore{
		client: client,
		bucket: cfg.S3.Bucket,
	}, nil
}

func regionOrDefault(r string) string {
	if r == "" {
		return "us-east-1"
	}
	return r
}

// Put writes data to S3 at the given key, overwriting any existing object.
func (s *PayloadStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return fmt.Errorf("s3 put %q: %w", key, err)
	}
	return nil
}

// Get retrieves the object at key. Returns ErrNotFound if the object does not exist.
func (s *PayloadStore) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("s3 get %q: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3 read body %q: %w", key, err)
	}
	return data, nil
}

// Delete removes the object at key. Idempotent — returns nil if the object does not exist.
func (s *PayloadStore) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("s3 delete %q: %w", key, err)
	}
	return nil
}

// isNotFound returns true when err represents an S3 NoSuchKey or NotFound error.
func isNotFound(err error) bool {
	var noKey *types.NoSuchKey
	if errors.As(err, &noKey) {
		return true
	}
	// Some S3-compatible stores return a generic 404 via ResponseError
	var apiErr interface{ HTTPStatusCode() int }
	if errors.As(err, &apiErr) {
		return apiErr.HTTPStatusCode() == 404
	}
	return false
}

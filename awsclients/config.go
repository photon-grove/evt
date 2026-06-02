package awsclients

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// ResolveLocalEndpoint returns the configured AWS emulator endpoint.
//
// AWS_ENDPOINT_URL is the preferred variable. LOCALSTACK_ENDPOINT is accepted
// for compatibility with existing local-development scripts.
func ResolveLocalEndpoint() string {
	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		return endpoint
	}

	return os.Getenv("LOCALSTACK_ENDPOINT")
}

// NewConfig loads an AWS config. When localEndpoint is set, service clients use
// static throwaway credentials and route requests to the local emulator.
func NewConfig(ctx context.Context, region, localEndpoint string) (*aws.Config, error) {
	if localEndpoint == "" {
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			return nil, err
		}

		return &cfg, nil
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithRetryMaxAttempts(1),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				SessionToken:    "test",
				Source:          "local emulator credentials",
			},
		}),
		config.WithBaseEndpoint(localEndpoint),
	)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

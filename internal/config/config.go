package config

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/spf13/pflag"
)

type Config struct {
	Cursor           int
	CustomNSIDFilter string
	SQSQueueURL      string
	SQSClient        *sqs.Client
	RelayHost        string
}

func LoadConfig(fs *pflag.FlagSet) (*Config, error) {
	var cursor int
	var customNSID string
	fs.IntVar(&cursor, "cursor", 0, "Cursor to start from")
	fs.StringVar(&customNSID, "nsid", "", "Custom NSID to filter by")
	fs.Parse(os.Args[1:])

	sqsQueueURL := os.Getenv("SQS_QUEUE_URL")
	if sqsQueueURL == "" {
		return nil, errors.New("SQS_QUEUE_URL environment variable not set")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %w", err)
	}

	sqsClient := sqs.NewFromConfig(cfg)

	return &Config{
		Cursor:           cursor,
		CustomNSIDFilter: customNSID,
		SQSQueueURL:      sqsQueueURL,
		SQSClient:        sqsClient,
		RelayHost:        "wss://bsky.network",
	}, nil
}

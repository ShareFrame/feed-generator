package aws

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func SendMessageToSQS(ctx context.Context, client *sqs.Client, queueURL string, message string) error {
	_, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &queueURL,
		MessageBody: aws.String(message),
	})
	if err != nil {
		slog.Error("failed to send message to SQS", "err", err)
		return err
	}
	return nil
}

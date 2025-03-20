package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/ShareFrame/feed-generator/internal/aws"
	"github.com/ShareFrame/feed-generator/internal/config"
	"github.com/ShareFrame/feed-generator/internal/events"
	"github.com/gorilla/websocket"
	"github.com/spf13/pflag"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	fs := pflag.NewFlagSet("feed-generator", pflag.ContinueOnError)

	cfg, err := config.LoadConfig(fs)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	dialer := websocket.DefaultDialer
	u, err := url.Parse(cfg.RelayHost)
	if err != nil {
		slog.Error("invalid relayHost URI", "err", err)
		os.Exit(1)
	}
	u.Path = "xrpc/com.atproto.sync.subscribeRepos"
	if cfg.Cursor != 0 {
		u.RawQuery = fmt.Sprintf("cursor=%d", cfg.Cursor)
	}
	urlString := u.String()
	slog.Debug("GET", "url", urlString)
	con, _, err := dialer.Dial(urlString, http.Header{
		"User-Agent": []string{"ec2-firehose"},
	})
	if err != nil {
		slog.Error("subscribing to firehose failed (dialing)", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	sendMessageFunc := func(ctx context.Context, message string) error {
		return aws.SendMessageToSQS(ctx, cfg.SQSClient, cfg.SQSQueueURL, message)
	}

	err = events.HandleRepoStream(ctx, con, cfg.RelayHost, cfg.Cursor, cfg.CustomNSIDFilter, sendMessageFunc)
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("HandleRepoStream error", "err", err)
		os.Exit(1)
	}

	slog.Info("firehose consumer stopped", "cursor", cfg.Cursor)
}

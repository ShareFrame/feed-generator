package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/gorilla/websocket"

	"github.com/ShareFrame/feed-generator/internal/repo"
)

func HandleRepoStream(ctx context.Context, con *websocket.Conn, relayHost string, cursor int, customNSIDFilter string, sendMessageFunc func(ctx context.Context, message string) error) error {
	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			if isShareFrameMessage(evt) {
				messageBytes, err := json.Marshal(evt)
				if err != nil {
					slog.Error("failed to marshal json", "err", err)
					return err
				}
				slog.Info("Received ShareFrame message", "message", string(messageBytes))
			}

			slog.Debug("commit event", "did", evt.Repo, "seq", evt.Seq)
			return repo.HandleCommitEventOps(ctx, evt, customNSIDFilter, sendMessageFunc)
		},
	}

	scheduler := parallel.NewScheduler(1, 100, relayHost, rsc.EventHandler)
	slog.Info("starting firehose consumer", "relayHost", relayHost, "cursor", cursor)

	return events.HandleRepoStream(ctx, con, scheduler, nil)
}

func isShareFrameMessage(evt *comatproto.SyncSubscribeRepos_Commit) bool {
	for _, op := range evt.Ops {
		if strings.Contains(op.Path, "social.shareframe") {
			return true
		}
	}
	return false
}

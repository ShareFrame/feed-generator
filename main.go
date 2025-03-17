package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/data"
	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	lexutil "github.com/bluesky-social/indigo/lex/util"

	"github.com/gorilla/websocket"
)

type GoatFirehoseConsumer struct {
        EventLogger     *slog.Logger
        Cursor          int
        CustomNSIDFilter string
}

func main() {
        var cursor int
        var customNSID string
        flag.IntVar(&cursor, "cursor", 0, "Cursor to start from")
        flag.StringVar(&customNSID, "nsid", "", "Custom NSID to filter by")
        flag.Parse()

        slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

        gfc := GoatFirehoseConsumer{
                EventLogger:     slog.New(slog.NewJSONHandler(os.Stdout, nil)),
                Cursor:          cursor,
                CustomNSIDFilter: customNSID,
        }

        relayHost := "wss://bsky.network"
        dialer := websocket.DefaultDialer
        u, err := url.Parse(relayHost)
        if err != nil {
                slog.Error("invalid relayHost URI", "err", err)
                os.Exit(1)
        }
        u.Path = "xrpc/com.atproto.sync.subscribeRepos"
        if gfc.Cursor != 0 {
                u.RawQuery = fmt.Sprintf("cursor=%d", gfc.Cursor)
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

        rsc := &events.RepoStreamCallbacks{
                RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
                        slog.Debug("commit event", "did", evt.Repo, "seq", evt.Seq)
                        gfc.Cursor = int(evt.Seq)
                        return gfc.handleCommitEventOps(context.Background(), evt)
                },
        }

        scheduler := parallel.NewScheduler(
                1,
                100,
                relayHost,
                rsc.EventHandler,
        )
        slog.Info("starting firehose consumer", "relayHost", relayHost, "cursor", gfc.Cursor)

        ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
        defer cancel()

        err = events.HandleRepoStream(ctx, con, scheduler, nil)
        if err != nil && !errors.Is(err, context.Canceled) {
                slog.Error("HandleRepoStream error", "err", err)
                os.Exit(1)
        }

        slog.Info("firehose consumer stopped", "cursor", gfc.Cursor)
}

func (gfc *GoatFirehoseConsumer) handleCommitEventOps(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
        logger := slog.With("event", "commit", "did", evt.Repo, "rev", evt.Rev, "seq", evt.Seq)

        if evt.TooBig {
                logger.Warn("skipping tooBig events for now")
                return nil
        }

        _, rr, err := repo.LoadFromCAR(ctx, bytes.NewReader(evt.Blocks))
        if err != nil {
                logger.Error("failed to read repo from car", "err", err)
                return nil
        }

        for _, op := range evt.Ops {
                collection, rkey, err := syntax.ParseRepoPath(op.Path)
                if err != nil {
                        logger.Error("invalid path in repo op", "eventKind", op.Action, "path", op.Path)
                        return nil
                }
                logger = logger.With("eventKind", op.Action, "collection", collection, "rkey", rkey)

                if gfc.CustomNSIDFilter != "" && collection.String() != gfc.CustomNSIDFilter {
                        continue
                }

                out := make(map[string]interface{})
                out["seq"] = evt.Seq
                out["rev"] = evt.Rev
                out["time"] = evt.Time
                out["collection"] = collection
                out["rkey"] = rkey

                switch op.Action {
                case "create", "update":
                        coll, rkey, err := syntax.ParseRepoPath(op.Path)
                        if err != nil {
                                return err
                        }
                        recBytes, rc, err := rr.GetRecordBytes(ctx, coll, rkey)
                        if err != nil {
                                logger.Error("reading record from event blocks (CAR)", "err", err)
                                break
                        }
                        if op.Cid == nil || lexutil.LexLink(*rc) != *op.Cid {
                                logger.Error("mismatch between commit op CID and record block", "recordCID", rc, "opCID", op.Cid)
                                break
                        }

                        out["action"] = op.Action
                        d, err := data.UnmarshalCBOR(recBytes)
                        if err != nil {
                                slog.Warn("failed to parse record CBOR")
                                continue
                        }
                        out["cid"] = op.Cid.String()
                        out["record"] = d
                        b, err := json.Marshal(out)
                        if err != nil {
                                return err
                        }
                        fmt.Println(string(b))
                case "delete":
                        out["action"] = "delete"
                        b, err := json.Marshal(out)
                        if err != nil {
                                return err
                        }
                        fmt.Println(string(b))
                default:
                        logger.Error("unexpected record op kind")
                }
        }
        return nil
}
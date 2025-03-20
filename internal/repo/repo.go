package repo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/data"
	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lexutil "github.com/bluesky-social/indigo/lex/util"
)

func HandleCommitEventOps(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit, customNSIDFilter string, sendMessageFunc func(ctx context.Context, message string) error) error {
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

		if customNSIDFilter != "" && collection.String() != customNSIDFilter {
			continue
		}

		out := make(map[string]any)
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

			if sendMessageFunc != nil {
				if err := sendMessageFunc(ctx, string(b)); err != nil {
					return err
				}
			} else {
				fmt.Println(string(b))
			}

		case "delete":
			out["action"] = "delete"
			b, err := json.Marshal(out)
			if err != nil {
				return err
			}

			if sendMessageFunc != nil {
				if err := sendMessageFunc(ctx, string(b)); err != nil {
					return err
				}
			} else {
				fmt.Println(string(b))
			}
		default:
			logger.Error("unexpected record op kind")
		}
	}
	return nil
}

package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"

	"orbix-scheduler/internal/model"
)

const claimScanLimit = 64

func (q *Queue) ClaimNext(ctx context.Context, now time.Time) (*model.Ticket, bool, error) {
	resp, err := q.cli.Get(ctx, PendingPrefix,
		clientv3.WithPrefix(),
		clientv3.WithLimit(claimScanLimit),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, false, fmt.Errorf("queue: scan: %w", err)
	}
	for _, kv := range resp.Kvs {
		var t model.Ticket
		if err := json.Unmarshal(kv.Value, &t); err != nil {
			continue
		}
		if t.NotBefore != "" {
			if nb, err := time.Parse(time.RFC3339, t.NotBefore); err == nil && nb.After(now) {
				continue
			}
		}
		ok, err := q.claim(ctx, kv, &t)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return &t, true, nil
		}
		if t.NoOverlap && q.overlapHeld(ctx, t.JobID) {
			if err := q.skipStale(ctx, kv, &t); err != nil {
				return nil, false, err
			}
		}
	}
	return nil, false, nil
}

func (q *Queue) claim(ctx context.Context, kv *mvccpb.KeyValue, t *model.Ticket) (bool, error) {
	key := string(kv.Key)
	claimK := ClaimedPrefix + key[len(PendingPrefix):]
	t.Attempt++
	tb, err := json.Marshal(t)
	if err != nil {
		return false, err
	}
	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.Version(claimK), "=", 0),
		clientv3.Compare(clientv3.ModRevision(key), "=", kv.ModRevision),
	}
	then := []clientv3.Op{
		clientv3.OpPut(claimK, q.nodeID, clientv3.WithLease(q.claimLease())),
		clientv3.OpPut(key, string(tb)),
	}
	if t.NoOverlap {
		cmps = append(cmps, clientv3.Compare(clientv3.Version(OverlapKey(t.JobID)), "=", 0))
		then = append(then,
			clientv3.OpPut(OverlapKey(t.JobID), q.nodeID, clientv3.WithLease(q.claimLease())))
	}
	resp, err := q.cli.Txn(ctx).If(cmps...).Then(then...).Commit()
	return resp.Succeeded, err
}

func (q *Queue) overlapHeld(ctx context.Context, jobID int64) bool {
	resp, err := q.cli.Get(ctx, OverlapKey(jobID))
	return err == nil && len(resp.Kvs) > 0
}

func (q *Queue) skipStale(ctx context.Context, kv *mvccpb.KeyValue, t *model.Ticket) error {
	res := model.Result{
		JobID:     t.JobID,
		SchedAt:   t.SchedAt,
		Attempt:   t.Attempt,
		NodeID:    q.nodeID,
		Status:    model.StatusSkipped,
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}
	rb, err := json.Marshal(res)
	if err != nil {
		return err
	}
	_, err = q.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(string(kv.Key)), "=", kv.ModRevision)).
		Then(
			clientv3.OpDelete(string(kv.Key)),
			clientv3.OpPut(ResultKey(t), string(rb)),
		).Commit()
	return err
}

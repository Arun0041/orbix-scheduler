package queue

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"go.etcd.io/etcd/client/v3"

	"orbix-scheduler/internal/model"
)

type Claim struct {
	JobID   int64  `json:"job_id"`
	SchedAt string `json:"sched_at"`
	NodeID  string `json:"node_id"`
}

func (q *Queue) PendingTickets(ctx context.Context, limit int64) ([]model.Ticket, error) {
	if limit <= 0 {
		limit = 50
	}
	resp, err := q.cli.Get(ctx, PendingPrefix,
		clientv3.WithPrefix(),
		clientv3.WithLimit(limit),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, err
	}
	var out []model.Ticket
	for _, kv := range resp.Kvs {
		var t model.Ticket
		if json.Unmarshal(kv.Value, &t) == nil {
			out = append(out, t)
		}
	}
	return out, nil
}

func (q *Queue) CountPending(ctx context.Context) (int64, error) {
	resp, err := q.cli.Get(ctx, PendingPrefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func (q *Queue) Claims(ctx context.Context) ([]Claim, error) {
	resp, err := q.cli.Get(ctx, ClaimedPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var out []Claim
	for _, kv := range resp.Kvs {
		c := Claim{NodeID: string(kv.Value)}
		body := strings.TrimPrefix(string(kv.Key), ClaimedPrefix)
		if i := strings.Index(body, "@"); i > 0 {
			if id, err := strconv.ParseInt(body[:i], 10, 64); err == nil {
				c.JobID = id
				c.SchedAt = body[i+1:]
				out = append(out, c)
			}
		}
	}
	return out, nil
}

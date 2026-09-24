package queue

import (
	"context"
	"encoding/json"
	"time"

	"go.etcd.io/etcd/client/v3"

	"orbix-scheduler/internal/model"
)

func (q *Queue) GCResults(ctx context.Context, retention time.Duration, now time.Time) (int, error) {
	resp, err := q.cli.Get(ctx, ResultsPrefix,
		clientv3.WithPrefix(), clientv3.WithLimit(1000))
	if err != nil {
		return 0, err
	}
	var doomed []string
	for _, kv := range resp.Kvs {
		var r model.Result
		if json.Unmarshal(kv.Value, &r) != nil {
			continue
		}
		stamp := r.FinishedAt
		if stamp == "" {
			stamp = r.StartedAt
		}
		if stamp == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, stamp)
		if err != nil {
			continue
		}
		if now.Sub(t) > retention {
			doomed = append(doomed, string(kv.Key))
		}
	}
	const batch = 100
	for i := 0; i < len(doomed); i += batch {
		end := i + batch
		if end > len(doomed) {
			end = len(doomed)
		}
		ops := make([]clientv3.Op, 0, end-i)
		for _, k := range doomed[i:end] {
			ops = append(ops, clientv3.OpDelete(k))
		}
		if _, err := q.cli.Txn(ctx).Then(ops...).Commit(); err != nil {
			return i, err
		}
	}
	return len(doomed), nil
}
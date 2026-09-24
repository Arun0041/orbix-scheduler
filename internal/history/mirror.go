package history

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"go.etcd.io/etcd/client/v3"

	"orbix-scheduler/internal/model"
	"orbix-scheduler/internal/queue"
	"orbix-scheduler/internal/store"
)

type Mirror struct {
	cli   *clientv3.Client
	store *store.Store
}

func New(cli *clientv3.Client, st *store.Store) *Mirror {
	return &Mirror{cli: cli, store: st}
}

func (m *Mirror) Run(ctx context.Context) {
	for ctx.Err() == nil {
		resp, err := m.cli.Get(ctx, queue.ResultsPrefix, clientv3.WithPrefix())
		if err != nil {
			m.retry(ctx, err)
			continue
		}
		for _, kv := range resp.Kvs {
			m.put(string(kv.Key), kv.Value)
		}
		wch := m.cli.Watch(ctx, queue.ResultsPrefix,
			clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision+1))
		for wresp := range wch {
			for _, ev := range wresp.Events {
				if ev.Type == clientv3.EventTypePut {
					m.put(string(ev.Kv.Key), ev.Kv.Value)
				}
			}
		}
		if ctx.Err() != nil {
			return
		}
		m.retry(ctx, nil)
	}
}

func (m *Mirror) put(key string, val []byte) {
	var r model.Result
	if err := json.Unmarshal(val, &r); err != nil {
		log.Printf("[history] undecodable result %s: %v", key, err)
		return
	}
	if err := m.store.InsertRun(key, r); err != nil {
		log.Printf("[history] insert %s: %v", key, err)
	}
}

func (m *Mirror) retry(ctx context.Context, err error) {
	if err != nil {
		log.Printf("[history] watch: %v", err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
	}
}
package cluster

import (
	"context"
	"encoding/json"
	"time"

	"go.etcd.io/etcd/client/v3"

	"orbix-scheduler/internal/model"
)

func (n *Node) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := n.ensureSession(ctx); err != nil {
			return
		}
		if err := n.registerMember(ctx); err != nil {
			n.dropSession()
			n.sleep(ctx, time.Second)
			continue
		}
		won, err := n.campaign(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			n.dropSession()
			n.sleep(ctx, time.Second)
			continue
		}
		if won {
			n.isLeader.Store(true)
			n.leader.Store(model.LeaderInfo{
				NodeID: n.ID,
				Since:  time.Now().UTC().Format(time.RFC3339),
			})
			n.emit(Event{Kind: LeaderGained})
			<-n.sessionDone()
			n.isLeader.Store(false)
			n.leader.Store(model.LeaderInfo{})
			n.emit(Event{Kind: LeaderLost})
			n.dropSession()
			continue
		}
		if !n.follow(ctx) {
			return
		}
	}
}

func (n *Node) campaign(ctx context.Context) (bool, error) {
	info, _ := json.Marshal(model.LeaderInfo{
		NodeID: n.ID,
		Since:  time.Now().UTC().Format(time.RFC3339),
	})
	resp, err := n.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(leaderKey), "=", 0)).
		Then(clientv3.OpPut(leaderKey, string(info), clientv3.WithLease(n.LeaseID()))).
		Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

func (n *Node) follow(ctx context.Context) bool {
	resp, err := n.client.Get(ctx, leaderKey)
	if err != nil {
		n.dropSession()
		n.sleep(ctx, time.Second)
		return ctx.Err() == nil
	}
	if len(resp.Kvs) == 0 {
		return ctx.Err() == nil
	}
	n.rememberLeader(resp.Kvs[0].Value)
	wch := n.client.Watch(ctx, leaderKey, clientv3.WithRev(resp.Header.Revision+1))
	sessDone := n.sessionDone()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-sessDone:
			n.dropSession()
			return true
		case wresp, ok := <-wch:
			if !ok {
				return ctx.Err() == nil
			}
			for _, ev := range wresp.Events {
				if ev.Type == clientv3.EventTypeDelete {
					n.leader.Store(model.LeaderInfo{})
					return true
				}
				n.rememberLeader(ev.Kv.Value)
			}
		}
	}
}

func (n *Node) rememberLeader(v []byte) {
	var li model.LeaderInfo
	if json.Unmarshal(v, &li) == nil {
		n.leader.Store(li)
	}
}

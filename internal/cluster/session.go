package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"orbix-scheduler/internal/config"
	"orbix-scheduler/internal/model"
)

const (
	leaderKey     = "/cron/leader"
	membersPrefix = "/cron/members/"
)

type EventKind int

const (
	LeaderGained EventKind = iota
	LeaderLost
)

type Event struct {
	Kind EventKind
}

type Node struct {
	ID     string
	cfg    *config.Config
	client *clientv3.Client

	mu      sync.Mutex
	session *concurrency.Session

	claimMu      sync.Mutex
	claimSession *concurrency.Session

	isLeader atomic.Bool
	leader   atomic.Value
	events   chan Event
}

func Connect(ctx context.Context, cfg *config.Config) (*Node, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout:  5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("cluster: etcd dial: %w", err)
	}
	n := &Node{
		ID:     cfg.NodeID,
		cfg:    cfg,
		client: cli,
		events: make(chan Event, 8),
	}
	n.leader.Store(model.LeaderInfo{})
	if err := n.ensureSession(ctx); err != nil {
		cli.Close()
		return nil, err
	}
	if err := n.ensureClaimSession(ctx); err != nil {
		n.dropSession()
		cli.Close()
		return nil, err
	}
	return n, nil
}

func (n *Node) Client() *clientv3.Client { return n.client }

func (n *Node) LeaseID() clientv3.LeaseID {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.session == nil {
		return clientv3.NoLease
	}
	return n.session.Lease()
}

func (n *Node) IsLeader() bool { return n.isLeader.Load() }

func (n *Node) Leader() model.LeaderInfo {
	v, _ := n.leader.Load().(model.LeaderInfo)
	return v
}

func (n *Node) Events() <-chan Event { return n.events }

func (n *Node) Close() {
	n.dropClaimSession()
	n.dropSession()
	n.client.Close()
}

func (n *Node) ensureSession(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.session != nil {
		return nil
	}
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		sess, err := concurrency.NewSession(n.client, concurrency.WithTTL(n.cfg.ElectionTTLSec))
		if err == nil {
			n.session = sess
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			if backoff < 8*time.Second {
				backoff *= 2
			}
		}
	}
}

func (n *Node) ensureClaimSession(ctx context.Context) error {
	n.claimMu.Lock()
	defer n.claimMu.Unlock()
	if n.claimSession != nil {
		return nil
	}
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		sess, err := concurrency.NewSession(n.client, concurrency.WithTTL(n.cfg.WorkerLeaseSec))
		if err == nil {
			n.claimSession = sess
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			if backoff < 8*time.Second {
				backoff *= 2
			}
		}
	}
}

func (n *Node) ClaimLeaseID() clientv3.LeaseID {
	n.claimMu.Lock()
	defer n.claimMu.Unlock()
	if n.claimSession == nil {
		return clientv3.NoLease
	}
	return n.claimSession.Lease()
}

func (n *Node) MaintainClaims(ctx context.Context) {
	for ctx.Err() == nil {
		if err := n.ensureClaimSession(ctx); err != nil {
			return
		}
		done := n.claimDone()
		select {
		case <-ctx.Done():
			return
		case <-done:
			n.dropClaimSession()
			n.sleep(ctx, time.Second)
		}
	}
}

func (n *Node) claimDone() <-chan struct{} {
	n.claimMu.Lock()
	defer n.claimMu.Unlock()
	if n.claimSession == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return n.claimSession.Done()
}

func (n *Node) dropClaimSession() {
	n.claimMu.Lock()
	defer n.claimMu.Unlock()
	if n.claimSession != nil {
		n.claimSession.Close()
		n.claimSession = nil
	}
}

func (n *Node) registerMember(ctx context.Context) error {
	n.mu.Lock()
	sess := n.session
	n.mu.Unlock()
	if sess == nil {
		return fmt.Errorf("cluster: no session")
	}
	m := model.Member{
		NodeID:   n.ID,
		Addr:     n.cfg.HTTPListen,
		JoinedAt: time.Now().UTC().Format(time.RFC3339),
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = n.client.Put(ctx, membersPrefix+n.ID, string(b), clientv3.WithLease(sess.Lease()))
	return err
}

func (n *Node) Members(ctx context.Context) ([]model.Member, error) {
	resp, err := n.client.Get(ctx, membersPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var out []model.Member
	for _, kv := range resp.Kvs {
		var m model.Member
		if json.Unmarshal(kv.Value, &m) == nil {
			out = append(out, m)
		}
	}
	return out, nil
}

func (n *Node) sessionDone() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.session == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return n.session.Done()
}

func (n *Node) dropSession() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.session != nil {
		n.session.Close()
		n.session = nil
	}
}

func (n *Node) emit(e Event) {
	select {
	case n.events <- e:
	default:
	}
}

func (n *Node) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

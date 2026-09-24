package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"go.etcd.io/etcd/client/v3"

	"orbix-scheduler/internal/model"
)

const (
	PendingPrefix   = "/cron/tasks/pending/"
	ClaimedPrefix   = "/cron/tasks/claimed/"
	ResultsPrefix   = "/cron/results/"
	OverlapPrefix   = "/cron/overlap/"
	WatermarkPrefix = "/cron/watermark/"
)

type Queue struct {
	cli        *clientv3.Client
	nodeID     string
	claimLease func() clientv3.LeaseID
}

func New(cli *clientv3.Client, nodeID string, claimLease func() clientv3.LeaseID) *Queue {
	return &Queue{cli: cli, nodeID: nodeID, claimLease: claimLease}
}

func stamp(t *model.Ticket) string {
	return fmt.Sprintf("%d@%s", t.JobID, t.SchedAt)
}

func PendingKey(t *model.Ticket) string { return PendingPrefix + stamp(t) }
func ClaimedKey(t *model.Ticket) string { return ClaimedPrefix + stamp(t) }
func OverlapKey(jobID int64) string     { return OverlapPrefix + strconv.FormatInt(jobID, 10) }
func WatermarkKey(jobID int64) string   { return WatermarkPrefix + strconv.FormatInt(jobID, 10) }
func ResultKey(t *model.Ticket) string {
	return ResultsPrefix + stamp(t) + "#" + strconv.Itoa(t.Attempt)
}

func (q *Queue) Enqueue(ctx context.Context, t model.Ticket) (bool, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return false, err
	}
	key := PendingKey(&t)
	resp, err := q.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, string(b))).
		Commit()
	if err != nil {
		return false, fmt.Errorf("queue: enqueue: %w", err)
	}
	return resp.Succeeded, nil
}

func (q *Queue) EnqueueFire(ctx context.Context, t model.Ticket, fireTime string) (bool, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return false, err
	}
	key := PendingKey(&t)
	resp, err := q.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(
			clientv3.OpPut(key, string(b)),
			clientv3.OpPut(WatermarkKey(t.JobID), fireTime),
		).
		Commit()
	if err != nil {
		return false, fmt.Errorf("queue: enqueue fire: %w", err)
	}
	return resp.Succeeded, nil
}

func (q *Queue) AdvanceWatermark(ctx context.Context, jobID int64, fireTime string) error {
	if _, err := q.cli.Put(ctx, WatermarkKey(jobID), fireTime); err != nil {
		return fmt.Errorf("queue: advance watermark: %w", err)
	}
	return nil
}

func (q *Queue) Watermark(ctx context.Context, jobID int64) (string, bool, error) {
	resp, err := q.cli.Get(ctx, WatermarkKey(jobID))
	if err != nil {
		return "", false, fmt.Errorf("queue: watermark: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return "", false, nil
	}
	return string(resp.Kvs[0].Value), true, nil
}

func (q *Queue) Trigger(ctx context.Context, j *model.Job) error {
	now := time.Now().UTC()
	t := model.Ticket{
		JobID:      j.ID,
		Name:       j.Name,
		Command:    j.Command,
		TimeoutSec: j.TimeoutSec,
		MaxRetries: j.MaxRetries,
		NoOverlap:  j.NoOverlap,
		SchedAt:    manualStamp(now),
		Manual:     true,
	}
	_, err := q.Enqueue(ctx, t)
	return err
}

func manualStamp(now time.Time) string {
	return now.Format(time.RFC3339Nano) + fmt.Sprintf("-m%04x", rand.Intn(0xFFFF))
}

func Backoff(attempt int, now time.Time) time.Time {
	d := time.Duration(1<<uint(attempt)) * time.Second
	if d > 5*time.Minute {
		d = 5 * time.Minute
	}
	return now.Add(d)
}

func (q *Queue) FinishSuccess(ctx context.Context, t *model.Ticket, res model.Result) error {
	rb, err := json.Marshal(res)
	if err != nil {
		return err
	}
	pub := clientv3.OpPut(ResultKey(t), string(rb))
	then := []clientv3.Op{
		clientv3.OpDelete(PendingKey(t)),
		clientv3.OpDelete(ClaimedKey(t)),
		pub,
	}
	then = append(then, overlapRelease(t)...)
	_, err = q.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(ClaimedKey(t)), "=", q.nodeID)).
		Then(then...).
		Else(pub).
		Commit()
	if err != nil {
		return fmt.Errorf("queue: finish success: %w", err)
	}
	return nil
}

func (q *Queue) FinishFailure(ctx context.Context, t *model.Ticket, res model.Result, now time.Time) error {
	giveUp := t.Attempt > t.MaxRetries
	if res.Status == "" {
		res.Status = model.StatusFailed
	}
	if giveUp {
		res.Status = model.StatusFailedPermanent
	}
	rb, err := json.Marshal(res)
	if err != nil {
		return err
	}
	pub := clientv3.OpPut(ResultKey(t), string(rb))
	var then []clientv3.Op
	if giveUp {
		then = []clientv3.Op{
			clientv3.OpDelete(PendingKey(t)),
			clientv3.OpDelete(ClaimedKey(t)),
			pub,
		}
	} else {
		t.NotBefore = Backoff(t.Attempt, now).UTC().Format(time.RFC3339)
		tb, err := json.Marshal(t)
		if err != nil {
			return err
		}
		then = []clientv3.Op{
			clientv3.OpPut(PendingKey(t), string(tb)),
			clientv3.OpDelete(ClaimedKey(t)),
			pub,
		}
	}
	then = append(then, overlapRelease(t)...)
	_, err = q.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(ClaimedKey(t)), "=", q.nodeID)).
		Then(then...).
		Else(pub).
		Commit()
	if err != nil {
		return fmt.Errorf("queue: finish failure: %w", err)
	}
	return nil
}

func (q *Queue) Abandon(ctx context.Context, t model.Ticket) error {
	res := model.Result{
		JobID:     t.JobID,
		SchedAt:   t.SchedAt,
		Attempt:   t.Attempt,
		NodeID:    q.nodeID,
		Status:    model.StatusAbandoned,
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}
	rb, err := json.Marshal(res)
	if err != nil {
		return err
	}
	ck := ClaimedKey(&t)
	_, err = q.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(ck), "=", 0)).
		Then(
			clientv3.OpDelete(PendingKey(&t)),
			clientv3.OpPut(ResultKey(&t), string(rb)),
		).Commit()
	return err
}

func overlapRelease(t *model.Ticket) []clientv3.Op {
	if !t.NoOverlap {
		return nil
	}
	return []clientv3.Op{clientv3.OpDelete(OverlapKey(t.JobID))}
}

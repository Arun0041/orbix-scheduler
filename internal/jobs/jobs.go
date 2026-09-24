package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/client/v3"

	"orbix-scheduler/internal/cron"
	"orbix-scheduler/internal/model"
	"orbix-scheduler/internal/queue"
)

const (
	JobsPrefix   = "/cron/jobs/"
	idCounterKey = "/cron/jobs/_counter"
)

type Repo struct {
	cli *clientv3.Client
}

func New(cli *clientv3.Client) *Repo { return &Repo{cli: cli} }

func key(id int64) string { return JobsPrefix + strconv.FormatInt(id, 10) }

func (r *Repo) Create(ctx context.Context, j *model.Job) error {
	if err := validate(j); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	j.CreatedAt, j.UpdatedAt = now, now
	j.Enabled = true

	for attempt := 0; attempt < 8; attempt++ {
		cur, modrev := int64(0), int64(0)
		resp, err := r.cli.Get(ctx, idCounterKey)
		if err != nil {
			return fmt.Errorf("jobs: read counter: %w", err)
		}
		if len(resp.Kvs) > 0 {
			if v, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64); err == nil {
				cur = v
			}
			modrev = resp.Kvs[0].ModRevision
		}
		j.ID = cur + 1
		b, err := json.Marshal(j)
		if err != nil {
			return err
		}
		txn, err := r.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(idCounterKey), "=", modrev)).
			Then(
				clientv3.OpPut(idCounterKey, strconv.FormatInt(j.ID, 10)),
				clientv3.OpPut(key(j.ID), string(b)),
			).Commit()
		if err != nil {
			return fmt.Errorf("jobs: create txn: %w", err)
		}
		if txn.Succeeded {
			return nil
		}
	}
	return fmt.Errorf("jobs: id allocation contention, retry")
}

func validate(j *model.Job) error {
	if strings.TrimSpace(j.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if strings.TrimSpace(j.Command) == "" {
		return fmt.Errorf("command is required")
	}
	if err := cron.Validate(j.Cron); err != nil {
		return err
	}
	if j.Timezone != "" {
		if _, err := time.LoadLocation(j.Timezone); err != nil {
			return fmt.Errorf("unknown timezone %q", j.Timezone)
		}
	}
	switch j.CatchUp {
	case "", model.CatchUpRunLatest, model.CatchUpRunAll, model.CatchUpSkip:
	default:
		return fmt.Errorf("catch_up must be run_latest|run_all|skip")
	}
	if j.TimeoutSec < 0 || j.TimeoutSec > 86400 {
		return fmt.Errorf("timeout_s must be between 0 and 86400")
	}
	if j.MaxRetries < 0 || j.MaxRetries > 10 {
		return fmt.Errorf("max_retries must be between 0 and 10")
	}
	return nil
}

func (r *Repo) Get(ctx context.Context, id int64) (*model.Job, error) {
	resp, err := r.cli.Get(ctx, key(id))
	if err != nil {
		return nil, fmt.Errorf("jobs: get: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("job %d not found", id)
	}
	var j model.Job
	if err := json.Unmarshal(resp.Kvs[0].Value, &j); err != nil {
		return nil, fmt.Errorf("jobs: decode: %w", err)
	}
	return &j, nil
}

func (r *Repo) List(ctx context.Context) ([]model.Job, error) {
	resp, err := r.cli.Get(ctx, JobsPrefix,
		clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, fmt.Errorf("jobs: list: %w", err)
	}
	var out []model.Job
	for _, kv := range resp.Kvs {
		var j model.Job
		if json.Unmarshal(kv.Value, &j) == nil {
			out = append(out, j)
		}
	}
	return out, nil
}

func (r *Repo) Update(ctx context.Context, id int64, j *model.Job) (*model.Job, error) {
	j.ID = id
	if err := validate(j); err != nil {
		return nil, err
	}
	cur, err := r.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	j.CreatedAt = cur.CreatedAt
	j.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	b, err := json.Marshal(j)
	if err != nil {
		return nil, err
	}
	if _, err := r.cli.Put(ctx, key(id), string(b)); err != nil {
		return nil, fmt.Errorf("jobs: update: %w", err)
	}
	return j, nil
}

func (r *Repo) SetEnabled(ctx context.Context, id int64, enabled bool) (*model.Job, error) {
	j, err := r.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	j.Enabled = enabled
	j.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	b, err := json.Marshal(j)
	if err != nil {
		return nil, err
	}
	if _, err := r.cli.Put(ctx, key(id), string(b)); err != nil {
		return nil, fmt.Errorf("jobs: toggle: %w", err)
	}
	return j, nil
}

func (r *Repo) Delete(ctx context.Context, id int64) error {
	_, err := r.cli.Txn(ctx).
		Then(
			clientv3.OpDelete(key(id)),
			clientv3.OpDelete(queue.WatermarkKey(id)),
		).Commit()
	if err != nil {
		return fmt.Errorf("jobs: delete: %w", err)
	}
	return nil
}


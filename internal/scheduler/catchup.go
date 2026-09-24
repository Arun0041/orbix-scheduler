package scheduler

import (
	"context"
	"log"
	"time"

	"orbix-scheduler/internal/cron"
	"orbix-scheduler/internal/model"
)

func (s *Scheduler) scheduleJob(ctx context.Context, j *model.Job, now time.Time) {
	policy := j.CatchUp
	if policy == "" {
		policy = model.CatchUpRunLatest
	}
	wmStr, hasWM, err := s.q.Watermark(ctx, j.ID)
	if err != nil {
		log.Printf("[sched] watermark %d: %v", j.ID, err)
		return
	}
	var from time.Time
	if hasWM {
		if from, err = time.Parse(time.RFC3339, wmStr); err != nil {
			from = time.Time{}
		}
	}
	if from.IsZero() {
		nxt, err := cron.NextAfter(j.Cron, j.Timezone, now)
		if err != nil {
			log.Printf("[sched] arm %d: %v", j.ID, err)
			return
		}
		s.advance(ctx, j.ID, nxt)
		return
	}

	var fires []time.Time
	var future time.Time
	for len(fires) < maxCatchUpFires {
		nxt, err := cron.NextAfter(j.Cron, j.Timezone, from)
		if err != nil {
			log.Printf("[sched] advance %d: %v", j.ID, err)
			return
		}
		if nxt.After(now) {
			future = nxt
			break
		}
		fires = append(fires, nxt)
		from = nxt
	}

	switch policy {
	case model.CatchUpSkip:
		if len(fires) > 0 {
			target := future
			if target.IsZero() {
				target = from
			}
			s.advance(ctx, j.ID, target)
		}
	case model.CatchUpRunAll:
		for _, f := range fires {
			if !s.leader() {
				return
			}
			s.enqueueFire(ctx, j, f, f)
		}
	default:
		if len(fires) == 0 {
			return
		}
		latest := fires[len(fires)-1]
		target := future
		if target.IsZero() {
			target = latest
		}
		s.enqueueFire(ctx, j, latest, target)
	}
}

func (s *Scheduler) enqueueFire(ctx context.Context, j *model.Job, fire, wm time.Time) {
	t := model.Ticket{
		JobID:      j.ID,
		Name:       j.Name,
		Command:    j.Command,
		TimeoutSec: j.TimeoutSec,
		MaxRetries: j.MaxRetries,
		NoOverlap:  j.NoOverlap,
		SchedAt:    fire.UTC().Format(time.RFC3339Nano),
	}
	created, err := s.q.EnqueueFire(ctx, t, wm.UTC().Format(time.RFC3339))
	if err != nil {
		log.Printf("[sched] enqueue %d@%s: %v", j.ID, t.SchedAt, err)
		return
	}
	if created {
		log.Printf("[sched] enqueued %q fire %s (attempt %d)", j.Name, t.SchedAt, t.Attempt)
	}
}

func (s *Scheduler) advance(ctx context.Context, id int64, t time.Time) {
	if err := s.q.AdvanceWatermark(ctx, id, t.UTC().Format(time.RFC3339)); err != nil {
		log.Printf("[sched] watermark %d: %v", id, err)
	}
}
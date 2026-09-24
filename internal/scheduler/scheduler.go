package scheduler

import (
	"context"
	"log"
	"time"

	"orbix-scheduler/internal/jobs"
	"orbix-scheduler/internal/model"
	"orbix-scheduler/internal/queue"
)

const (
	maxCatchUpFires  = 8         
	resultsRetention = time.Hour 
	gcEvery          = time.Minute
)

type Scheduler struct {
	repo   *jobs.Repo
	q      *queue.Queue
	tick   func() time.Duration
	leader func() bool

	lastGC time.Time
}

func New(repo *jobs.Repo, q *queue.Queue, tick func() time.Duration, leader func() bool) *Scheduler {
	return &Scheduler{repo: repo, q: q, tick: tick, leader: leader}
}

func (s *Scheduler) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if s.leader() {
			s.tickOnce(ctx)
			s.gcOnce(ctx)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.tick()):
		}
	}
}

func (s *Scheduler) tickOnce(ctx context.Context) {
	now := time.Now().UTC()
	all, err := s.repo.List(ctx)
	if err != nil {
		log.Printf("[sched] list jobs: %v", err)
		return
	}
	for i := range all {
		if !s.leader() {
			return
		}
		if !all[i].Enabled {
			continue
		}
		s.scheduleJob(ctx, &all[i], now)
	}
	if s.leader() {
		s.sweepOrphans(ctx, all)
	}
}

func (s *Scheduler) gcOnce(ctx context.Context) {
	if time.Since(s.lastGC) < gcEvery {
		return
	}
	s.lastGC = time.Now()
	n, err := s.q.GCResults(ctx, resultsRetention, time.Now().UTC())
	if err != nil {
		log.Printf("[sched] gc: %v", err)
	} else if n > 0 {
		log.Printf("[sched] gc: reaped %d old results", n)
	}
}

func (s *Scheduler) sweepOrphans(ctx context.Context, all []model.Job) {
	live := make(map[int64]bool, len(all))
	for _, j := range all {
		if j.Enabled {
			live[j.ID] = true
		}
	}
	pending, err := s.q.PendingTickets(ctx, 256)
	if err != nil {
		log.Printf("[sched] sweep list: %v", err)
		return
	}
	for _, t := range pending {
		if !s.leader() {
			return
		}
		if t.Manual || live[t.JobID] {
			continue
		}
		if err := s.q.Abandon(ctx, t); err != nil {
			log.Printf("[sched] abandon %d@%s: %v", t.JobID, t.SchedAt, err)
		} else {
			log.Printf("[sched] abandoned %d@%s (job gone or disabled)", t.JobID, t.SchedAt)
		}
	}
}
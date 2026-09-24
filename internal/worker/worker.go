package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"orbix-scheduler/internal/executor"
	"orbix-scheduler/internal/model"
	"orbix-scheduler/internal/queue"
)

type Worker struct {
	queue    *queue.Queue
	nodeID   string
	interval func() time.Duration
	maxConc  int

	mu    sync.Mutex
	slots int
	wg    sync.WaitGroup
}

func New(q *queue.Queue, nodeID string, interval func() time.Duration, maxConc int) *Worker {
	if maxConc < 1 {
		maxConc = 1
	}
	return &Worker{queue: q, nodeID: nodeID, interval: interval, maxConc: maxConc}
}

func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		w.tryClaim(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(w.interval()):
		}
	}
}

func (w *Worker) tryClaim(ctx context.Context) {
	w.mu.Lock()
	busy := w.slots
	free := w.maxConc - busy
	w.mu.Unlock()
	if free <= 0 {
		return
	}

	ticket, ok, err := w.queue.ClaimNext(ctx, time.Now().UTC())
	if err != nil {
		log.Printf("[worker] claim: %v", err)
		return
	}
	if !ok {
		return
	}
	w.mu.Lock()
	w.slots++
	w.mu.Unlock()

	w.wg.Add(1)
	go func(t model.Ticket) {
		defer w.wg.Done()
		defer func() {
			w.mu.Lock()
			w.slots--
			w.mu.Unlock()
		}()
		w.execute(t)
	}(*ticket)
}

func (w *Worker) Drain(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		log.Printf("[worker] drain timed out; stragglers fail over via lease expiry")
	}
}

func (w *Worker) execute(t model.Ticket) {
	ctx := context.Background()
	start := time.Now().UTC()
	log.Printf("[worker] %s run %q (attempt %d)", w.nodeID, t.Name, t.Attempt)

	timeout := time.Duration(t.TimeoutSec) * time.Second
	res := executor.Run(ctx, t.Command, timeout)

	finished := time.Now().UTC()
	outcome := model.Result{
		JobID:      t.JobID,
		SchedAt:    t.SchedAt,
		Attempt:    t.Attempt,
		NodeID:     w.nodeID,
		ExitCode:   res.ExitCode,
		StartedAt:  start.Format(time.RFC3339),
		FinishedAt: finished.Format(time.RFC3339),
		DurationMS: res.DurationMS,
		Stdout:     truncate(res.Stdout, 8192),
		Stderr:     truncate(res.Stderr, 8192),
	}

	if res.Outcome == executor.TimedOut {
		outcome.Status = model.StatusTimeout
	} else if res.Outcome == executor.Success {
		outcome.Status = model.StatusSuccess
	} else {
		outcome.Status = model.StatusFailed
	}

	if outcome.Status == model.StatusSuccess {
		if err := w.queue.FinishSuccess(ctx, &t, outcome); err != nil {
			log.Printf("[worker] finish %d: %v", t.JobID, err)
		} else {
			log.Printf("[worker] ok  %q fire=%s attempt=%d success (%dms)",
				t.Name, t.SchedAt, t.Attempt, outcome.DurationMS)
		}
		return
	}
	if err := w.queue.FinishFailure(ctx, &t, outcome, time.Now().UTC()); err != nil {
		log.Printf("[worker] finish-fail %d: %v", t.JobID, err)
	} else {
		log.Printf("[worker] !!  %q fire=%s attempt=%d %s",
			t.Name, t.SchedAt, t.Attempt, outcome.Status)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…[truncated]"
}

func (w *Worker) Busy() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.slots
}

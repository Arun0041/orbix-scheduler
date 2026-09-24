package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"orbix-scheduler/internal/cron"
)

type statusRec struct {
	http.ResponseWriter
	code int
}

func (r *statusRec) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.reqTotal.Add(1)
		rec := &statusRec{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		if rec.code >= 500 {
			s.reqFail.Add(1)
		}
	})
}

func (s *Server) Healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"node":   s.node.ID,
	})
}// Readyz reports whether this node can do useful work right now:
func (s *Server) Readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	code := http.StatusOK
	checks := map[string]string{}
	if _, err := s.node.Members(ctx); err != nil {
		code = http.StatusServiceUnavailable
		checks["etcd"] = "error: " + err.Error()
	} else {
		checks["etcd"] = "ok"
	}
	if _, err := s.store.Counts(); err != nil {
		code = http.StatusServiceUnavailable
		checks["sqlite"] = "error: " + err.Error()
	} else {
		checks["sqlite"] = "ok"
	}
	writeJSON(w, code, map[string]any{
		"ready":  code == http.StatusOK,
		"node":   s.node.ID,
		"checks": checks,
	})
}

func (s *Server) PromMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	members, _ := s.node.Members(ctx)
	pending, _ := s.queue.CountPending(ctx)
	claims, _ := s.queue.Claims(ctx)
	counts, _ := s.store.Counts()
	leader := 0
	if s.node.IsLeader() {
		leader = 1
	}
	var b strings.Builder
	gauge := func(name, help, labels string, v any) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s%s %v\n",
			name, help, name, name, labels, v)
	}
	counter := func(name, help, labels string, v any) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n%s%s %v\n",
			name, help, name, name, labels, v)
	}
	nodelbl := fmt.Sprintf("{node=%q}", s.node.ID)
	gauge("orbix_leader", "1 when this node holds the cluster leadership", nodelbl, leader)
	gauge("orbix_cluster_members", "live cluster members visible to this node", nodelbl, len(members))
	gauge("orbix_queue_pending", "tickets waiting to be claimed", nodelbl, pending)
	gauge("orbix_queue_in_flight", "tickets currently claimed by workers", nodelbl, len(claims))
	gauge("orbix_worker_busy_local", "executions running on this node", nodelbl, s.worker.Busy())
	for st, n := range counts {
		gauge("orbix_runs_total", "finished runs mirrored into local history", fmt.Sprintf("{node=%q,status=%q}", s.node.ID, st), n)
	}
	counter("orbix_api_requests_total", "API requests served by this node", nodelbl, s.reqTotal.Load())
	counter("orbix_api_errors_total", "API requests that returned 5xx", nodelbl, s.reqFail.Load())
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String()))
}// cronPreview answers GET /api/cron/preview with the next n fire times (400 + parse error on bad input).
func (s *Server) cronPreview(w http.ResponseWriter, r *http.Request) {
	expr := strings.TrimSpace(r.URL.Query().Get("expr"))
	if expr == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("query param expr is required"))
		return
	}
	tz := strings.TrimSpace(r.URL.Query().Get("tz"))
	n := 5
	if raw := strings.TrimSpace(r.URL.Query().Get("n")); raw != "" {
		var v int
		if _, err := fmt.Sscanf(raw, "%d", &v); err != nil || v < 1 || v > 20 {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("n must be 1..20"))
			return
		}
		n = v
	}
	from := time.Now().UTC()
	if raw := strings.TrimSpace(r.URL.Query().Get("from")); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("from must be RFC3339: %w", err))
			return
		}
		from = t
	}
	fires := make([]string, 0, n)
	for i := 0; i < n; i++ {
		nxt, err := cron.NextAfter(expr, tz, from)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		fires = append(fires, nxt.Format(time.RFC3339))
		from = nxt
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"expr":  expr,
		"tz":    tz,
		"fires": fires,
	})
}
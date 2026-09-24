package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"orbix-scheduler/internal/cluster"
	"orbix-scheduler/internal/cron"
	"orbix-scheduler/internal/docker"
	"orbix-scheduler/internal/jobs"
	"orbix-scheduler/internal/model"
	"orbix-scheduler/internal/queue"
	"orbix-scheduler/internal/store"
	"orbix-scheduler/internal/worker"
)

type Server struct {
	repo    *jobs.Repo
	queue   *queue.Queue
	node    *cluster.Node
	store   *store.Store
	worker  *worker.Worker
	version string
	reqTotal atomic.Int64
	reqFail  atomic.Int64
	dock     *docker.Client
	onLeave  func()
}

func New(repo *jobs.Repo, q *queue.Queue, n *cluster.Node, st *store.Store,
	w *worker.Worker, version string, dock *docker.Client, onLeave func()) *Server {
	return &Server{repo: repo, queue: q, node: n, store: st, worker: w, version: version, dock: dock, onLeave: onLeave}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/status", s.status)
	mux.HandleFunc("GET /api/jobs", s.listJobs)
	mux.HandleFunc("POST /api/jobs", s.createJob)
	mux.HandleFunc("GET /api/jobs/{id}", s.getJob)
	mux.HandleFunc("PUT /api/jobs/{id}", s.updateJob)
	mux.HandleFunc("DELETE /api/jobs/{id}", s.deleteJob)
	mux.HandleFunc("POST /api/jobs/{id}/enable", s.enable(true))
	mux.HandleFunc("POST /api/jobs/{id}/disable", s.enable(false))
	mux.HandleFunc("POST /api/jobs/{id}/trigger", s.trigger)
	mux.HandleFunc("GET /api/jobs/{id}/runs", s.jobRuns)
	mux.HandleFunc("GET /api/runs", s.allRuns)
	mux.HandleFunc("GET /api/queue", s.queueSnapshot)
	mux.HandleFunc("GET /api/cron/preview", s.cronPreview)
	mux.HandleFunc("GET /api/nodes", s.listNodes)
	mux.HandleFunc("POST /api/nodes", s.addNode)
	mux.HandleFunc("POST /api/nodes/{id}/kill", s.killNode)
	mux.HandleFunc("POST /api/nodes/{id}/start", s.startNode)
	mux.HandleFunc("POST /api/nodes/self/leave", s.leaveNode)

	return s.instrument(logRequests(mux))
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("[api] %s %s (%s)", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func pathID(r *http.Request) (int64, error) {
	return strconv.ParseInt(r.PathValue("id"), 10, 64)
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	members, _ := s.node.Members(ctx)
	pending, _ := s.queue.CountPending(ctx)
	claims, _ := s.queue.Claims(ctx)
	counts, _ := s.store.Counts()
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":      s.node.ID,
		"is_leader":    s.node.IsLeader(),
		"leader":       s.node.Leader(),
		"members":      members,
		"pending":      pending,
		"in_flight":    claims,
		"busy_local":   s.worker.Busy(),
		"run_counts":   counts,
		"version":      s.version,
		"server_time":  time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	list, err := s.repo.List(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if list == nil {
		list = []model.Job{}
	}
	enrichNextFire(list)
	writeJSON(w, http.StatusOK, list)
}

func enrichNextFire(list []model.Job) {
	now := time.Now().UTC()
	for i := range list {
		if !list[i].Enabled {
			continue
		}
		if t, err := cron.NextAfter(list[i].Cron, list[i].Timezone, now); err == nil {
			list[i].NextFire = t.Format(time.RFC3339)
		}
	}
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var j model.Job
	if err := json.NewDecoder(r.Body).Decode(&j); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.repo.Create(r.Context(), &j); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, j)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	j, err := s.repo.Get(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) updateJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	var j model.Job
	if err := json.NewDecoder(r.Body).Decode(&j); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	updated, err := s.repo.Update(r.Context(), id, &j)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.repo.Delete(r.Context(), id); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func limitOf(r *http.Request) int {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return 50
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 50
	}
	if n > 500 {
		return 500
	}
	return n
}


func (s *Server) enable(v bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := pathID(r)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		j, err := s.repo.SetEnabled(r.Context(), id, v)
		if err != nil {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusOK, j)
	}
}

func (s *Server) trigger(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	j, err := s.repo.Get(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err := s.queue.Trigger(r.Context(), j); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{
		"status": "queued",
		"job":    j.Name,
	})
}

func (s *Server) jobRuns(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	runs, err := s.store.ListRuns(id, limitOf(r))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (s *Server) allRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.store.ListRuns(0, limitOf(r))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (s *Server) queueSnapshot(w http.ResponseWriter, r *http.Request) {
	pending, err := s.queue.PendingTickets(r.Context(), 50)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	claims, err := s.queue.Claims(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if pending == nil {
		pending = []model.Ticket{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pending":   pending,
		"in_flight": claims,
	})
}

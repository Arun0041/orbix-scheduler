package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata"

	"orbix-scheduler/internal/api"
	"orbix-scheduler/internal/cluster"
	"orbix-scheduler/internal/docker"
	"orbix-scheduler/internal/config"
	"orbix-scheduler/internal/history"
	"orbix-scheduler/internal/jobs"
	"orbix-scheduler/internal/queue"
	"orbix-scheduler/internal/scheduler"
	"orbix-scheduler/internal/store"
	"orbix-scheduler/internal/worker"
)

var webFS embed.FS

const version = "0.1.0"

func main() {
	cfgPath := flag.String("config", "orbix.yaml", "path to config file (empty = env only)")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	log.Printf("orbixd %s starting — node %s, etcd %v, api %s",
		version, cfg.NodeID, cfg.EtcdEndpoints, cfg.HTTPListen)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer stop()

	node, err := cluster.Connect(runCtx, cfg)

	if err != nil {
		log.Fatalf("etcd: %v", err)
	}
	defer node.Close()

	st, err := store.Open(cfg.DBPath())
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()

	repo := jobs.New(node.Client())
	q := queue.New(node.Client(), node.ID, node.ClaimLeaseID)
	mirror := history.New(node.Client(), st)
	sched := scheduler.New(repo, q, cfg.TickInterval, node.IsLeader)
	wrk := worker.New(q, node.ID, cfg.ClaimInterval, cfg.MaxConcurrent)

	dock := docker.New()
	if dock.Available() {
		log.Printf("docker control available: dashboard can add/kill nodes")
	} else {
		log.Printf("docker control unavailable (no socket): node add/kill disabled")
	}

	go node.Run(runCtx)
	go node.MaintainClaims(runCtx)
	go mirror.Run(runCtx)
	go sched.Run(runCtx)
	go wrk.Run(runCtx)

	srv := &http.Server{
		Addr:              cfg.HTTPListen,
		Handler:           routes(api.New(repo, q, node, st, wrk, version, dock, cancel)),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("api listening on %s", cfg.HTTPListen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()

	<-runCtx.Done()
	log.Printf("shutting down…")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Printf("draining workers...")
	wrk.Drain(10 * time.Second)
	node.Close()
	log.Printf("bye")
}

func routes(s *api.Server) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/api/", s.Handler())
	mux.HandleFunc("/healthz", s.Healthz)
	mux.HandleFunc("/readyz", s.Readyz)
	mux.HandleFunc("/metrics", s.PromMetrics)
	if sub, err := fs.Sub(webFS, "web"); err == nil {
		mux.Handle("/", http.FileServer(http.FS(sub)))
	}
	return mux
}

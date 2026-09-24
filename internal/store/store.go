package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"

	"orbix-scheduler/internal/model"
)

type Run struct {
	model.Result
	Key string `json:"-"`
}

const schema = `
CREATE TABLE IF NOT EXISTS runs (
  key         TEXT PRIMARY KEY,
  job_id      INTEGER NOT NULL,
  sched_at    TEXT    NOT NULL,
  attempt     INTEGER NOT NULL DEFAULT 1,
  node_id     TEXT    NOT NULL,
  status      TEXT    NOT NULL,
  exit_code   INTEGER NOT NULL DEFAULT 0,
  started_at  TEXT,
  finished_at TEXT,
  duration_ms INTEGER NOT NULL DEFAULT 0,
  stdout      TEXT,
  stderr      TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_job ON runs(job_id, key DESC);
`

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA busy_timeout=5000;",
		"PRAGMA synchronous=NORMAL;",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("store: %s: %w", pragma, err)
		}
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: migrate: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) InsertRun(key string, r model.Result) error {
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO runs
		 (key, job_id, sched_at, attempt, node_id, status, exit_code,
		  started_at, finished_at, duration_ms, stdout, stderr)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		key, r.JobID, r.SchedAt, r.Attempt, r.NodeID, r.Status, r.ExitCode,
		r.StartedAt, r.FinishedAt, r.DurationMS, r.Stdout, r.Stderr)
	if err != nil {
		return fmt.Errorf("store: insert run: %w", err)
	}
	return nil
}

func (s *Store) ListRuns(jobID int64, limit int) ([]Run, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	q := `SELECT key, job_id, sched_at, attempt, node_id, status, exit_code,
	             started_at, finished_at, duration_ms, stdout, stderr
	      FROM runs`
	args := []any{}
	if jobID > 0 {
		q += ` WHERE job_id = ?`
		args = append(args, jobID)
	}
	q += ` ORDER BY rowid DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: list runs: %w", err)
	}
	defer rows.Close()

	var out []Run
	for rows.Next() {
		var r Run
		var stdout, stderr sql.NullString
		if err := rows.Scan(&r.Key, &r.JobID, &r.SchedAt, &r.Attempt, &r.NodeID,
			&r.Status, &r.ExitCode, &r.StartedAt, &r.FinishedAt,
			&r.DurationMS, &stdout, &stderr); err != nil {
			return nil, fmt.Errorf("store: scan run: %w", err)
		}
		r.Stdout, r.Stderr = stdout.String, stderr.String
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Counts() (map[string]int, error) {
	rows, err := s.db.Query(`SELECT status, COUNT(*) FROM runs GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("store: counts: %w", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}

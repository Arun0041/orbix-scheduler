package model

const (
	CatchUpRunLatest = "run_latest"
	CatchUpRunAll    = "run_all"   
	CatchUpSkip      = "skip"      
)

const (
	StatusSuccess        = "success"
	StatusFailed         = "failed"
	StatusTimeout        = "timeout"
	StatusFailedPermanent = "failed_permanent"
	StatusSkipped        = "skipped"         
	StatusAbandoned      = "abandoned"       
)

type Job struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Cron       string `json:"cron"`
	Timezone   string `json:"tz,omitempty"`
	Command    string `json:"command"`     
	TimeoutSec int    `json:"timeout_s"`   
	MaxRetries int    `json:"max_retries"`  
	CatchUp    string `json:"catch_up"`     
	NoOverlap  bool   `json:"no_overlap"`   
	Enabled    bool   `json:"enabled"`
	NextFire   string `json:"next_fire,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

type Ticket struct {
	JobID      int64  `json:"job_id"`
	Name       string `json:"name"`
	Command    string `json:"command"`
	TimeoutSec int    `json:"timeout_s"`
	MaxRetries int    `json:"max_retries"`
	NoOverlap  bool   `json:"no_overlap"`
	SchedAt    string `json:"sched_at"`
	Attempt    int    `json:"attempt"` 
	NotBefore  string `json:"not_before,omitempty"`
	Manual     bool   `json:"manual,omitempty"`
}

type Result struct {
	JobID      int64  `json:"job_id"`
	SchedAt    string `json:"sched_at"`
	Attempt    int    `json:"attempt"`
	NodeID     string `json:"node_id"`
	Status     string `json:"status"`
	ExitCode   int    `json:"exit_code,omitempty"`
	StartedAt  string `json:"started_at,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
	Stdout     string `json:"stdout,omitempty"`
	Stderr     string `json:"stderr,omitempty"`
}

type Member struct {
	NodeID   string `json:"node_id"`
	Addr     string `json:"addr"`
	JoinedAt string `json:"joined_at"`
}

type LeaderInfo struct {
	NodeID string `json:"node_id"`
	Since  string `json:"since"`
}

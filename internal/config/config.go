package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	NodeID          string   `yaml:"node_id"`
	HTTPListen      string   `yaml:"http_listen"`
	EtcdEndpoints   []string `yaml:"etcd_endpoints"`
	ElectionTTLSec  int      `yaml:"election_ttl_s"` 
	WorkerLeaseSec  int      `yaml:"worker_lease_s"`
	TickIntervalMS  int      `yaml:"tick_interval_ms"`
	ClaimIntervalMS int      `yaml:"claim_interval_ms"`
	DataDir         string   `yaml:"data_dir"`
	CatchUp         string   `yaml:"catch_up"`    
	MaxConcurrent   int      `yaml:"max_concurrent"`
}

func Default() *Config {
	host, _ := os.Hostname()
	return &Config{
		NodeID:          host,
		HTTPListen:      ":8080",
		EtcdEndpoints:   []string{"http://localhost:2379"},
		ElectionTTLSec:  10,
		WorkerLeaseSec:  5,
		TickIntervalMS:  1000,
		ClaimIntervalMS: 500,
		DataDir:         "./data",
		CatchUp:         "run_latest",
		MaxConcurrent:   4,
	}
}

func Load(path string) (*Config, error) {
	c := Default()
	if path != "" {
		b, err := os.ReadFile(path)
		if err == nil {
			if err := yaml.Unmarshal(b, c); err != nil {
				return nil, fmt.Errorf("config: %w", err)
			}
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("config: %w", err)
		}
	}
	envStr("ORBIX_NODE_ID", &c.NodeID)
	envStr("ORBIX_LISTEN", &c.HTTPListen)
	envStr("ORBIX_DATA_DIR", &c.DataDir)
	envStr("ORBIX_CATCHUP", &c.CatchUp)
	if v := os.Getenv("ORBIX_ETCD"); v != "" {
		c.EtcdEndpoints = strings.Split(v, ",")
	}

	if strings.TrimSpace(c.NodeID) == "" {
		return nil, fmt.Errorf("config: node_id must not be empty")
	}
	if len(c.EtcdEndpoints) == 0 {
		return nil, fmt.Errorf("config: etcd_endpoints must not be empty")
	}
	switch c.CatchUp {
	case "run_latest", "run_all", "skip":
	default:
		return nil, fmt.Errorf("config: catch_up must be run_latest|run_all|skip, got %q", c.CatchUp)
	}
	if c.ElectionTTLSec < 2 {
		c.ElectionTTLSec = 2
	}
	if c.WorkerLeaseSec < 2 {
		c.WorkerLeaseSec = 2
	}
	if c.TickIntervalMS < 100 {
		c.TickIntervalMS = 100
	}
	if c.ClaimIntervalMS < 100 {
		c.ClaimIntervalMS = 100
	}
	if c.MaxConcurrent < 1 {
		c.MaxConcurrent = 1
	}
	return c, nil
}

func envStr(key string, dst *string) {
	if v := os.Getenv(key); v != "" {
		*dst = v
	}
}

func (c *Config) DBPath() string {
	return filepath.Join(c.DataDir, "orbix.db")
}

func (c *Config) TickInterval() time.Duration {
	return time.Duration(c.TickIntervalMS) * time.Millisecond
}

func (c *Config) ClaimInterval() time.Duration {
	return time.Duration(c.ClaimIntervalMS) * time.Millisecond
}

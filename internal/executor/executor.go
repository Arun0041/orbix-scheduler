package executor

import (
	"bytes"
	"context"
	"os/exec"
	"runtime"
	"time"
)

type Outcome string

const (
	Success  Outcome = "success"
	Failed   Outcome = "failed"
	TimedOut Outcome = "timeout"
)

type Result struct {
	Outcome    Outcome
	ExitCode   int
	Stdout     string
	Stderr     string
	DurationMS int64
}

func Run(ctx context.Context, command string, timeout time.Duration) Result {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	sh := shell()
	cmd := exec.CommandContext(ctx, sh[0], append(sh[1:], command)...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	start := time.Now()
	err := cmd.Run()
	dur := time.Since(start)

	res := Result{
		Stdout:     stdout.String(),
		Stderr:     stderr.String(),
		DurationMS: dur.Milliseconds(),
	}
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		res.Outcome = TimedOut
		res.ExitCode = -1
		res.Stderr += "\n[orbix] command exceeded timeout of " + timeout.String()
	case err != nil:
		res.Outcome = Failed
		if code := exitCode(err); code >= 0 {
			res.ExitCode = code
		} else {
			res.ExitCode = -1
		}
	default:
		res.Outcome = Success
	}
	return res
}

func shell() []string {
	if runtime.GOOS == "windows" {
		return []string{"cmd", "/C"}
	}
	return []string{"sh", "-c"}
}

func exitCode(err error) int {
	type exitError interface{ ExitCode() int }
	if ee, ok := err.(exitError); ok {
		return ee.ExitCode()
	}
	return -1
}

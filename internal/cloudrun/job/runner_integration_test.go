//go:build integration

package job

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/slokam-ai/localgcp/internal/orchestrator"
)

// TestDockerRunnerRealExecution runs real containers and asserts exit codes.
// Run with: go test -tags integration ./internal/cloudrun/job/
func TestDockerRunnerRealExecution(t *testing.T) {
	rt := orchestrator.NewDockerRuntime(log.New(os.Stderr, "[it] ", 0))
	if !rt.Available() {
		t.Skip("Docker not available")
	}
	runner := NewDockerRunner(rt)
	ctx := context.Background()

	code, err := runner.RunTask(ctx, TaskSpec{
		Name:    "localgcp-job-it-ok",
		Image:   "alpine:3.21",
		Command: []string{"/bin/sh", "-c", "exit 0"},
	})
	if err != nil || code != 0 {
		t.Fatalf("expected exit 0, got code=%d err=%v", code, err)
	}

	code, err = runner.RunTask(ctx, TaskSpec{
		Name:    "localgcp-job-it-fail",
		Image:   "alpine:3.21",
		Command: []string{"/bin/sh", "-c", "exit 3"},
	})
	if err != nil || code != 3 {
		t.Fatalf("expected exit 3, got code=%d err=%v", code, err)
	}
}

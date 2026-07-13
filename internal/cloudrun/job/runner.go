package job

import (
	"context"

	"github.com/slokam-ai/localgcp/internal/orchestrator"
)

// TaskSpec describes a single container run for one task of an execution.
type TaskSpec struct {
	Name    string   // unique container name (also used for orphan cleanup)
	Image   string   // container image to run
	Command []string // entrypoint + args (empty = image default)
	Env     []string // KEY=VALUE pairs, including CLOUD_RUN_TASK_* vars
}

// Runner runs a single task's container to completion and returns its exit code.
// A non-nil error means the task could not be run at all (as opposed to running
// and exiting non-zero, which is reported via exitCode).
type Runner interface {
	RunTask(ctx context.Context, spec TaskSpec) (exitCode int, err error)
}

// DockerRunner runs tasks as real containers via the orchestrator runtime.
type DockerRunner struct {
	rt orchestrator.ContainerRuntime
}

func NewDockerRunner(rt orchestrator.ContainerRuntime) *DockerRunner {
	return &DockerRunner{rt: rt}
}

func (r *DockerRunner) RunTask(ctx context.Context, spec TaskSpec) (int, error) {
	return r.rt.RunToCompletion(ctx, orchestrator.ContainerConfig{
		Name:  spec.Name,
		Image: spec.Image,
		Cmd:   spec.Command,
		Env:   spec.Env,
	})
}

// StubRunner reports every task as an immediate success. It is the fallback when
// Docker is unavailable, so the control-plane API still works without Docker.
type StubRunner struct{}

func (StubRunner) RunTask(context.Context, TaskSpec) (int, error) { return 0, nil }

// Package job implements the Cloud Run v2 Jobs, Executions and Tasks APIs.
package job

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/slokam-ai/localgcp/internal/cloudrun/lro"
)

// Server serves the Cloud Run v2 Jobs, Executions and Tasks APIs. Only the Jobs
// service defines IAM methods, so a single struct can embed all three
// Unimplemented servers without method-name conflicts.
type Server struct {
	runpb.UnimplementedJobsServer
	runpb.UnimplementedExecutionsServer
	runpb.UnimplementedTasksServer
	store *Store
}

func New(runner Runner, logger *log.Logger) *Server {
	return &Server{store: NewStore(runner, logger)}
}

// Register registers the Jobs, Executions and Tasks APIs on the gRPC server.
func (s *Server) Register(srv *grpc.Server) {
	runpb.RegisterJobsServer(srv, s)
	runpb.RegisterExecutionsServer(srv, s)
	runpb.RegisterTasksServer(srv, s)
}

// --- Jobs ---
func (s *Server) CreateJob(_ context.Context, req *runpb.CreateJobRequest) (*longrunning.Operation, error) {
	name := req.GetParent() + "/jobs/" + req.GetJobId()
	job, err := s.store.CreateJob(name, req.GetJob())
	if err != nil {
		var ia *invalidArgError
		if errors.As(err, &ia) {
			return nil, status.Error(codes.InvalidArgument, ia.Error())
		}
		return nil, status.Errorf(codes.AlreadyExists, "Job %s already exists", name)
	}
	return lro.Completed(name+"/operations/create", job)
}

func (s *Server) GetJob(_ context.Context, req *runpb.GetJobRequest) (*runpb.Job, error) {
	job, ok := s.store.GetJob(req.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Job %s not found", req.GetName())
	}
	return job, nil
}

func (s *Server) ListJobs(_ context.Context, req *runpb.ListJobsRequest) (*runpb.ListJobsResponse, error) {
	return &runpb.ListJobsResponse{Jobs: s.store.ListJobs(req.GetParent())}, nil
}

func (s *Server) UpdateJob(_ context.Context, req *runpb.UpdateJobRequest) (*longrunning.Operation, error) {
	job := req.GetJob()
	name := job.GetName()
	updated, err := s.store.UpdateJob(name, job)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Job %s not found", name)
	}
	return lro.Completed(name+"/operations/update", updated)
}

func (s *Server) DeleteJob(_ context.Context, req *runpb.DeleteJobRequest) (*longrunning.Operation, error) {
	name := req.GetName()
	job, ok := s.store.DeleteJob(name)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Job %s not found", name)
	}
	return lro.Completed(name+"/operations/delete", job)
}

// RunJob triggers a new Execution. Its long-running operation carries the
// freshly created (still RUNNING) Execution as the response; clients poll
// GetExecution to observe completion.
func (s *Server) RunJob(_ context.Context, req *runpb.RunJobRequest) (*longrunning.Operation, error) {
	name := req.GetName()
	exec, err := s.store.RunJob(name, req.GetOverrides())
	if err != nil {
		var ia *invalidArgError
		if errors.As(err, &ia) {
			return nil, status.Error(codes.InvalidArgument, ia.Error())
		}
		return nil, status.Errorf(codes.NotFound, "Job %s not found", name)
	}
	return lro.Completed(name+"/operations/run", exec)
}

// --- Executions ---
func (s *Server) GetExecution(_ context.Context, req *runpb.GetExecutionRequest) (*runpb.Execution, error) {
	exec, ok := s.store.GetExecution(req.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Execution %s not found", req.GetName())
	}
	return exec, nil
}

func (s *Server) ListExecutions(_ context.Context, req *runpb.ListExecutionsRequest) (*runpb.ListExecutionsResponse, error) {
	return &runpb.ListExecutionsResponse{Executions: s.store.ListExecutions(req.GetParent())}, nil
}

func (s *Server) DeleteExecution(_ context.Context, req *runpb.DeleteExecutionRequest) (*longrunning.Operation, error) {
	name := req.GetName()
	exec, ok := s.store.DeleteExecution(name)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Execution %s not found", name)
	}
	return lro.Completed(name+"/operations/delete", exec)
}

func (s *Server) CancelExecution(_ context.Context, req *runpb.CancelExecutionRequest) (*longrunning.Operation, error) {
	name := req.GetName()
	exec, ok := s.store.CancelExecution(name)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Execution %s not found", name)
	}
	return lro.Completed(name+"/operations/cancel", exec)
}

// --- Tasks ---
func (s *Server) GetTask(_ context.Context, req *runpb.GetTaskRequest) (*runpb.Task, error) {
	task, ok := s.store.GetTask(req.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Task %s not found", req.GetName())
	}
	return task, nil
}

func (s *Server) ListTasks(_ context.Context, req *runpb.ListTasksRequest) (*runpb.ListTasksResponse, error) {
	return &runpb.ListTasksResponse{Tasks: s.store.ListTasks(req.GetParent())}, nil
}

// --- Environment-size validation ---
// Passing large data (e.g. a whole payload) through a container's environment
// fails at run time — a single env string over MAX_ARG_STRLEN makes the
// container fail to exec, and real Cloud Run rejects oversized env at the API.
// localgcp enforces the same so the problem surfaces locally instead of
// silently working and then breaking in production.
const (
	// maxEnvVarBytes bounds a single "KEY=VALUE" entry (Linux MAX_ARG_STRLEN).
	maxEnvVarBytes = 128 * 1024 // 128 KiB
	// maxTotalEnvBytes bounds a task's combined environment.
	maxTotalEnvBytes = 1 << 20 // 1 MiB
)

// invalidArgError marks a request that should map to codes.InvalidArgument.
type invalidArgError struct{ msg string }

func (e *invalidArgError) Error() string { return e.msg }

// validateEnv rejects environments that are too large to pass to a container.
// It points at the likely fix: pass a reference (e.g. a message id) and have
// the worker fetch the payload instead of inlining it in env.
func validateEnv(env []string) error {
	total := 0
	for _, kv := range env {
		if len(kv) > maxEnvVarBytes {
			name := kv
			if i := strings.IndexByte(kv, '='); i >= 0 {
				name = kv[:i]
			}
			return &invalidArgError{fmt.Sprintf(
				"environment variable %q is %d bytes, over the %d-byte per-variable limit; "+
					"pass a reference (e.g. a message id) and fetch the payload instead of inlining it in env",
				name, len(kv), maxEnvVarBytes)}
		}
		total += len(kv) + 1
	}
	if total > maxTotalEnvBytes {
		return &invalidArgError{fmt.Sprintf(
			"task environment is %d bytes, over the %d-byte limit; "+
				"reduce env size (pass a reference and fetch the payload instead of inlining it)",
			total, maxTotalEnvBytes)}
	}
	return nil
}

// containerEnv flattens a container's env vars plus any override env into
// "KEY=VALUE" strings (override entries last, so they win under Docker's
// last-wins semantics). This is the environment a task container receives,
// minus the small CLOUD_RUN_TASK_* vars added per task.
func containerEnv(containers []*runpb.Container, ovr taskOverride) []string {
	var env []string
	if len(containers) > 0 {
		for _, e := range containers[0].GetEnv() {
			env = append(env, e.GetName()+"="+e.GetValue())
		}
	}
	return append(env, ovr.env...)
}

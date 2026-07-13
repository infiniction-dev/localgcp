// Package job implements the Cloud Run v2 Jobs, Executions and Tasks APIs.
package job

import (
	"context"
	"log"

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

func New(runner Runner, logger *log.Logger, seeds []SeedJob) *Server {
	s := &Server{store: NewStore(runner, logger)}
	for _, sj := range seeds {
		if _, err := s.store.CreateJob(sj.fullName(), sj.toJob()); err != nil {
			if logger != nil {
				logger.Printf("seed job %s: %v", sj.Name, err)
			}
		} else if logger != nil {
			logger.Printf("seeded job %s", sj.fullName())
		}
	}
	return s
}

// Register registers the Jobs, Executions and Tasks APIs on the gRPC server.
func (s *Server) Register(srv *grpc.Server) {
	runpb.RegisterJobsServer(srv, s)
	runpb.RegisterExecutionsServer(srv, s)
	runpb.RegisterTasksServer(srv, s)
}

// ── Jobs ──────────────────────────────────────────────────────────────────────

func (s *Server) CreateJob(_ context.Context, req *runpb.CreateJobRequest) (*longrunning.Operation, error) {
	name := req.GetParent() + "/jobs/" + req.GetJobId()
	job, err := s.store.CreateJob(name, req.GetJob())
	if err != nil {
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
		return nil, status.Errorf(codes.NotFound, "Job %s not found", name)
	}
	return lro.Completed(name+"/operations/run", exec)
}

// ── Executions ────────────────────────────────────────────────────────────────

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

// ── Tasks ─────────────────────────────────────────────────────────────────────

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

// Package cloudrun wires the Cloud Run emulator. The Services API and the Jobs
// API (Jobs, Executions, Tasks) are distinct Cloud Run v2 API groups, each
// implemented in its own sub-package (service, job) and served together on one
// gRPC port.
package cloudrun

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/slokam-ai/localgcp/internal/cloudrun/job"
	"github.com/slokam-ai/localgcp/internal/cloudrun/service"
)

// Service is the Cloud Run emulator lifecycle: it owns the gRPC server and
// registers the Services and Jobs API handlers on a single port.
type Service struct {
	quiet  bool
	logger *log.Logger
	svc    *service.Server
	jobs   *job.Server
}

// New creates the Cloud Run emulator. runner executes Jobs tasks (a Docker-backed
// runner for real execution, or a stub when Docker is unavailable). seeds are
// jobs to auto-register at startup (from --jobs).
func New(dataDir string, quiet bool, runner job.Runner, seeds []job.SeedJob) *Service {
	logger := log.New(os.Stderr, "[cloudrun] ", log.LstdFlags)
	return &Service{
		quiet:  quiet,
		logger: logger,
		svc:    service.New(),
		jobs:   job.New(runner, logger, seeds),
	}
}

func (s *Service) Name() string { return "Cloud Run" }

func (s *Service) Start(ctx context.Context, addr string) error {
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(s.loggingInterceptor),
	)
	s.svc.Register(srv)
	s.jobs.Register(srv)
	reflection.Register(srv)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	if err := srv.Serve(ln); err != nil {
		return err
	}
	return nil
}

func (s *Service) loggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	if !s.quiet {
		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}
		s.logger.Printf("%s %s", info.FullMethod, code)
	}
	return resp, err
}

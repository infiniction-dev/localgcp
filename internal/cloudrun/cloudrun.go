// Package cloudrun wires the Cloud Run emulator. The Cloud Run v2 API is split
// into distinct groups; each is implemented in its own sub-package (service) and
// served together on one gRPC port.
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

	"github.com/slokam-ai/localgcp/internal/cloudrun/service"
)

// Service is the Cloud Run emulator lifecycle: it owns the gRPC server and
// registers the Cloud Run API handlers on a single port.
type Service struct {
	quiet  bool
	logger *log.Logger
	svc    *service.Server
}

func New(dataDir string, quiet bool) *Service {
	logger := log.New(os.Stderr, "[cloudrun] ", log.LstdFlags)
	return &Service{
		quiet:  quiet,
		logger: logger,
		svc:    service.New(),
	}
}

func (s *Service) Name() string { return "Cloud Run" }

func (s *Service) Start(ctx context.Context, addr string) error {
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(s.loggingInterceptor),
	)
	s.svc.Register(srv)
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

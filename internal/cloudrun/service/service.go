// Package service implements the Cloud Run v2 Services API.
package service

import (
	"context"

	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/slokam-ai/localgcp/internal/cloudrun/lro"
)

// Server serves the Cloud Run v2 Services API.
type Server struct {
	runpb.UnimplementedServicesServer
	store *Store
}

func New() *Server {
	return &Server{store: NewStore()}
}

// Register registers the Services API on the given gRPC server.
func (s *Server) Register(srv *grpc.Server) {
	runpb.RegisterServicesServer(srv, s)
}

func (s *Server) CreateService(_ context.Context, req *runpb.CreateServiceRequest) (*longrunning.Operation, error) {
	name := req.GetParent() + "/services/" + req.GetServiceId()
	svc, err := s.store.Create(name, req.GetService())
	if err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "Service %s already exists", name)
	}
	return lro.Completed(name+"/operations/create", svc)
}

func (s *Server) GetService(_ context.Context, req *runpb.GetServiceRequest) (*runpb.Service, error) {
	svc, ok := s.store.Get(req.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Service %s not found", req.GetName())
	}
	return svc, nil
}

func (s *Server) ListServices(_ context.Context, req *runpb.ListServicesRequest) (*runpb.ListServicesResponse, error) {
	return &runpb.ListServicesResponse{Services: s.store.List(req.GetParent())}, nil
}

func (s *Server) UpdateService(_ context.Context, req *runpb.UpdateServiceRequest) (*longrunning.Operation, error) {
	svc := req.GetService()
	name := svc.GetName()
	updated, err := s.store.Update(name, svc)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Service %s not found", name)
	}
	return lro.Completed(name+"/operations/update", updated)
}

func (s *Server) DeleteService(_ context.Context, req *runpb.DeleteServiceRequest) (*longrunning.Operation, error) {
	name := req.GetName()
	if !s.store.Delete(name) {
		return nil, status.Errorf(codes.NotFound, "Service %s not found", name)
	}
	return lro.Completed(name+"/operations/delete", &runpb.Service{Name: name})
}

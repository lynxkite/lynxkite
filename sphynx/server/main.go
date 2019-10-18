package main

import (
	"context"
	"log"
	"net"
	"os"

	pb "github.com/biggraph/biggraph/sphynx/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type server struct{}

func (s *server) CanCompute(ctx context.Context, in *pb.CanComputeRequest) (*pb.CanComputeReply, error) {
	log.Printf("Received: %v", in.Operation)
	return &pb.CanComputeReply{CanCompute: false}, nil
}

func main() {
	port := os.Getenv("SPHYNX_PORT")
	creds, err := credentials.NewServerTLSFromFile("sphynx/server/cert.pem", "sphynx/server/private-key.pem")
	if err != nil {
		log.Fatalf("failed to read credentials: %v", err)
	}
	s := grpc.NewServer(grpc.Creds(creds))
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	pb.RegisterSphynxServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

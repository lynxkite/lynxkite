package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"

	"encoding/json"
	pb "github.com/biggraph/biggraph/sphynx/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func shortenClassName(className string) string {
	return className[len("com.lynxanalytics.biggraph.graph_operations."):]
}

func OperationInstanceFromJSON(op_json string) operationInstance {
	var opInst operationInstance
	b := []byte(op_json)
	json.Unmarshal(b, &opInst)
	return opInst
}

func (s *server) CanCompute(ctx context.Context, in *pb.CanComputeRequest) (*pb.CanComputeReply, error) {
	log.Printf("Received: %v", in.Operation)
	opInst := OperationInstanceFromJSON(in.Operation)
	shortenedClass := shortenClassName(opInst.Operation.Class)
	switch shortenedClass {
	case "GetBetter":
		return &pb.CanComputeReply{CanCompute: true}, nil
	default:
		return &pb.CanComputeReply{CanCompute: false}, nil
	}
}

func (s *server) Compute(ctx context.Context, in *pb.ComputeRequest) (*pb.ComputeReply, error) {
	opInst := OperationInstanceFromJSON(in.Operation)
	shortenedClass := shortenClassName(opInst.Operation.Class)
	switch shortenedClass {
	case "GetBetter":
		s.getBetter(opInst)
	default:
		log.Fatalf("Can't compute %v", opInst)
	}
	return &pb.ComputeReply{}, nil
}

func (s *server) GetScalar(ctx context.Context, in *pb.GetScalarRequest) (*pb.GetScalarReply, error) {
	scalar := s.scalars[guid(in.Guid)]
	scalarJSON, err := json.Marshal(scalar)
	if err != nil {
		log.Fatalf("Converting scalar to json failed: %v", err)
	}
	return &pb.GetScalarReply{Scalar: string(scalarJSON)}, nil
}

func main() {
	port := os.Getenv("SPHYNX_PORT")
	keydir := flag.String(
		"keydir", "", "directory of cert.pem and private-key.pem files (for encryption)")
	flag.Parse()
	var s *grpc.Server
	if *keydir != "" {
		creds, err := credentials.NewServerTLSFromFile(*keydir+"/cert.pem", *keydir+"/private-key.pem")
		if err != nil {
			log.Fatalf("failed to read credentials: %v", err)
		}
		s = grpc.NewServer(grpc.Creds(creds))
	} else {
		s = grpc.NewServer()
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	pb.RegisterSphynxServer(s, &server{scalars: make(map[guid]scalarValue)})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

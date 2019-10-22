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

type server struct{}
type guid string
type operationDescription struct {
	Class string
	Data  string
}
type operationInstance struct {
	GUID      guid
	Inputs    map[string]guid
	Outputs   map[string]guid
	Operation operationDescription
}

func canCompute(op operationDescription) bool {
	shortenedClass := op.Class[len("com.lynxanalytics.biggraph.graph_operations."):]
	switch shortenedClass {
	case "DoNothing":
		return true
	default:
		return false
	}
}

func (s *server) CanCompute(ctx context.Context, in *pb.CanComputeRequest) (*pb.CanComputeReply, error) {
	var op_inst operationInstance
	// log.Printf("Received: %v", in.Operation)
	b := []byte(in.Operation)
	json.Unmarshal(b, &op_inst)
	return &pb.CanComputeReply{CanCompute: canCompute(op_inst.Operation)}, nil
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
	pb.RegisterSphynxServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

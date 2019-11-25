// Sphynx is a gRPC server. LynxKite can connect to it and ask it to do some work.
// The idea is that Sphynx performs operations on graphs that fit into the memory,
// so there's no need to do slow distributed computations.

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"

	"encoding/json"
	pb "github.com/biggraph/biggraph/sphynx/proto"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func OperationInstanceFromJSON(opJSON string) OperationInstance {
	var opInst OperationInstance
	b := []byte(opJSON)
	json.Unmarshal(b, &opInst)
	return opInst
}

func getExecutableOperation(opInst OperationInstance) (Operation, bool) {
	className := opInst.Operation.Class
	shortenedClass := className[len("com.lynxanalytics.biggraph.graph_operations."):]
	op, exists := operations[shortenedClass]
	return op, exists
}

func NewServer() Server {
	dataDir := os.Getenv("SPHYNX_DATA_DIR")
	unorderedDataDir := os.Getenv("UNORDERED_SPHYNX_DATA_DIR")
	os.MkdirAll(unorderedDataDir, 0775)
	os.MkdirAll(dataDir, 0775)
	return Server{entities: make(map[GUID]interface{}),
		dataDir: dataDir, unorderedDataDir: unorderedDataDir}
}

func (s *Server) CanCompute(ctx context.Context, in *pb.CanComputeRequest) (*pb.CanComputeReply, error) {
	opInst := OperationInstanceFromJSON(in.Operation)
	_, exists := getExecutableOperation(opInst)
	return &pb.CanComputeReply{CanCompute: exists}, nil
}

func (s *Server) Compute(ctx context.Context, in *pb.ComputeRequest) (*pb.ComputeReply, error) {
	opInst := OperationInstanceFromJSON(in.Operation)
	op, exists := getExecutableOperation(opInst)
	if !exists {
		return nil, status.Errorf(codes.Unimplemented, "Can't compute %v", opInst)
	} else {
		outputs := op.execute(s, opInst)
		s.Lock()
		defer s.Unlock()
		for name, entity := range outputs {
			guid := opInst.Outputs[name]
			s.entities[guid] = entity
			switch e := entity.(type) {
			case EdgeBundle:
				idSetGUID := opInst.Outputs[name+"-idSet"]
				idSetEntity := VertexSet{e.edgeMapping}
				s.entities[idSetGUID] = idSetEntity
			}
		}
		return &pb.ComputeReply{}, nil
	}
}

func (s *Server) GetScalar(ctx context.Context, in *pb.GetScalarRequest) (*pb.GetScalarReply, error) {
	log.Printf("Received GetScalar request with GUID %v.", in.Guid)
	scalar := s.entities[GUID(in.Guid)]
	scalarJSON, err := json.Marshal(scalar)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "Converting scalar to json failed: %v", err)
	}
	return &pb.GetScalarReply{Scalar: string(scalarJSON)}, nil
}

func (s *Server) ToSparkIds(ctx context.Context, in *pb.ToSparkIdsRequest) (*pb.ToSparkIdsReply, error) {
	entity := s.entities[GUID(in.Guid)]
	log.Printf("Reindexing %v to use spark IDs.", entity)
	switch e := entity.(type) {
	case VertexSet:
		vertexSet := &pb.VertexSet{Ids: e.vertexMapping}
		out, err := proto.Marshal(vertexSet)
		if err != nil {
			return nil, status.Errorf(codes.Unknown,
				"Failed to encode vertex set: %v", err)
		}
		fname := fmt.Sprintf("%v/%v", s.unorderedDataDir, in.Guid)
		if err := ioutil.WriteFile(fname, out, 0755); err != nil {
			return nil, status.Errorf(codes.Unknown,
				"Failed to write encoded vertex set: %v", err)
		}
		return &pb.ToSparkIdsReply{}, nil
	case EdgeBundle:
		edgeBundle := &pb.EdgeBundle{}
		for sphynxId, sparkId := range e.edgeMapping {
			edgeBundle.Edges = append(edgeBundle.Edges,
				&pb.Edge{Id: sparkId,
					Src: e.src[sphynxId],
					Dst: e.dst[sphynxId],
				})
		}
		out, err := proto.Marshal(edgeBundle)
		if err != nil {
			return nil, status.Errorf(codes.Unknown,
				"Failed to encode edge bundle: %v", err)
		}
		fname := fmt.Sprintf("%v/%v", s.unorderedDataDir, in.Guid)
		if err := ioutil.WriteFile(fname, out, 0755); err != nil {
			return nil, status.Errorf(codes.Unknown,
				"Failed to write encoded edge bundle: %v", err)
		}
		return &pb.ToSparkIdsReply{}, nil
	default:
		return nil, status.Errorf(codes.Unimplemented, "Can't reindex %v to use Spark IDs.", entity)
	}
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

	sphynxServer := NewServer()
	pb.RegisterSphynxServer(s, &sphynxServer)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

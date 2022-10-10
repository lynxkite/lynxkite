// Sphynx is a gRPC server. LynxKite can connect to it and ask it to do some work.
// The idea is that Sphynx performs operations on graphs that fit into the memory,
// so there's no need to do slow distributed computations.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	pb "github.com/lynxkite/lynxkite/sphynx/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
)

func OperationInstanceFromJSON(opJSON string) OperationInstance {
	var opInst OperationInstance
	b := []byte(opJSON)
	json.Unmarshal(b, &opInst)
	return opInst
}

func shortOpName(opInst OperationInstance) string {
	className := opInst.Operation.Class
	return className[len("com.lynxanalytics.biggraph.graph_operations."):]
}

func NewServer() Server {
	dataDir := os.Getenv("ORDERED_SPHYNX_DATA_DIR")
	unorderedDataDir := os.Getenv("UNORDERED_SPHYNX_DATA_DIR")
	os.MkdirAll(unorderedDataDir, 0775)
	os.MkdirAll(dataDir, 0775)
	return Server{
		entityCache:      NewEntityCache(),
		dataDir:          dataDir,
		unorderedDataDir: unorderedDataDir}
}

func (s *Server) CanCompute(ctx context.Context, in *pb.CanComputeRequest) (*pb.CanComputeReply, error) {
	opInst := OperationInstanceFromJSON(in.Operation)
	exists := false
	switch in.Domain {
	case "SphynxMemory":
		op, ok := operationRepository[shortOpName(opInst)]
		if ok {
			if op.canCompute == nil {
				exists = true
			} else {
				exists = op.canCompute(opInst.Operation)
			}
		}
	case "OrderedSphynxDisk":
		op, ok := diskOperationRepository[shortOpName(opInst)]
		if ok {
			if op.canCompute == nil {
				exists = true
			} else {
				exists = op.canCompute(opInst.Operation)
			}
		}
	case "UnorderedSphynxDisk":
		op, ok := unorderedOperationRepository[shortOpName(opInst)]
		if ok {
			if op.canCompute == nil {
				exists = true
			} else {
				exists = op.canCompute(opInst.Operation)
			}
		}
	}
	return &pb.CanComputeReply{CanCompute: exists}, nil
}

func (s *Server) Compute(ctx context.Context, in *pb.ComputeRequest) (*pb.ComputeReply, error) {
	opInst := OperationInstanceFromJSON(in.Operation)
	log.Printf("Computing %v.", shortOpName(opInst))
	switch in.Domain {
	case "SphynxMemory":
		op, exists := operationRepository[shortOpName(opInst)]
		if !exists {
			return nil, fmt.Errorf("Can't compute %v in %v", opInst, in.Domain)
		}
		inputs, err := collectInputs(s, &opInst)
		if err != nil {
			return nil, err
		}
		ea := EntityAccessor{inputs: inputs, outputs: make(map[GUID]Entity), opInst: &opInst, server: s}
		err = op.execute(&ea)
		if err != nil {
			return nil, err
		}
		for name, _ := range opInst.Outputs {
			if strings.HasSuffix(name, "-idSet") {
				// Output names ending with '-idSet' are automatically generated edge bundle id sets.
				// Sphynx operations do not know about this, so we create these id sets based on the
				// edge bundle here.
				edgeBundleName := name[:len(name)-len("-idSet")]
				edgeBundleGuid := opInst.Outputs[edgeBundleName]
				edgeBundle := ea.outputs[edgeBundleGuid]
				switch eb := edgeBundle.(type) {
				case *EdgeBundle:
					idSet := VertexSet{MappingToUnordered: eb.EdgeMapping}
					ea.output(name, &idSet)
				default:
					return nil,
						fmt.Errorf("operation output (name : %v, guid: %v) is not an EdgeBundle",
							edgeBundleName, edgeBundleGuid)
				}
			}
		}
		for guid, entity := range ea.outputs {
			s.entityCache.Set(guid, entity)
		}
	case "OrderedSphynxDisk":
		op, exists := diskOperationRepository[shortOpName(opInst)]
		if !exists {
			return nil, fmt.Errorf("Can't compute %v in %v", opInst, in.Domain)
		}
		err := op.execute(s.dataDir, &opInst)
		if err != nil {
			return nil, err
		}
	case "UnorderedSphynxDisk":
		op, exists := unorderedOperationRepository[shortOpName(opInst)]
		if !exists {
			return nil, fmt.Errorf("Can't compute %v in %v", opInst, in.Domain)
		}
		err := op.execute(s.unorderedDataDir, &opInst)
		if err != nil {
			return nil, err
		}
	}
	return &pb.ComputeReply{}, nil
}

func (s *Server) GetScalar(ctx context.Context, in *pb.GetScalarRequest) (*pb.GetScalarReply, error) {
	guid := GUID(in.Guid)
	log.Printf("Received GetScalar request with GUID %v.", guid)
	entity, exists := s.entityCache.Get(guid)
	if !exists {
		return nil, NotInCacheError("scalar", guid)
	}

	switch scalar := entity.(type) {
	case *Scalar:
		return &pb.GetScalarReply{Scalar: string([]byte(*scalar))}, nil
	default:
		return nil, fmt.Errorf("entity %v (guid %v) is not a Scalar", scalar, guid)
	}

}

func (s *Server) HasInSphynxMemory(ctx context.Context, in *pb.HasInSphynxMemoryRequest) (*pb.HasInSphynxMemoryReply, error) {
	guid := GUID(in.Guid)
	_, exists := s.entityCache.Get(guid)
	return &pb.HasInSphynxMemoryReply{HasInMemory: exists}, nil
}

func (s *Server) getVertexSet(guid GUID) (*VertexSet, error) {
	entity, exists := s.entityCache.Get(guid)
	if !exists {
		return nil, NotInCacheError("vertex set", guid)
	}
	switch vs := entity.(type) {
	case *VertexSet:
		return vs, nil
	default:
		return nil, fmt.Errorf("Guid %v is a %T, not a vertex set", guid, vs)
	}
}

func (s *Server) HasOnOrderedSphynxDisk(ctx context.Context, in *pb.HasOnOrderedSphynxDiskRequest) (*pb.HasOnOrderedSphynxDiskReply, error) {
	guid := in.GetGuid()
	has, err := hasOnDisk(s.dataDir, GUID(guid))
	if err != nil {
		return nil, err
	}
	return &pb.HasOnOrderedSphynxDiskReply{HasOnDisk: has}, nil
}

func (s *Server) ReadFromOrderedSphynxDisk(ctx context.Context, in *pb.ReadFromOrderedSphynxDiskRequest) (*pb.ReadFromOrderedSphynxDiskReply, error) {
	guid := GUID(in.GetGuid())
	entity, err := loadFromOrderedDisk(s.dataDir, guid)
	if err != nil {
		return nil, err
	}
	s.entityCache.Set(guid, entity)
	return &pb.ReadFromOrderedSphynxDiskReply{}, nil
}

func (s *Server) Clear(ctx context.Context, in *pb.ClearRequest) (*pb.ClearReply, error) {
	switch in.Domain {
	case "SphynxMemory":
		s.entityCache.Clear()
	case "OrderedSphynxDisk":
		os.RemoveAll(s.dataDir)
		os.MkdirAll(s.dataDir, 0775)
	case "UnorderedSphynxDisk":
		os.RemoveAll(s.unorderedDataDir)
		os.MkdirAll(s.unorderedDataDir, 0775)
	}
	return &pb.ClearReply{}, nil
}

func main() {
	port := os.Getenv("SPHYNX_PORT")
	if port == "" {
		log.Fatalf("Please set SPHYNX_PORT.")
	}
	debugPort := os.Getenv("SPHYNX_DEBUG_PORT")
	if debugPort != "" {
		go func() error {
			return http.ListenAndServe(fmt.Sprintf(":%s", debugPort), nil)
		}()
	}
	keydir := os.Getenv("SPHYNX_CERT_DIR")
	var s *grpc.Server
	if keydir != "" {
		creds, err := credentials.NewServerTLSFromFile(keydir+"/cert.pem", keydir+"/private-key.pem")
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
	log.Printf("Sphynx listening on port %v", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

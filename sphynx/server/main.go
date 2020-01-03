// Sphynx is a gRPC server. LynxKite can connect to it and ask it to do some work.
// The idea is that Sphynx performs operations on graphs that fit into the memory,
// so there's no need to do slow distributed computations.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	pb "github.com/biggraph/biggraph/sphynx/proto"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"os"
	"strings"
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
	op, exists := operationRepository[shortenedClass]
	return op, exists
}

func NewServer() Server {
	dataDir := os.Getenv("SPHYNX_DATA_DIR")
	unorderedDataDir := os.Getenv("UNORDERED_SPHYNX_DATA_DIR")
	os.MkdirAll(unorderedDataDir, 0775)
	os.MkdirAll(dataDir, 0775)
	return Server{
		entities:         make(map[GUID]EntityPtr),
		dataDir:          dataDir,
		unorderedDataDir: unorderedDataDir}
}

func (s *Server) CanCompute(ctx context.Context, in *pb.CanComputeRequest) (*pb.CanComputeReply, error) {
	opInst := OperationInstanceFromJSON(in.Operation)
	_, exists := getExecutableOperation(opInst)
	return &pb.CanComputeReply{CanCompute: exists}, nil
}

// Temporary solution: Relocating to OrderedSphynxDisk is done here,
// we simply save any output, except the scalars.
// TODO: Saving should not be done automatically here
// to prevent getScalar from OrderedSphynxDomain
func saveOutputs(dataDir string, outputs map[GUID]EntityPtr) {
	for guid, entity := range outputs {
		err := saveToOrderedDisk(entity, dataDir, guid)
		if err != nil {
			log.Printf("Error while saving %v (guid: %v): %v", entity, guid, err)
		}
	}
}

func (s *Server) Compute(ctx context.Context, in *pb.ComputeRequest) (*pb.ComputeReply, error) {
	//	log.Printf("Compute called")
	opInst := OperationInstanceFromJSON(in.Operation)
	op, exists := getExecutableOperation(opInst)
	if !exists {
		return nil, status.Errorf(codes.Unimplemented, "Can't compute %v", opInst)
	} else {
		ea := EntityAccessor{outputs: make(map[GUID]EntityPtr), opInst: &opInst, server: s}
		err := op.execute(&ea)
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
					idSet := VertexSet{Mapping: eb.EdgeMapping}
					ea.add(name, &idSet)
				default:
					return nil,
						fmt.Errorf("operation output (name : %v, guid: %v) is not an EdgeBundle",
							edgeBundleName, edgeBundleGuid)
				}
			}
		}
		s.Lock()
		for guid, entity := range ea.outputs {
			s.entities[guid] = entity
		}
		s.Unlock()
		go saveOutputs(s.dataDir, ea.outputs)
		return &pb.ComputeReply{}, nil
	}
}

func (s *Server) GetScalar(ctx context.Context, in *pb.GetScalarRequest) (*pb.GetScalarReply, error) {
	guid := GUID(in.Guid)
	log.Printf("Received GetScalar request with GUID %v.", guid)
	entity, exists := s.get(guid)
	if !exists {
		return nil, fmt.Errorf("guid %v is not found", guid)
	}
	switch scalar := entity.(type) {
	case *Scalar:
		scalarJSON, err := json.Marshal(scalar.Value)
		if err != nil {
			return nil, status.Errorf(codes.Unknown, "Converting scalar to json failed: %v", err)
		}
		return &pb.GetScalarReply{Scalar: string(scalarJSON)}, nil
	default:
		return nil, fmt.Errorf("entity %v (guid %v) is not a Scalar", scalar, guid)
	}

}

func (s *Server) HasInSphynxMemory(ctx context.Context, in *pb.HasInSphynxMemoryRequest) (*pb.HasInSphynxMemoryReply, error) {
	guid := GUID(in.Guid)
	_, exists := s.get(guid)
	return &pb.HasInSphynxMemoryReply{HasInMemory: exists}, nil
}

func (s *Server) getVertexSet(guid GUID) (*VertexSet, error) {
	e, ok := s.get(guid)
	if !ok {
		return nil, fmt.Errorf("Guid %v not found among entities", guid)
	}
	switch vs := e.(type) {
	case *VertexSet:
		return vs, nil
	default:
		return nil, fmt.Errorf("Guid %v is a %T, not a vertex set", guid, vs)
	}
}

func (s *Server) WriteToUnorderedDisk(ctx context.Context, in *pb.WriteToUnorderedDiskRequest) (*pb.WriteToUnorderedDiskReply, error) {
	var numGoRoutines int64 = 4
	guid := GUID(in.Guid)

	entity, exists := s.get(guid)
	if !exists {
		return nil, fmt.Errorf("guid %v not found", guid)
	}
	log.Printf("Reindexing %v to use spark IDs.", entity)
	fname := fmt.Sprintf("%v/%v", s.unorderedDataDir, guid)
	fw, err := local.NewLocalFileWriter(fname)
	defer fw.Close()
	if err != nil {
		log.Printf("Failed to create file: %v", err)
		return nil, err
	}
	switch e := entity.(type) {
	case *VertexSet:
		pw, err := writer.NewParquetWriter(fw, new(Vertex), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		for _, v := range e.Mapping {
			if err := pw.Write(Vertex{Id: v}); err != nil {
				return nil, status.Errorf(codes.Unknown,
					"Failed to write parquet file: %v", err)
			}
		}
		if err = pw.WriteStop(); err != nil {
			return nil, status.Errorf(codes.Unknown,
				"Parquet WriteStop error: %v", err)
		}
		return &pb.WriteToUnorderedDiskReply{}, nil
	case *EdgeBundle:
		vs1, err := s.getVertexSet(GUID(in.Vsguid1))
		if err != nil {
			return nil, err
		}
		vs2, err := s.getVertexSet(GUID(in.Vsguid2))
		if err != nil {
			return nil, err
		}
		pw, err := writer.NewParquetWriter(fw, new(Edge), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		for sphynxId, sparkId := range e.EdgeMapping {
			err := pw.Write(Edge{
				Id:  sparkId,
				Src: vs1.Mapping[e.Src[sphynxId]],
				Dst: vs2.Mapping[e.Dst[sphynxId]],
			})
			if err != nil {
				return nil, status.Errorf(codes.Unknown,
					"Failed to write parquet file: %v", err)
			}
		}
		if err = pw.WriteStop(); err != nil {
			return nil, status.Errorf(codes.Unknown,
				"Parquet WriteStop error: %v", err)
		}
		return &pb.WriteToUnorderedDiskReply{}, nil
	case *StringAttribute:
		vs1, err := s.getVertexSet(GUID(in.Vsguid1))
		if err != nil {
			return nil, err
		}
		pw, err := writer.NewParquetWriter(fw, new(SingleStringAttribute), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		if err != nil {
			return nil, err
		}
		for sphynxId, def := range e.Defined {
			if def {
				sparkId := vs1.Mapping[sphynxId]
				err := pw.Write(SingleStringAttribute{
					Id:    sparkId,
					Value: e.Values[sphynxId],
				})
				if err != nil {
					return nil, status.Errorf(codes.Unknown,
						"Failed to write parquet file: %v", err)
				}
			}
		}
		if err = pw.WriteStop(); err != nil {
			return nil, status.Errorf(codes.Unknown,
				"Parquet WriteStop error: %v", err)
		}
		return &pb.WriteToUnorderedDiskReply{}, nil
	case *DoubleAttribute:
		vs1, err := s.getVertexSet(GUID(in.Vsguid1))
		if err != nil {
			return nil, err
		}
		pw, err := writer.NewParquetWriter(fw, new(SingleDoubleAttribute), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		if err != nil {
			return nil, err
		}
		for sphynxId, def := range e.Defined {
			if def {
				sparkId := vs1.Mapping[sphynxId]
				err := pw.Write(SingleDoubleAttribute{
					Id:    sparkId,
					Value: e.Values[sphynxId],
				})
				if err != nil {
					return nil, status.Errorf(codes.Unknown,
						"Failed to write parquet file: %v", err)
				}
			}
		}
		if err = pw.WriteStop(); err != nil {
			return nil, status.Errorf(codes.Unknown,
				"Parquet WriteStop error: %v", err)
		}
		return &pb.WriteToUnorderedDiskReply{}, nil
	case *DoubleTuple2Attribute:
		vs1, err := s.getVertexSet(GUID(in.Vsguid1))
		if err != nil {
			return nil, err
		}
		pw, err := writer.NewParquetWriter(fw, new(SingleDoubleTuple2Attribute), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		if err != nil {
			return nil, err
		}
		for sphynxId, def := range e.Defined {
			if def {
				sparkId := vs1.Mapping[sphynxId]
				err := pw.Write(SingleDoubleTuple2Attribute{
					Id:     sparkId,
					Value1: e.Values1[sphynxId],
					Value2: e.Values2[sphynxId],
				})
				if err != nil {
					return nil, status.Errorf(codes.Unknown,
						"Failed to write parquet file: %v", err)
				}
			}
		}
		if err = pw.WriteStop(); err != nil {
			return nil, status.Errorf(codes.Unknown,
				"Parquet WriteStop error: %v", err)
		}
		return &pb.WriteToUnorderedDiskReply{}, nil
	default:
		return nil, status.Errorf(
			codes.Unimplemented, "Can't reindex entity %v with GUID %v to use Spark IDs.", entity, in.Guid)
	}
}

func (s *Server) HasOnOrderedSphynxDisk(ctx context.Context, in *pb.HasOnOrderedSphynxDiskRequest) (*pb.HasOnOrderedSphynxDiskReply, error) {
	guid := in.GetGuid()
	has := hasOnDisk(GUID(guid))
	return &pb.HasOnOrderedSphynxDiskReply{HasOnDisk: has}, nil
}

func (s *Server) ReadFromOrderedSphynxDisk(ctx context.Context, in *pb.ReadFromOrderedSphynxDiskRequest) (*pb.ReadFromOrderedSphynxDiskReply, error) {
	guid := GUID(in.GetGuid())
	if !hasOnDisk(guid) {
		return nil, status.Errorf(codes.NotFound, "Guid %v not found in sphynx disk cache", guid)
	}
	entity, err := loadFromOrderedDisk(s.dataDir, guid)
	if err != nil {
		return nil, err
	}
	s.Lock()
	s.entities[guid] = entity
	s.Unlock()
	return &pb.ReadFromOrderedSphynxDiskReply{}, nil
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
	sphynxServer.initDisk() // For now, we just block, until the files are checked
	pb.RegisterSphynxServer(s, &sphynxServer)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

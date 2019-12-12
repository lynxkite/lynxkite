// Sphynx is a gRPC server. LynxKite can connect to it and ask it to do some work.
// The idea is that Sphynx performs operations on graphs that fit into the memory,
// so there's no need to do slow distributed computations.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"encoding/json"
	pb "github.com/biggraph/biggraph/sphynx/proto"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
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
	log.Printf("Received: %v", in.Operation)
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
		for name, guid := range opInst.Outputs {
			if strings.HasSuffix(name, "-idSet") {
				// Output names ending with '-idSet' are automatically generated edge bundle id sets.
				// Sphynx operations do not know about this, so we create these id sets based on the
				// edge bundle here.
				edgeBundleName := name[:len(name)-len("-idSet")]
				e := outputs[edgeBundleName]
				switch e := e.(type) {
				case EdgeBundle:
					idSet := VertexSet{e.edgeMapping}
					s.entities[guid] = idSet
				default:
					return nil, status.Errorf(codes.Unknown, "%v is not an EdgeBundle", e)
				}
			} else {
				s.entities[guid] = outputs[name]
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

func (s *Server) WriteToUnorderedDisk(ctx context.Context, in *pb.WriteToUnorderedDiskRequest) (*pb.WriteToUnorderedDiskReply, error) {
	var numGoRoutines int64 = 4
	entity := s.entities[GUID(in.Guid)]
	log.Printf("Reindexing %v to use spark IDs.", entity)
	fname := fmt.Sprintf("%v/%v", s.unorderedDataDir, in.Guid)
	fw, err := local.NewLocalFileWriter(fname)
	defer fw.Close()
	if err != nil {
		log.Printf("Failed to create file: %v", err)
	}
	switch e := entity.(type) {
	case VertexSet:
		pw, err := writer.NewParquetWriter(fw, new(Vertex), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		for _, v := range e.mapping {
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
	case EdgeBundle:
		pw, err := writer.NewParquetWriter(fw, new(Edge), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		for sphynxId, sparkId := range e.edgeMapping {
			err := pw.Write(Edge{
				Id:  sparkId,
				Src: e.src[sphynxId],
				Dst: e.dst[sphynxId],
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
	case StringAttribute:
		pw, err := writer.NewParquetWriter(fw, new(SingleStringAttribute), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		for sphynxId, def := range e.defined {
			if def {
				sparkId := e.vertexSet.mapping[sphynxId]
				err := pw.Write(SingleStringAttribute{
					Id:    sparkId,
					Value: e.values[sphynxId],
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
	case DoubleAttribute:
		pw, err := writer.NewParquetWriter(fw, new(SingleDoubleAttribute), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		for sphynxId, def := range e.defined {
			if def {
				sparkId := e.vertexSet.mapping[sphynxId]
				err := pw.Write(SingleDoubleAttribute{
					Id:    sparkId,
					Value: e.values[sphynxId],
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
	case DoubleTuple2Attribute:
		pw, err := writer.NewParquetWriter(fw, new(SingleDoubleTuple2Attribute), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		for sphynxId, def := range e.defined {
			if def {
				sparkId := e.vertexSet.mapping[sphynxId]
				err := pw.Write(SingleDoubleTuple2Attribute{
					Id:     sparkId,
					Value1: e.values1[sphynxId],
					Value2: e.values2[sphynxId],
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

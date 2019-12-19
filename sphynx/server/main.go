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
	return Server{entities: EntityMap{
		vertexSets:             make(map[GUID]*VertexSet),
		edgeBundles:            make(map[GUID]*EdgeBundle),
		scalars:                make(map[GUID]*Scalar),
		stringAttributes:       make(map[GUID]*StringAttribute),
		doubleAttributes:       make(map[GUID]*DoubleAttribute),
		doubleTuple2Attributes: make(map[GUID]*DoubleTuple2Attribute),
	},
		dataDir: dataDir, unorderedDataDir: unorderedDataDir}
}

func (s *Server) CanCompute(ctx context.Context, in *pb.CanComputeRequest) (*pb.CanComputeReply, error) {
	//	log.Printf("Received: %v", in.Operation)
	opInst := OperationInstanceFromJSON(in.Operation)
	_, exists := getExecutableOperation(opInst)
	return &pb.CanComputeReply{CanCompute: exists}, nil
}

func (s *Server) MergeAndSaveOutputs(output OperationOutput) error {
	s.entities.Lock()
	defer s.entities.Unlock()
	for guid, entity := range output.vertexSets {
		s.entities.vertexSets[guid] = entity
		err := s.saveEntityAndThenReloadAsATest(guid, entity)
		if err != nil {
			return err
		}
	}
	for guid, entity := range output.doubleAttributes {
		s.entities.doubleAttributes[guid] = entity
		err := s.saveEntityAndThenReloadAsATest(guid, entity)
		if err != nil {
			return err
		}
	}
	for guid, entity := range output.doubleTuple2Attributes {
		s.entities.doubleTuple2Attributes[guid] = entity
		err := s.saveEntityAndThenReloadAsATest(guid, entity)
		if err != nil {
			return err
		}
	}
	for guid, entity := range output.edgeBundles {
		s.entities.edgeBundles[guid] = entity
		err := s.saveEntityAndThenReloadAsATest(guid, entity)
		if err != nil {
			return err
		}
	}
	for guid, entity := range output.stringAttributes {
		s.entities.stringAttributes[guid] = entity
		err := s.saveEntityAndThenReloadAsATest(guid, entity)
		if err != nil {
			return err
		}
	}
	for guid, scalar := range output.scalars {
		s.entities.scalars[guid] = scalar
		err := s.saveEntityAndThenReloadAsATest(guid, scalar)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) Compute(ctx context.Context, in *pb.ComputeRequest) (*pb.ComputeReply, error) {
	log.Printf("Compute called")
	opInst := OperationInstanceFromJSON(in.Operation)
	op, exists := getExecutableOperation(opInst)
	if !exists {
		return nil, status.Errorf(codes.Unimplemented, "Can't compute %v", opInst)
	} else {
		outputs := op.execute(s, opInst)
		for name, guid := range opInst.Outputs {
			if strings.HasSuffix(name, "-idSet") {
				// Output names ending with '-idSet' are automatically generated edge bundle id sets.
				// Sphynx operations do not know about this, so we create these id sets based on the
				// edge bundle here.
				edgeBundleName := name[:len(name)-len("-idSet")]
				e := outputs.edgeBundles[opInst.Outputs[edgeBundleName]]
				idSet := VertexSet{Mapping: e.EdgeMapping}
				outputs.vertexSets[guid] = &idSet
			}
		}
		err := s.MergeAndSaveOutputs(outputs)
		if err != nil {
			return nil, err
		}
		return &pb.ComputeReply{}, nil
	}
}

func (s *Server) GetScalarInner(guid GUID) (*Scalar, error) {
	// This serves for both domains SphynxMemory and SphynxDisk. If
	// SphynxMemory has it, we return it, otherwise we retrieve it
	// from SphynxDisk
	em := s.entities
	em.Lock()
	scalar, exists := em.scalars[guid]
	em.Unlock()
	if exists {
		return scalar, nil
	}
	entity, err := s.loadEntity(guid)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Entity (scalar) %v cannot be loaded: %v", guid, err)
	}
	switch s := entity.(type) {
	case *Scalar:
		em.Lock()
		em.scalars[guid] = s
		em.Unlock()
		return s, nil
	default:
		return nil, status.Errorf(codes.NotFound, "Entity %v should be a Scalar, but it is %T", guid, s)
	}
}

func (s *Server) GetScalar(ctx context.Context, in *pb.GetScalarRequest) (*pb.GetScalarReply, error) {
	guid := GUID(in.Guid)
	log.Printf("Received GetScalar request with GUID %v.", guid)
	scalar, err := s.GetScalarInner(guid)
	if err != nil {
		return nil, err
	}
	scalarJSON, err := json.Marshal(scalar.Value)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "Converting scalar to json failed: %v", err)
	}
	return &pb.GetScalarReply{Scalar: string(scalarJSON)}, nil
}

func (s *Server) GetVertexSet(vsGuid GUID) (*VertexSet, error) {
	s.entities.Lock()
	defer s.entities.Unlock()
	vs, exists := s.entities.vertexSets[vsGuid]
	if !exists {
		return nil, status.Errorf(codes.Unknown, "VertexSet guid %v is missing", vsGuid)
	}
	return vs, nil
}

func (s *Server) WriteToUnorderedDisk(ctx context.Context, in *pb.WriteToUnorderedDiskRequest) (*pb.WriteToUnorderedDiskReply, error) {
	var numGoRoutines int64 = 4
	entity := s.entities.get(GUID(in.Guid))
	log.Printf("Reindexing %v to use spark IDs.", entity)
	fname := fmt.Sprintf("%v/%v", s.unorderedDataDir, in.Guid)
	fw, err := local.NewLocalFileWriter(fname)
	defer fw.Close()
	if err != nil {
		log.Printf("Failed to create file: %v", err)
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
		pw, err := writer.NewParquetWriter(fw, new(Edge), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		for sphynxId, sparkId := range e.EdgeMapping {
			err := pw.Write(Edge{
				Id:  sparkId,
				Src: e.Src[sphynxId],
				Dst: e.Dst[sphynxId],
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
		pw, err := writer.NewParquetWriter(fw, new(SingleStringAttribute), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		vs, err := s.GetVertexSet(e.VertexSetGuid)
		if err != nil {
			return nil, err
		}
		for sphynxId, def := range e.Defined {
			if def {
				sparkId := vs.Mapping[sphynxId]
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
		pw, err := writer.NewParquetWriter(fw, new(SingleDoubleAttribute), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		vs, err := s.GetVertexSet(e.VertexSetGuid)
		if err != nil {
			return nil, err
		}
		for sphynxId, def := range e.Defined {
			if def {
				sparkId := vs.Mapping[sphynxId]
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
		pw, err := writer.NewParquetWriter(fw, new(SingleDoubleTuple2Attribute), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet writer: %v", err)
		}
		vs, err := s.GetVertexSet(e.VertexSetGuid)
		if err != nil {
			return nil, err
		}
		for sphynxId, def := range e.Defined {
			if def {
				sparkId := vs.Mapping[sphynxId]
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

func (s *Server) HasOnSphynxDisk(ctx context.Context, in *pb.HasOnSphynxDiskRequest) (*pb.HasOnSphynxDiskReply, error) {
	guid := in.GetGuid()
	has := hasOnDisk(GUID(guid))
	return &pb.HasOnSphynxDiskReply{HasOnDisk: has}, nil
}

func (s *Server) RelocateFromSphynxDisk(ctx context.Context, in *pb.RelocateFromSphynxDiskRequest) (*pb.RelocateFromSphynxDiskReply, error) {
	guid := GUID(in.GetGuid())
	log.Printf("RelocateFromSphynxDisk: %v", guid)
	if !hasOnDisk(guid) {
		return nil, status.Errorf(codes.NotFound, "Guid %v not found in sphynx disk cache", guid)
	}
	entity, err := s.loadEntity(guid)
	if err != nil {
		return nil, err
	}
	log.Printf("entity: %v", entity)
	s.entities.Lock()
	defer s.entities.Unlock()
	switch e := entity.(type) {
	case *VertexSet:
		s.entities.vertexSets[guid] = e
	case *EdgeBundle:
		s.entities.edgeBundles[guid] = e
	case *DoubleAttribute:
		s.entities.doubleAttributes[guid] = e
	case *DoubleTuple2Attribute:
		s.entities.doubleTuple2Attributes[guid] = e
	case *StringAttribute:
		s.entities.stringAttributes[guid] = e
	case *Scalar:
		s.entities.scalars[guid] = e
	default:
		return nil, status.Errorf(codes.Unknown, "Unknown entity type: %T", e)
	}
	return &pb.RelocateFromSphynxDiskReply{}, nil
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

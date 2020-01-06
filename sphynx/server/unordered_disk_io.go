package main

import (
	"context"
	"fmt"
	pb "github.com/biggraph/biggraph/sphynx/proto"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
	"log"
)

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
			return nil, fmt.Errorf("Failed to create parquet writer: %v", err)
		}
		for _, v := range e.Mapping {
			if err := pw.Write(Vertex{Id: v}); err != nil {
				return nil, fmt.Errorf("Failed to write parquet file: %v", err)
			}
		}
		if err = pw.WriteStop(); err != nil {
			return nil, fmt.Errorf("Parquet WriteStop error: %v", err)
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
				return nil, fmt.Errorf("Failed to write parquet file: %v", err)
			}
		}
		if err = pw.WriteStop(); err != nil {
			return nil, fmt.Errorf("Parquet WriteStop error: %v", err)
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
					return nil, fmt.Errorf("Failed to write parquet file: %v", err)
				}
			}
		}
		if err = pw.WriteStop(); err != nil {
			return nil, fmt.Errorf("Parquet WriteStop error: %v", err)
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
					return nil, fmt.Errorf("Failed to write parquet file: %v", err)
				}
			}
		}
		if err = pw.WriteStop(); err != nil {
			return nil, fmt.Errorf("Parquet WriteStop error: %v", err)
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
					return nil, fmt.Errorf("Failed to write parquet file: %v", err)
				}
			}
		}
		if err = pw.WriteStop(); err != nil {
			return nil, fmt.Errorf("Parquet WriteStop error: %v", err)
		}
		return &pb.WriteToUnorderedDiskReply{}, nil
	default:
		return nil, fmt.Errorf("Can't reindex entity %v with GUID %v to use Spark IDs.", entity, in.Guid)
	}
}

func (s *Server) ReadFromUnorderedDisk(
	ctx context.Context, in *pb.ReadFromUnorderedDiskRequest) (*pb.ReadFromUnorderedDiskReply, error) {
	var numGoRoutines int64 = 4
	fname := fmt.Sprintf("%v/%v", s.unorderedDataDir, in.Guid)
	fr, err := local.NewLocalFileReader(fname)
	defer fr.Close()
	if err != nil {
		log.Printf("Failed to open file: %v", err)
	}
	fmt.Println(in.Type)
	switch in.Type {
	case "VertexSet":
		pr, err := reader.NewParquetReader(fr, new(Vertex), numGoRoutines)
		if err != nil {
			log.Printf("Failed to create parquet reader: %v", err)
		}
		num_vs := int(pr.GetNumRows())
		rawVertexSet := make([]Vertex, num_vs)
		if err := pr.Read(&rawVertexSet); err != nil {
			return nil, fmt.Errorf("Failed to read parquet file: %v", err)
		}
		pr.ReadStop()
		vertexMapping := make([]int64, num_vs)
		for _, wrappedSparkId := range rawVertexSet {
			vertexMapping = append(vertexMapping, wrappedSparkId.Id)
		}
		s.Lock()
		s.entities[GUID(in.Guid)] = &VertexSet{Mapping: vertexMapping}
		s.Unlock()
		fmt.Println(VertexSet{Mapping: vertexMapping})
		return &pb.ReadFromUnorderedDiskReply{}, nil
	default:
		return nil, fmt.Errorf("Can't reindex entity with GUID %v to use Sphynx IDs.", in.Guid)
	}
}

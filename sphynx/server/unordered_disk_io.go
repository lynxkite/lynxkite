// Functions to read and write Unordered Sphynx Disk.

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
		return nil, fmt.Errorf("Failed to create file: %v", err)
	}
	switch e := entity.(type) {
	case *VertexSet:
		pw, err := writer.NewParquetWriter(fw, new(Vertex), numGoRoutines)
		if err != nil {
			return nil, fmt.Errorf("Failed to create parquet writer: %v", err)
		}
		for _, v := range e.MappingToUnordered {
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
			return nil, fmt.Errorf("Failed to create parquet writer: %v", err)
		}
		for sphynxId, sparkId := range e.EdgeMapping {
			err := pw.Write(Edge{
				Id:  sparkId,
				Src: vs1.MappingToUnordered[e.Src[sphynxId]],
				Dst: vs2.MappingToUnordered[e.Dst[sphynxId]],
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
			return nil, fmt.Errorf("Failed to create parquet writer: %v", err)
		}
		if err != nil {
			return nil, err
		}
		for sphynxId, def := range e.Defined {
			if def {
				sparkId := vs1.MappingToUnordered[sphynxId]
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
			return nil, fmt.Errorf("Failed to create parquet writer: %v", err)
		}
		if err != nil {
			return nil, err
		}
		for sphynxId, def := range e.Defined {
			if def {
				sparkId := vs1.MappingToUnordered[sphynxId]
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
			return nil, fmt.Errorf("Failed to create parquet writer: %v", err)
		}
		if err != nil {
			return nil, err
		}
		for sphynxId, def := range e.Defined {
			if def {
				sparkId := vs1.MappingToUnordered[sphynxId]
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
		return nil, fmt.Errorf("Failed to open file: %v", err)
	}
	switch in.Type {
	case "VertexSet":
		pr, err := reader.NewParquetReader(fr, new(Vertex), numGoRoutines)
		if err != nil {
			return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
		}
		num_vs := int(pr.GetNumRows())
		rawVertexSet := make([]Vertex, num_vs)
		if err := pr.Read(&rawVertexSet); err != nil {
			return nil, fmt.Errorf("Failed to read parquet file: %v", err)
		}
		pr.ReadStop()
		mappingToUnordered := make([]int64, 0, num_vs)
		mappingToOrdered := make(map[int64]int)
		for i, wrappedSparkId := range rawVertexSet {
			unorderedId := wrappedSparkId.Id
			mappingToUnordered = append(mappingToUnordered, unorderedId)
			mappingToOrdered[unorderedId] = i
		}
		s.Lock()
		s.entities[GUID(in.Guid)] = &VertexSet{
			MappingToUnordered: mappingToUnordered,
			MappingToOrdered:   mappingToOrdered,
		}
		s.Unlock()
		return &pb.ReadFromUnorderedDiskReply{}, nil
	case "EdgeBundle":
		vs1, err := s.getVertexSet(GUID(in.Vsguid1))
		if err != nil {
			return nil, err
		}
		vs2, err := s.getVertexSet(GUID(in.Vsguid2))
		if err != nil {
			return nil, err
		}
		pr, err := reader.NewParquetReader(fr, new(Edge), numGoRoutines)
		if err != nil {
			return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
		}
		num_es := int(pr.GetNumRows())
		rawEdgeBundle := make([]Edge, num_es)
		if err := pr.Read(&rawEdgeBundle); err != nil {
			return nil, fmt.Errorf("Failed to read parquet file: %v", err)
		}
		pr.ReadStop()
		edgeMapping := make([]int64, 0, num_es)
		src := make([]int, 0, num_es)
		dst := make([]int, 0, num_es)
		for _, rawEdge := range rawEdgeBundle {
			edgeMapping = append(edgeMapping, rawEdge.Id)
			src = append(src, vs1.MappingToOrdered[rawEdge.Src])
			dst = append(dst, vs2.MappingToOrdered[rawEdge.Dst])
		}
		s.Lock()
		s.entities[GUID(in.Guid)] = &EdgeBundle{
			Src:         src,
			Dst:         dst,
			EdgeMapping: edgeMapping,
		}
		s.Unlock()
		return &pb.ReadFromUnorderedDiskReply{}, nil
	case "StringAttribute":
		vs, err := s.getVertexSet(GUID(in.Vsguid1))
		if err != nil {
			return nil, err
		}
		num_vs := len(vs.MappingToUnordered)
		pr, err := reader.NewParquetReader(fr, new(SingleStringAttribute), numGoRoutines)
		if err != nil {
			return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
		}
		rawAttribute := make([]SingleStringAttribute, num_vs)
		if err := pr.Read(&rawAttribute); err != nil {
			return nil, fmt.Errorf("Failed to read parquet file: %v", err)
		}
		pr.ReadStop()
		values := make([]string, num_vs, num_vs)
		defined := make([]bool, num_vs, num_vs)
		for _, singleAttr := range rawAttribute {
			orderedId := vs.MappingToOrdered[singleAttr.Id]
			values[orderedId] = singleAttr.Value
			defined[orderedId] = true
		}
		s.Lock()
		s.entities[GUID(in.Guid)] = &StringAttribute{
			Values:  values,
			Defined: defined,
		}
		s.Unlock()
		return &pb.ReadFromUnorderedDiskReply{}, nil
	case "DoubleAttribute":
		vs, err := s.getVertexSet(GUID(in.Vsguid1))
		if err != nil {
			return nil, err
		}
		num_vs := len(vs.MappingToUnordered)
		pr, err := reader.NewParquetReader(fr, new(SingleDoubleAttribute), numGoRoutines)
		if err != nil {
			return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
		}
		rawAttribute := make([]SingleDoubleAttribute, num_vs)
		if err := pr.Read(&rawAttribute); err != nil {
			return nil, fmt.Errorf("Failed to read parquet file: %v", err)
		}
		pr.ReadStop()
		values := make([]float64, num_vs, num_vs)
		defined := make([]bool, num_vs, num_vs)
		for _, singleAttr := range rawAttribute {
			orderedId := vs.MappingToOrdered[singleAttr.Id]
			values[orderedId] = singleAttr.Value
			defined[orderedId] = true
		}
		s.Lock()
		s.entities[GUID(in.Guid)] = &DoubleAttribute{
			Values:  values,
			Defined: defined,
		}
		s.Unlock()
		return &pb.ReadFromUnorderedDiskReply{}, nil
	case "DoubleTuple2Attribute":
		vs, err := s.getVertexSet(GUID(in.Vsguid1))
		if err != nil {
			return nil, err
		}
		num_vs := len(vs.MappingToUnordered)
		pr, err := reader.NewParquetReader(fr, new(SingleDoubleTuple2Attribute), numGoRoutines)
		if err != nil {
			return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
		}
		rawAttribute := make([]SingleDoubleTuple2Attribute, num_vs)
		if err := pr.Read(&rawAttribute); err != nil {
			return nil, fmt.Errorf("Failed to read parquet file: %v", err)
		}
		pr.ReadStop()
		values1 := make([]float64, num_vs, num_vs)
		values2 := make([]float64, num_vs, num_vs)
		defined := make([]bool, num_vs, num_vs)
		for _, singleAttr := range rawAttribute {
			orderedId := vs.MappingToOrdered[singleAttr.Id]
			values1[orderedId] = singleAttr.Value1
			values2[orderedId] = singleAttr.Value2
			defined[orderedId] = true
		}
		s.Lock()
		s.entities[GUID(in.Guid)] = &DoubleTuple2Attribute{
			Values1: values1,
			Values2: values2,
			Defined: defined,
		}
		s.Unlock()
		return &pb.ReadFromUnorderedDiskReply{}, nil
	default:
		return nil, fmt.Errorf("Can't reindex entity with GUID %v to use Sphynx IDs.", in.Guid)
	}
}

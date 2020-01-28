// Functions to read and write Unordered Sphynx Disk.

package main

import (
	"context"
	"fmt"
	pb "github.com/biggraph/biggraph/sphynx/proto"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func toUnorderedRows(e ParquetEntity, vs1 *VertexSet, vs2 *VertexSet) []interface{} {
	switch e := e.(type) {
	case *VertexSet:
		return e.toUnorderedRows()
	case *EdgeBundle:
		return e.toUnorderedRows(vs1, vs2)
	default:
		return AttributeToUnorderedRows(e, vs1)
	}
}

func (s *Server) WriteToUnorderedDisk(ctx context.Context, in *pb.WriteToUnorderedDiskRequest) (*pb.WriteToUnorderedDiskReply, error) {
	const numGoRoutines int64 = 4
	guid := GUID(in.Guid)
	entity, exists := s.get(guid)
	if !exists {
		return nil, fmt.Errorf("guid %v not found", guid)
	}
	log.Printf("Reindexing entity with guid %v to use spark IDs.", guid)
	dirName := fmt.Sprintf("%v/%v", s.unorderedDataDir, guid)
	_ = os.Mkdir(dirName, 0775)
	fname := fmt.Sprintf("%v/part-00000.parquet", dirName)
	successFile := fmt.Sprintf("%v/_SUCCESS", dirName)
	fw, err := local.NewLocalFileWriter(fname)
	defer fw.Close()
	if err != nil {
		return nil, fmt.Errorf("Failed to create file: %v", err)
	}
	switch e := entity.(type) {
	case ParquetEntity:
		pw, err := writer.NewParquetWriter(fw, e.unorderedRow(), numGoRoutines)
		if err != nil {
			return nil, fmt.Errorf("Failed to create parquet writer: %v", err)
		}
		var vs1 *VertexSet
		var vs2 *VertexSet
		if in.Vsguid1 != "" {
			vs1, err = s.getVertexSet(GUID(in.Vsguid1))
			if err != nil {
				return nil, err
			}
		}
		if in.Vsguid2 != "" {
			vs2, err = s.getVertexSet(GUID(in.Vsguid2))
			if err != nil {
				return nil, err
			}
		}
		rows := toUnorderedRows(e, vs1, vs2)
		for _, row := range rows {
			if err := pw.Write(row); err != nil {
				return nil, fmt.Errorf("Failed to write parquet file: %v", err)
			}
		}
		if err = pw.WriteStop(); err != nil {
			return nil, fmt.Errorf("Parquet WriteStop error: %v", err)
		}
		err = ioutil.WriteFile(successFile, nil, 0775)
		if err != nil {
			return nil, fmt.Errorf("Failed to write Success File: %v", err)
		}
		return &pb.WriteToUnorderedDiskReply{}, nil
	case *Scalar:
		err = e.write(dirName)
		if err != nil {
			return nil, err
		}
		return &pb.WriteToUnorderedDiskReply{}, nil
	default:
		return nil, fmt.Errorf("Can't reindex entity %v with GUID %v to use Spark IDs.", entity, in.Guid)
	}
}

func (s *Server) ReadFromUnorderedDisk(
	ctx context.Context, in *pb.ReadFromUnorderedDiskRequest) (*pb.ReadFromUnorderedDiskReply, error) {
	const numGoRoutines int64 = 4
	dirName := fmt.Sprintf("%v/%v", s.unorderedDataDir, in.Guid)
	files, err := ioutil.ReadDir(dirName)
	if err != nil {
		return nil, fmt.Errorf("Failed to read directory: %v", err)
	}
	fileReaders := make([]source.ParquetFile, 0, len(files))
	for _, f := range files {
		fname := f.Name()
		if strings.HasPrefix(fname, "part-") {
			path := fmt.Sprintf("%v/%v", dirName, fname)
			fr, err := local.NewLocalFileReader(path)
			defer fr.Close()
			if err != nil {
				return nil, fmt.Errorf("Failed to open file: %v", err)
			}
			fileReaders = append(fileReaders, fr)
		}
	}
	var entity Entity
	switch in.Type {
	case "VertexSet":
		rawVertexSet := make([]UnorderedVertexRow, 0)
		numVS := 0
		for _, fr := range fileReaders {
			pr, err := reader.NewParquetReader(fr, new(UnorderedVertexRow), numGoRoutines)
			if err != nil {
				return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
			}
			partialNumVS := int(pr.GetNumRows())
			partialRawVertexSet := make([]UnorderedVertexRow, partialNumVS)
			numVS = numVS + partialNumVS
			if err := pr.Read(&partialRawVertexSet); err != nil {
				return nil, fmt.Errorf("Failed to read parquet file: %v", err)
			}
			pr.ReadStop()
			rawVertexSet = append(rawVertexSet, partialRawVertexSet...)
		}
		mappingToUnordered := make([]int64, numVS)
		mappingToOrdered := make(map[int64]int)
		for i, v := range rawVertexSet {
			mappingToUnordered[i] = v.Id
			mappingToOrdered[v.Id] = i
		}
		entity = &VertexSet{
			MappingToUnordered: mappingToUnordered,
			MappingToOrdered:   mappingToOrdered,
		}
	case "EdgeBundle":
		vs1, err := s.getVertexSet(GUID(in.Vsguid1))
		if err != nil {
			return nil, err
		}
		vs2, err := s.getVertexSet(GUID(in.Vsguid2))
		if err != nil {
			return nil, err
		}
		rawEdgeBundle := make([]UnorderedEdgeRow, 0)
		numES := 0
		for _, fr := range fileReaders {
			pr, err := reader.NewParquetReader(fr, new(UnorderedEdgeRow), numGoRoutines)
			if err != nil {
				return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
			}
			partialNumES := int(pr.GetNumRows())
			partialRawEdgeBundle := make([]UnorderedEdgeRow, partialNumES)
			numES = numES + partialNumES
			if err := pr.Read(&partialRawEdgeBundle); err != nil {
				return nil, fmt.Errorf("Failed to read parquet file: %v", err)
			}
			pr.ReadStop()
			rawEdgeBundle = append(rawEdgeBundle, partialRawEdgeBundle...)
		}
		edgeMapping := make([]int64, numES)
		src := make([]int, numES)
		dst := make([]int, numES)
		mappingToOrdered1 := vs1.GetMappingToOrdered()
		mappingToOrdered2 := vs2.GetMappingToOrdered()
		for i, rawEdge := range rawEdgeBundle {
			edgeMapping[i] = rawEdge.Id
			src[i] = mappingToOrdered1[rawEdge.Src]
			dst[i] = mappingToOrdered2[rawEdge.Dst]
		}
		entity = &EdgeBundle{
			Src:         src,
			Dst:         dst,
			EdgeMapping: edgeMapping,
		}
	case "Attribute":
		attributeType := in.AttributeType[len("TypeTag[") : len(in.AttributeType)-1]
		switch attributeType {
		case "String":
			vs, err := s.getVertexSet(GUID(in.Vsguid1))
			if err != nil {
				return nil, err
			}
			rawAttribute := make([]UnorderedStringAttributeRow, 0)
			numVS := 0
			for _, fr := range fileReaders {
				pr, err := reader.NewParquetReader(fr, new(UnorderedStringAttributeRow), numGoRoutines)
				if err != nil {
					return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
				}
				partialNumVS := int(pr.GetNumRows())
				partialRawAttribute := make([]UnorderedStringAttributeRow, partialNumVS)
				numVS = numVS + partialNumVS
				if err := pr.Read(&partialRawAttribute); err != nil {
					return nil, fmt.Errorf("Failed to read parquet file: %v", err)
				}
				pr.ReadStop()
				rawAttribute = append(rawAttribute, partialRawAttribute...)
			}
			values := make([]string, numVS)
			defined := make([]bool, numVS)
			mappingToOrdered := vs.GetMappingToOrdered()
			for _, singleAttr := range rawAttribute {
				orderedId := mappingToOrdered[singleAttr.Id]
				values[orderedId] = singleAttr.Value
				defined[orderedId] = true
			}
			entity = &StringAttribute{
				Values:  values,
				Defined: defined,
			}
		case "Double":
			vs, err := s.getVertexSet(GUID(in.Vsguid1))
			if err != nil {
				return nil, err
			}
			rawAttribute := make([]UnorderedDoubleAttributeRow, 0)
			numVS := 0
			for _, fr := range fileReaders {
				pr, err := reader.NewParquetReader(fr, new(UnorderedDoubleAttributeRow), numGoRoutines)
				if err != nil {
					return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
				}
				partialNumVS := int(pr.GetNumRows())
				partialRawAttribute := make([]UnorderedDoubleAttributeRow, partialNumVS)
				numVS = numVS + partialNumVS
				if err := pr.Read(&partialRawAttribute); err != nil {
					return nil, fmt.Errorf("Failed to read parquet file: %v", err)
				}
				pr.ReadStop()
				rawAttribute = append(rawAttribute, partialRawAttribute...)
			}
			values := make([]float64, numVS)
			defined := make([]bool, numVS)
			mappingToOrdered := vs.GetMappingToOrdered()
			for _, singleAttr := range rawAttribute {
				orderedId := mappingToOrdered[singleAttr.Id]
				values[orderedId] = singleAttr.Value
				defined[orderedId] = true
			}
			entity = &DoubleAttribute{
				Values:  values,
				Defined: defined,
			}
		case "(Double, Double)":
			vs, err := s.getVertexSet(GUID(in.Vsguid1))
			if err != nil {
				return nil, err
			}
			rawAttribute := make([]UnorderedDoubleTuple2AttributeRow, 0)
			numVS := 0
			for _, fr := range fileReaders {
				pr, err := reader.NewParquetReader(fr, new(UnorderedDoubleTuple2AttributeRow), numGoRoutines)
				if err != nil {
					return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
				}
				partialNumVS := int(pr.GetNumRows())
				partialRawAttribute := make([]UnorderedDoubleTuple2AttributeRow, partialNumVS)
				numVS = numVS + partialNumVS
				if err := pr.Read(&partialRawAttribute); err != nil {
					return nil, fmt.Errorf("Failed to read parquet file: %v", err)
				}
				pr.ReadStop()
				rawAttribute = append(rawAttribute, partialRawAttribute...)
			}
			values := make([]DoubleTuple2AttributeValue, numVS)
			defined := make([]bool, numVS)
			mappingToOrdered := vs.GetMappingToOrdered()
			for _, singleAttr := range rawAttribute {
				orderedId := mappingToOrdered[singleAttr.Id]
				values[orderedId] = singleAttr.Value
				defined[orderedId] = true
			}
			entity = &DoubleTuple2Attribute{
				Values:  values,
				Defined: defined,
			}
		default:
			return nil, fmt.Errorf("Can't reindex attribute of type %v with GUID %v to use Sphynx IDs.", attributeType, in.Guid)
		}
	case "Scalar":
		sc, err := readScalar(dirName)
		if err != nil {
			return nil, err
		}
		entity = &sc
	default:
		return nil, fmt.Errorf("Can't reindex entity of type %v with GUID %v to use Sphynx IDs.", in.Type, in.Guid)
	}
	s.Lock()
	s.entities[GUID(in.Guid)] = entity
	s.Unlock()
	return &pb.ReadFromUnorderedDiskReply{}, nil
}

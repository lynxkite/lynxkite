// Functions to read and write Unordered Sphynx Disk.

package main

import (
	"bufio"
	"context"
	"encoding/json"
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

func writeSuccessFile(dirName string) {

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
		err = ioutil.WriteFile(successFile, nil, 0775)
		if err != nil {
			return nil, fmt.Errorf("Failed to write Success File: %v", err)
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
		err = ioutil.WriteFile(successFile, nil, 0775)
		if err != nil {
			return nil, fmt.Errorf("Failed to write Success File: %v", err)
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
		err = ioutil.WriteFile(successFile, nil, 0775)
		if err != nil {
			return nil, fmt.Errorf("Failed to write Success File: %v", err)
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
		err = ioutil.WriteFile(successFile, nil, 0775)
		if err != nil {
			return nil, fmt.Errorf("Failed to write Success File: %v", err)
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
		err = ioutil.WriteFile(successFile, nil, 0775)
		if err != nil {
			return nil, fmt.Errorf("Failed to write Success File: %v", err)
		}
		return &pb.WriteToUnorderedDiskReply{}, nil
	case *Scalar:
		scalarJSON, err := json.Marshal(e.Value)
		if err != nil {
			return nil, fmt.Errorf("Converting scalar to json failed: %v", err)
		}
		fname := fmt.Sprintf("%v/serialized_data", dirName)
		f, err := os.Create(fname)
		fw := bufio.NewWriter(f)
		if _, err := fw.WriteString(string(scalarJSON)); err != nil {
			return nil, fmt.Errorf("Writing scalar to file failed: %v", err)
		}
		fw.Flush()
		err = ioutil.WriteFile(successFile, nil, 0775)
		if err != nil {
			return nil, fmt.Errorf("Failed to write Success File: %v", err)
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
		rawVertexSet := make([]Vertex, 0)
		numVS := 0
		for _, fr := range fileReaders {
			pr, err := reader.NewParquetReader(fr, new(Vertex), numGoRoutines)
			if err != nil {
				return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
			}
			partialNumVS := int(pr.GetNumRows())
			partialRawVertexSet := make([]Vertex, partialNumVS)
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
		rawEdgeBundle := make([]Edge, 0)
		numES := 0
		for _, fr := range fileReaders {
			pr, err := reader.NewParquetReader(fr, new(Edge), numGoRoutines)
			if err != nil {
				return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
			}
			partialNumES := int(pr.GetNumRows())
			partialRawEdgeBundle := make([]Edge, partialNumES)
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
			rawAttribute := make([]SingleStringAttribute, 0)
			numVS := 0
			for _, fr := range fileReaders {
				pr, err := reader.NewParquetReader(fr, new(SingleStringAttribute), numGoRoutines)
				if err != nil {
					return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
				}
				partialNumVS := int(pr.GetNumRows())
				partialRawAttribute := make([]SingleStringAttribute, partialNumVS)
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
			return &pb.ReadFromUnorderedDiskReply{}, nil
		case "Double":
			vs, err := s.getVertexSet(GUID(in.Vsguid1))
			if err != nil {
				return nil, err
			}
			rawAttribute := make([]SingleDoubleAttribute, 0)
			numVS := 0
			for _, fr := range fileReaders {
				pr, err := reader.NewParquetReader(fr, new(SingleDoubleAttribute), numGoRoutines)
				if err != nil {
					return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
				}
				partialNumVS := int(pr.GetNumRows())
				partialRawAttribute := make([]SingleDoubleAttribute, partialNumVS)
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
			rawAttribute := make([]SingleDoubleTuple2Attribute, 0)
			numVS := 0
			for _, fr := range fileReaders {
				pr, err := reader.NewParquetReader(fr, new(SingleDoubleTuple2Attribute), numGoRoutines)
				if err != nil {
					return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
				}
				partialNumVS := int(pr.GetNumRows())
				partialRawAttribute := make([]SingleDoubleTuple2Attribute, partialNumVS)
				numVS = numVS + partialNumVS
				if err := pr.Read(&partialRawAttribute); err != nil {
					return nil, fmt.Errorf("Failed to read parquet file: %v", err)
				}
				pr.ReadStop()
				rawAttribute = append(rawAttribute, partialRawAttribute...)
			}
			values1 := make([]float64, numVS)
			values2 := make([]float64, numVS)
			defined := make([]bool, numVS)
			mappingToOrdered := vs.GetMappingToOrdered()
			for _, singleAttr := range rawAttribute {
				orderedId := mappingToOrdered[singleAttr.Id]
				values1[orderedId] = singleAttr.Value1
				values2[orderedId] = singleAttr.Value2
				defined[orderedId] = true
			}
			entity = &DoubleTuple2Attribute{
				Values1: values1,
				Values2: values2,
				Defined: defined,
			}
		default:
			return nil, fmt.Errorf("Can't reindex attribute of type %v with GUID %v to use Sphynx IDs.", attributeType, in.Guid)
		}
	default:
		return nil, fmt.Errorf("Can't reindex entity of type %v with GUID %v to use Sphynx IDs.", in.Type, in.Guid)
	}
	s.Lock()
	s.entities[GUID(in.Guid)] = entity
	s.Unlock()
	return &pb.ReadFromUnorderedDiskReply{}, nil
}

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
	"reflect"
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
	log.Printf("Reindexing entity with guid %v to use Sphynx IDs.", in.Guid)
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
	if in.Type == "Attribute" {
		attributeType := in.AttributeType[len("TypeTag[") : len(in.AttributeType)-1]
		switch attributeType {
		case "(Double, Double)":
			in.Type = "DoubleTuple2Attribute"
		case "Vector[Double]":
			in.Type = "DoubleVectorAttribute"
		default:
			in.Type = attributeType + in.Type
		}
	}
	entity, err := createEntity(in.Type)
	if err != nil {
		return nil, err
	}
	switch e := entity.(type) {
	case *VertexSet:
		rows := make([]UnorderedVertexRow, 0)
		numRows := 0
		for _, fr := range fileReaders {
			pr, err := reader.NewParquetReader(fr, e.unorderedRow(), numGoRoutines)
			if err != nil {
				return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
			}
			partialNumRows := int(pr.GetNumRows())
			partialRows := make([]UnorderedVertexRow, partialNumRows)
			numRows = numRows + partialNumRows
			if err := pr.Read(&partialRows); err != nil {
				return nil, fmt.Errorf("Failed to read parquet file of VertexSet: %v", err)
			}
			pr.ReadStop()
			rows = append(rows, partialRows...)
		}
		mappingToUnordered := make([]int64, numRows)
		mappingToOrdered := make(map[int64]SphynxId)
		for i, v := range rows {
			mappingToUnordered[i] = v.Id
			mappingToOrdered[v.Id] = SphynxId(i)
		}
		entity = &VertexSet{
			MappingToUnordered: mappingToUnordered,
			MappingToOrdered:   mappingToOrdered,
		}
	case *EdgeBundle:
		vs1, err := s.getVertexSet(GUID(in.Vsguid1))
		if err != nil {
			return nil, err
		}
		vs2, err := s.getVertexSet(GUID(in.Vsguid2))
		if err != nil {
			return nil, err
		}
		rows := make([]UnorderedEdgeRow, 0)
		numRows := 0
		for _, fr := range fileReaders {
			pr, err := reader.NewParquetReader(fr, new(UnorderedEdgeRow), numGoRoutines)
			if err != nil {
				return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
			}
			partialNumRows := int(pr.GetNumRows())
			partialRows := make([]UnorderedEdgeRow, partialNumRows)
			numRows = numRows + partialNumRows
			if err := pr.Read(&partialRows); err != nil {
				return nil, fmt.Errorf("Failed to read parquet file of EdgeBundle: %v", err)
			}
			pr.ReadStop()
			rows = append(rows, partialRows...)
		}
		edgeMapping := make([]int64, numRows)
		src := make([]SphynxId, numRows)
		dst := make([]SphynxId, numRows)
		mappingToOrdered1 := vs1.GetMappingToOrdered()
		mappingToOrdered2 := vs2.GetMappingToOrdered()
		for i, row := range rows {
			edgeMapping[i] = row.Id
			src[i] = mappingToOrdered1[row.Src]
			dst[i] = mappingToOrdered2[row.Dst]
		}
		entity = &EdgeBundle{
			Src:         src,
			Dst:         dst,
			EdgeMapping: edgeMapping,
		}
	case ParquetEntity:
		vs, err := s.getVertexSet(GUID(in.Vsguid1))
		if err != nil {
			return nil, err
		}
		numVS := len(vs.MappingToUnordered)
		rowType := reflect.Indirect(reflect.ValueOf(e.unorderedRow())).Type()
		rowSliceType := reflect.SliceOf(rowType)
		rowsPointer := reflect.New(rowSliceType)
		rows := rowsPointer.Elem()

		numRows := 0
		for _, fr := range fileReaders {
			pr, err := reader.NewParquetReader(fr, e.unorderedRow(), numGoRoutines)
			if err != nil {
				return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
			}
			partialNumRows := int(pr.GetNumRows())
			partialRowsPointer := reflect.New(rowSliceType)
			partialRows := partialRowsPointer.Elem()
			partialRows.Set(reflect.MakeSlice(rowSliceType, partialNumRows, partialNumRows))
			numRows = partialNumRows + numRows
			if err := pr.Read(partialRowsPointer.Interface()); err != nil {
				return nil, fmt.Errorf("Failed to read parquet file of %v: %v", reflect.TypeOf(e), err)
			}
			pr.ReadStop()
			rows = reflect.AppendSlice(rows, partialRows)
		}

		attr := reflect.ValueOf(e)
		InitializeAttribute(attr, numVS)
		values := attr.Elem().FieldByName("Values")
		defined := attr.Elem().FieldByName("Defined")
		idIndex := fieldIndex(rowType, "Id")
		valueIndex := fieldIndex(rowType, "Value")
		mappingToOrdered := vs.GetMappingToOrdered()
		true := reflect.ValueOf(true)
		for i := 0; i < numRows; i++ {
			row := rows.Index(i)
			orderedId := mappingToOrdered[row.Field(idIndex).Int()]
			values.Index(int(orderedId)).Set(row.Field(valueIndex))
			defined.Index(int(orderedId)).Set(true)
		}
	case *Scalar:
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

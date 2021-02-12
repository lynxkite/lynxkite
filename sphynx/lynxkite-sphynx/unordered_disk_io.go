// Functions to read and write Unordered Sphynx Disk.

package main

import (
	"context"
	"fmt"
	pb "github.com/lynxkite/lynxkite/sphynx/proto"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
)

func toUnorderedRows(e TabularEntity, vs1 *VertexSet, vs2 *VertexSet) []interface{} {
	switch e := e.(type) {
	case *VertexSet:
		return e.toUnorderedRows()
	case *EdgeBundle:
		return e.toUnorderedRows(vs1, vs2)
	default:
		return AttributeToUnorderedRows(e, vs1)
	}
}

func sortIds(ids []int64) {
	sort.Sort(Int64Slice(ids))
}
func assertSorted(ids []int64) {
	if !sort.IsSorted(Int64Slice(ids)) {
		// The previous loglines will point out which entity this is.
		panic("These IDs are not sorted.")
	}
}

type Int64Slice []int64

func (a Int64Slice) Len() int {
	return len(a)
}
func (a Int64Slice) Less(i, j int) bool {
	return a[i] < a[j]
}
func (a Int64Slice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

type unorderedEdgesSorter struct {
	rows []UnorderedEdgeRow
	less func(i, j int) bool
}

func (self unorderedEdgesSorter) Len() int {
	return len(self.rows)
}
func (self unorderedEdgesSorter) Swap(i, j int) {
	self.rows[i], self.rows[j] = self.rows[j], self.rows[i]
}
func (self unorderedEdgesSorter) Less(i, j int) bool {
	return self.less(i, j)
}
func sortEdgeRows(rows []UnorderedEdgeRow, less func(i, j int) bool) {
	sort.Sort(unorderedEdgesSorter{rows, less})
}

func (s *Server) WriteToUnorderedDisk(ctx context.Context, in *pb.WriteToUnorderedDiskRequest) (*pb.WriteToUnorderedDiskReply, error) {
	const numGoRoutines int64 = 4
	guid := GUID(in.Guid)
	entity, exists := s.entityCache.Get(guid)
	if !exists {
		return nil, fmt.Errorf("Guid %v is missing", guid)
	}
	log.Printf("Writing %v %v to unordered disk.", entity.typeName(), guid)
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
	case TabularEntity:
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
	if in.Type == "Attribute" {
		attributeType := in.AttributeType[len("TypeTag[") : len(in.AttributeType)-1]
		switch attributeType {
		case "Vector[Double]":
			in.Type = "DoubleVectorAttribute"
		case "Array[com.lynxanalytics.biggraph.graph_api.ID]":
			in.Type = "LongVectorAttribute"
		case "com.lynxanalytics.biggraph.graph_api.ID":
			in.Type = "LongAttribute"
		default:
			in.Type = attributeType + in.Type
		}
	}
	log.Printf("Reading %v %v from unordered disk.", in.Type, in.Guid)
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
		for i, v := range rows {
			mappingToUnordered[i] = v.Id
		}
		sortIds(mappingToUnordered)
		entity = &VertexSet{
			MappingToUnordered: mappingToUnordered,
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
		for _, fr := range fileReaders {
			pr, err := reader.NewParquetReader(fr, new(UnorderedEdgeRow), numGoRoutines)
			if err != nil {
				return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
			}
			partialNumRows := int(pr.GetNumRows())
			partialRows := make([]UnorderedEdgeRow, partialNumRows)
			if err := pr.Read(&partialRows); err != nil {
				return nil, fmt.Errorf("Failed to read parquet file of EdgeBundle: %v", err)
			}
			pr.ReadStop()
			rows = append(rows, partialRows...)
		}
		// Translate Src to ordered IDs.
		sortEdgeRows(rows, func(i, j int) bool { return rows[i].Src < rows[j].Src })
		for i, j := 0, 0; i < len(vs1.MappingToUnordered) && j < len(rows); {
			if vs1.MappingToUnordered[i] == rows[j].Src {
				rows[j].Src = int64(i)
				j++
			} else {
				i++
			}
		}
		// Translate Dst to ordered IDs.
		sortEdgeRows(rows, func(i, j int) bool { return rows[i].Dst < rows[j].Dst })
		for i, j := 0, 0; i < len(vs2.MappingToUnordered) && j < len(rows); {
			if vs2.MappingToUnordered[i] == rows[j].Dst {
				rows[j].Dst = int64(i)
				j++
			} else {
				i++
			}
		}
		// Store the results ordered by edge ID.
		sortEdgeRows(rows, func(i, j int) bool { return rows[i].Id < rows[j].Id })
		es := NewEdgeBundle(len(rows), len(rows))
		for i, row := range rows {
			es.EdgeMapping[i] = row.Id
			es.Src[i] = SphynxId(row.Src)
			es.Dst[i] = SphynxId(row.Dst)
		}
		entity = es
	case TabularEntity:
		vs, err := s.getVertexSet(GUID(in.Vsguid1))
		if err != nil {
			return nil, err
		}
		numVS := len(vs.MappingToUnordered)
		rowType := reflect.Indirect(reflect.ValueOf(e.unorderedRow())).Type()
		rowSliceType := reflect.SliceOf(rowType)
		rowsPointer := reflect.New(rowSliceType)
		rows := rowsPointer.Elem()
		for _, fr := range fileReaders {
			pr, err := reader.NewParquetReader(fr, e.unorderedRow(), numGoRoutines)
			if err != nil {
				return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
			}
			partialNumRows := int(pr.GetNumRows())
			partialRowsPointer := reflect.New(rowSliceType)
			partialRows := partialRowsPointer.Elem()
			partialRows.Set(reflect.MakeSlice(rowSliceType, partialNumRows, partialNumRows))
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
		true := reflect.ValueOf(true)
		sort.Slice(rows.Interface(), func(i, j int) bool {
			return rows.Index(i).Field(idIndex).Int() < rows.Index(j).Field(idIndex).Int()
		})
		for i, j := 0, 0; i < len(vs.MappingToUnordered) && j < rows.Len(); i++ {
			row := rows.Index(j)
			if vs.MappingToUnordered[i] == row.Field(idIndex).Int() {
				values.Index(i).Set(row.Field(valueIndex))
				defined.Index(i).Set(true)
				j++
			}
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
	s.entityCache.Set(GUID(in.Guid), entity)
	return &pb.ReadFromUnorderedDiskReply{}, nil
}

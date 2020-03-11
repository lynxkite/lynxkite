package main

import (
	"bufio"
	"context"
	"fmt"
	pb "github.com/biggraph/biggraph/sphynx/proto"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
	"io/ioutil"
	"log"
	"os"
)

func createEntity(typeName string) (Entity, error) {
	switch typeName {
	case "VertexSet":
		return &VertexSet{}, nil
	case "EdgeBundle":
		return &EdgeBundle{}, nil
	case "Scalar":
		return &Scalar{}, nil
	case "DoubleAttribute":
		return &DoubleAttribute{}, nil
	case "StringAttribute":
		return &StringAttribute{}, nil
	case "DoubleTuple2Attribute":
		return &DoubleTuple2Attribute{}, nil
	case "DoubleVectorAttribute":
		return &DoubleVectorAttribute{}, nil
	case "LongAttribute":
		return &LongAttribute{}, nil
	default:
		return nil, fmt.Errorf("Unknown entity to load: %v", typeName)
	}
}

func saveToOrderedDisk(e Entity, dataDir string, guid GUID) error {
	log.Printf("saveToOrderedDisk guid %v", guid)
	typeName := e.typeName()
	dirName := fmt.Sprintf("%v/%v", dataDir, guid)
	_ = os.Mkdir(dirName, 0775)
	typeFName := fmt.Sprintf("%v/type_name", dirName)
	typeFile, err := os.Create(typeFName)
	if err != nil {
		return err
	}
	tfw := bufio.NewWriter(typeFile)
	if _, err := tfw.WriteString(string(typeName)); err != nil {
		return fmt.Errorf("Failed to create type file: %v", err)
	}
	tfw.Flush()
	switch e := e.(type) {
	case ParquetEntity:
		onDisk, err := hasOnDisk(dataDir, guid)
		if err != nil {
			return err
		}
		if onDisk {
			log.Printf("guid %v is already on disk", guid)
			return nil
		}
		const numGoRoutines int64 = 4
		fname := fmt.Sprintf("%v/data.parquet", dirName)
		successFile := fmt.Sprintf("%v/_SUCCESS", dirName)
		fw, err := local.NewLocalFileWriter(fname)
		defer fw.Close()
		if err != nil {
			return fmt.Errorf("Failed to create file: %v", err)
		}
		pw, err := writer.NewParquetWriter(fw, e.orderedRow(), numGoRoutines)
		if err != nil {
			return fmt.Errorf("Failed to create parquet writer: %v", err)
		}
		rows := toOrderedRows(e)
		for _, row := range rows {
			if err := pw.Write(row); err != nil {
				return fmt.Errorf("Failed to write parquet file: %v", err)
			}
		}
		if err = pw.WriteStop(); err != nil {
			return fmt.Errorf("Parquet WriteStop error: %v", err)
		}
		err = ioutil.WriteFile(successFile, nil, 0775)
		if err != nil {
			return fmt.Errorf("Failed to write success file: %v", err)
		}
		return nil
	case *Scalar:
		return e.write(dirName)
	default:
		return fmt.Errorf("Can't write entity with GUID %v to Ordered Sphynx Disk.", guid)
	}
}

func toOrderedRows(e ParquetEntity) []interface{} {
	switch e := e.(type) {
	case *VertexSet:
		return e.toOrderedRows()
	case *EdgeBundle:
		return e.toOrderedRows()
	default:
		return AttributeToOrderedRows(e)
	}
}

func readFromOrdered(e ParquetEntity, pr *reader.ParquetReader) error {
	numRows := int(pr.GetNumRows())
	switch e := e.(type) {
	case *VertexSet:
		return e.readFromOrdered(pr, numRows)
	case *EdgeBundle:
		return e.readFromOrdered(pr, numRows)
	default:
		return ReadAttributeFromOrdered(e, pr, numRows)
	}
}

func loadFromOrderedDisk(dataDir string, guid GUID) (Entity, error) {
	log.Printf("loadFromOrderedDisk: %v", guid)
	dirName := fmt.Sprintf("%v/%v", dataDir, guid)
	typeFName := fmt.Sprintf("%v/type_name", dirName)
	typeData, err := ioutil.ReadFile(typeFName)
	if err != nil {
		return nil, fmt.Errorf("Failed to read type of %v: %v", dirName, err)
	}
	typeName := string(typeData)
	e, err := createEntity(typeName)
	if err != nil {
		return nil, err
	}
	switch e := e.(type) {
	case ParquetEntity:
		const numGoRoutines int64 = 4
		fname := fmt.Sprintf("%v/data.parquet", dirName)
		onDisk, err := hasOnDisk(dataDir, guid)
		if err != nil {
			return nil, err
		}
		if !onDisk {
			return nil, fmt.Errorf("Path is not present: %v", dirName)
		}
		fr, err := local.NewLocalFileReader(fname)
		defer fr.Close()
		if err != nil {
			return nil, fmt.Errorf("Failed to open %v: %v", dirName, err)
		}
		pr, err := reader.NewParquetReader(fr, e.orderedRow(), numGoRoutines)
		if err != nil {
			return nil, fmt.Errorf("Failed to create parquet reader %v: %v", dirName, err)
		}
		if err = readFromOrdered(e, pr); err != nil {
			return nil, fmt.Errorf("Could not read %v: %v", dirName, err)
		}
		pr.ReadStop()
	case *Scalar:
		*e, err = readScalar(dirName)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Failed to read entity with GUID %v from Ordered Sphynx Disk.", guid)
	}
	return e, nil
}

func (s *Server) WriteToOrderedDisk(
	ctx context.Context, in *pb.WriteToOrderedDiskRequest) (*pb.WriteToOrderedDiskReply, error) {
	guid := GUID(in.Guid)

	e, exists := s.entityCache.Get(guid)
	if !exists {
		return nil, fmt.Errorf("Guid %v is missing", guid)
	}

	if err := saveToOrderedDisk(e, s.dataDir, guid); err != nil {
		return nil, fmt.Errorf("failed to write %v to ordered disk: %v", guid, err)
	}
	return &pb.WriteToOrderedDiskReply{}, nil
}

func hasOnDisk(dataDir string, guid GUID) (bool, error) {
	filename := fmt.Sprintf("%v/%v/_SUCCESS", dataDir, guid)
	_, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

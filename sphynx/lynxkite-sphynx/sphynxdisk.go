package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	pb "github.com/lynxkite/lynxkite/sphynx/proto"
	"io/ioutil"
	"log"
	"os"
)

var arrowAllocator = memory.NewGoAllocator()

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
	case "DoubleVectorAttribute":
		return &DoubleVectorAttribute{}, nil
	case "LongAttribute":
		return &LongAttribute{}, nil
	case "LongVectorAttribute":
		return &LongVectorAttribute{}, nil
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
	case TabularEntity:
		onDisk, err := hasOnDisk(dataDir, guid)
		if err != nil {
			return err
		}
		if onDisk {
			log.Printf("guid %v is already on disk", guid)
			return nil
		}
		fname := fmt.Sprintf("%v/data.arrow", dirName)
		successFile := fmt.Sprintf("%v/_SUCCESS", dirName)
		f, err := os.Create(fname)
		if err != nil {
			return fmt.Errorf("Failed to create file: %v", err)
		}
		rec := e.toOrderedRows()
		w, err := ipc.NewFileWriter(
			f, ipc.WithSchema(rec.Schema()), ipc.WithAllocator(arrowAllocator))
		if err != nil {
			return fmt.Errorf("Failed to create Arrow writer: %v", err)
		}
		if err = w.Write(rec); err != nil {
			return fmt.Errorf("Failed to write Arrow file: %v", err)
		}
		if err = w.Close(); err != nil {
			return fmt.Errorf("Failed to write Arrow file: %v", err)
		}
		if err = f.Close(); err != nil {
			return fmt.Errorf("Failed to write Arrow file: %v", err)
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
	case TabularEntity:
		const numGoRoutines int64 = 4
		fname := fmt.Sprintf("%v/data.arrow", dirName)
		onDisk, err := hasOnDisk(dataDir, guid)
		if err != nil {
			return nil, err
		}
		if !onDisk {
			return nil, fmt.Errorf("Path is not present: %v", dirName)
		}
		f, err := os.Open(fname)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r, err := ipc.NewFileReader(f, ipc.WithAllocator(arrowAllocator))
		if err != nil {
			return nil, fmt.Errorf("Failed to open %v: %v", dirName, err)
		}
		defer r.Close()
		// Arrow files can have multiple records. We user zero records for empty
		// entities or one record. We never use more than one record.
		if r.NumRecords() == 1 {
			rec, err := r.Record(0)
			if err != nil {
				return nil, fmt.Errorf("Failed to read %v: %v", dirName, err)
			}
			defer rec.Release()
			if err = e.readFromOrdered(rec); err != nil {
				return nil, fmt.Errorf("Could not read %v: %v", dirName, err)
			}
		} else if r.NumRecords() > 1 {
			return nil, fmt.Errorf("%v has %v records, expected 1.", dirName, r.NumRecords())
		}
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

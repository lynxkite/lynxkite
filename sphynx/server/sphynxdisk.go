package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sync"
)

var filesOnDiskMutex sync.Mutex
var filesOnDisk = map[GUID]bool{}
var inprogressSuffix = ".inprogress"

func performIO(fields []EntityField, dir string, fn func(*error, string, interface{})) error {
	var wg sync.WaitGroup
	errors := make([]error, len(fields))
	for idx, f := range fields {
		dataFile := fmt.Sprintf("%v/%v", dir, f.fieldName)
		data := f.data
		err := &errors[idx]
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(err, dataFile, data)
		}()
	}
	wg.Wait()
	for _, e := range errors {
		if e != nil {
			return e
		}
	}
	return nil
}

func fieldSaver(err *error, path string, data interface{}) {
	file, e := os.Create(path)
	if e != nil {
		*err = e
		return
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	encoder := gob.NewEncoder(writer)
	e = encoder.Encode(data)
	if e != nil {
		*err = e
		return
	}
	*err = writer.Flush()
}

func fieldLoader(err *error, path string, data interface{}) {
	file, e := os.Open(path)
	if e != nil {
		*err = e
		return
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	decoder := gob.NewDecoder(reader)
	*err = decoder.Decode(data)
}

func createEntity(name string) (EntityPtr, error) {
	switch name {
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
	default:
		return nil, fmt.Errorf("Unknown entity to load: %v", name)
	}
}

func loadFromOrderedDisk(dataDir string, guid GUID) (EntityPtr, error) {
	log.Printf("loadFromOrderedDisk: %v", guid)
	if !hasOnDisk(guid) {
		return nil, fmt.Errorf("Path is not present in disk cache: %v", guid)
	}
	dir := fmt.Sprintf("%v/%v", dataDir, guid)
	var err error = nil
	var name string
	namePath := fmt.Sprintf("%v/%v", dir, "name")
	fieldLoader(&err, namePath, &name)
	if err != nil {
		log.Printf("Err: %v", err)
		return nil, err
	}
	log.Printf("Name: %v", name)
	entity, err := createEntity(name)
	if err != nil {
		return nil, err
	}
	err = performIO(entity.fields(), dir, fieldLoader)
	return entity, err
}

func saveToOrderedDiskInner(e EntityPtr, path string) error {
	name := e.name()
	ef := EntityField{fieldName: "name", data: name}
	fields := append(e.fields(), ef)
	return performIO(fields, path, fieldSaver)
}

func saveToOrderedDisk(e EntityPtr, dataDir string, guid GUID) error {
	log.Printf(" saveToOrderedDisk guid %v", guid)
	if hasOnDisk(guid) {
		log.Printf("guid %v is already on disk", guid)
		return nil
	}
	realDir := fmt.Sprintf("%v/%v", dataDir, guid)
	inProgressDir := fmt.Sprintf("%v%v", realDir, inprogressSuffix)
	err := os.Mkdir(inProgressDir, 0775)
	if err != nil {
		return err
	}
	err = saveToOrderedDiskInner(e, inProgressDir)
	if err != nil {
		return err
	}
	err = os.Rename(inProgressDir, realDir)
	if err != nil {
		return err
	}
	registerToDisk(guid)
	return nil
}

func hasOnDisk(guid GUID) bool {
	filesOnDiskMutex.Lock()
	defer filesOnDiskMutex.Unlock()
	_, has := filesOnDisk[guid]
	return has
}

func registerToDisk(guid GUID) {
	filesOnDiskMutex.Lock()
	defer filesOnDiskMutex.Unlock()
	filesOnDisk[guid] = true
}

func (server *Server) initDisk() error {
	log.Printf("InitDisk")
	rootDir := server.dataDir
	filesOnDiskMutex.Lock()
	defer filesOnDiskMutex.Unlock()
	err := filepath.Walk(rootDir, func(file string, info os.FileInfo, err error) error {
		if file != rootDir {
			guid := GUID(file[len(rootDir)+1:])
			filesOnDisk[guid] = true
			// TODO: Check if path is a guid
			// TODO: Delete any inprogress files
			log.Printf("Found on disk: %v", guid)
		}
		return nil
	})
	return err
}

func getConcreteTypeBasedOnFirstByte(reader *bufio.Reader) (EntityPtr, error) {
	code, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	switch code {
	case VertexSetCode:
		return &VertexSet{}, nil
	case EdgeBundleCode:
		return &EdgeBundle{}, nil
	case ScalarCode:
		return &Scalar{}, nil
	case DoubleTuple2AttributeCode:
		return &DoubleTuple2Attribute{}, nil
	case StringAttributeCode:
		return &StringAttribute{}, nil
	case DoubleAttributeCode:
		return &DoubleAttribute{}, nil
	default:
		return nil, status.Errorf(codes.Unknown, "Unknown EntityCode: %v", code)
	}
}

func (server *Server) loadEntity(guid GUID) (EntityPtr, error) {
	log.Printf("loadEntity: %v", guid)
	if !hasOnDisk(guid) {
		return nil, status.Errorf(codes.NotFound,
			"Path is not present in disk cache: %v", guid)
	}
	realPath := fmt.Sprintf("%v/%v", server.dataDir, guid)
	file, err := os.Open(realPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	entity, err := getConcreteTypeBasedOnFirstByte(reader)
	decoder := gob.NewDecoder(reader)
	err = decoder.Decode(entity)
	return entity, err
}

func (server *Server) saveEntityAndThenReloadAsATest(guid GUID, entity EntityPtr) error {
	err := server.saveEntity(guid, entity)
	if err != nil {
		return err
	}
	reloaded, err := server.loadEntity(guid)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(reloaded, entity) {
		return status.Errorf(codes.NotFound, "Reload check failed for %v", guid)
	}
	return nil
}

func (server *Server) saveEntity(guid GUID, entity EntityPtr) (errStatus error) {
	if hasOnDisk(guid) {
		log.Printf("guid %v is already on disk", guid)
		return nil
	}
	realPath := fmt.Sprintf("%v/%v", server.dataDir, guid)
	inProgressPath := fmt.Sprintf("%v%v", realPath, inprogressSuffix)
	file, err := os.Create(inProgressPath)
	if err != nil {
		return err
	}
	defer func() {
		e := file.Close()
		if e == nil {
			if errStatus == nil {
				errStatus = os.Rename(inProgressPath, realPath)
				if errStatus == nil {
					registerToDisk(guid)
				}
			}
		}
	}()
	writer := bufio.NewWriter(file)
	switch e := entity.(type) {
	case *VertexSet:
		err = writer.WriteByte(VertexSetCode)
	case *EdgeBundle:
		err = writer.WriteByte(EdgeBundleCode)
	case *Scalar:
		err = writer.WriteByte(ScalarCode)
	case *DoubleTuple2Attribute:
		err = writer.WriteByte(DoubleTuple2AttributeCode)
	case *StringAttribute:
		err = writer.WriteByte(StringAttributeCode)
	case *DoubleAttribute:
		err = writer.WriteByte(DoubleAttributeCode)
	default:
		return status.Errorf(codes.Unknown, "Unknown entity: %v, type: %T", e)
	}
	if err != nil {
		return err
	}

	encoder := gob.NewEncoder(writer)
	err = encoder.Encode(entity)
	if err != nil {
		return err
	}
	err = writer.Flush()
	return err
}

package main

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
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

type orderedDiskMeta struct {
	Name string
}

func saveEntityMeta(e EntityPtr, path string) error {
	meta := orderedDiskMeta{Name: e.name()}
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	log.Printf("meta: %v  b: %v", meta, b)
	return nil

}

func saveData(wg *sync.WaitGroup, echan *error, path string, i interface{}) {
	defer wg.Done()
	log.Printf("saveData: path: %v", path)
	defer log.Printf("saveData: over")
	file, err := os.Create(path)
	if err != nil {
		log.Printf("saveData: 1")
		*echan = err
		return
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	encoder := gob.NewEncoder(writer)
	err = encoder.Encode(i)
	if err != nil {
		log.Printf("saveData: 2")
		*echan = err
		return
	}
	err = writer.Flush()
	if err != nil {
		log.Printf("saveData: 3")
		*echan = err
		return
	}
	log.Printf("saveData: 4")
	*echan = nil
	log.Printf("saveData: 5")
}

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
	return os.Rename(inProgressDir, realDir)
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
	gob.Register(VertexSet{})
	gob.Register(DoubleAttribute{})
	gob.Register(DoubleTuple2Attribute{})
	gob.Register(EdgeBundle{})
	gob.Register(StringAttribute{})
	gob.Register(Scalar{})
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

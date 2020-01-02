package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var filesOnDiskMutex sync.Mutex
var filesOnDisk = map[GUID]bool{}
var inprogressSuffix = ".inprogress"

func parallelIO(fields []EntityField, dir string, fn func(*error, string, interface{})) error {
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
	err = parallelIO(entity.fields(), dir, fieldLoader)
	return entity, err
}

func saveToOrderedDiskInner(e EntityPtr, path string) error {
	name := e.name()
	ef := EntityField{fieldName: "name", data: name}
	fields := append(e.fields(), ef)
	return parallelIO(fields, path, fieldSaver)
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
	rootDir := server.dataDir
	filesOnDiskMutex.Lock()
	defer filesOnDiskMutex.Unlock()
	err := filepath.Walk(rootDir, func(file string, info os.FileInfo, err error) error {
		if file != rootDir {
			last := file[len(rootDir)+1:]
			if !strings.Contains(last, "/") {
				guid := GUID(last)
				filesOnDisk[guid] = true
				// TODO: Check if path is a guid
				// TODO: Delete any inprogress files
				log.Printf("Found on disk: %v", guid)
			}
		}
		return nil
	})
	return err
}

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

func hasOnDisk(guid GUID) bool {
	filesOnDiskMutex.Lock()
	defer filesOnDiskMutex.Unlock()
	_, has := filesOnDisk[guid]
	log.Printf("hasOnDisk %v -> %v", guid, has)
	return has
}

func registerToDisk(guid GUID) {
	filesOnDiskMutex.Lock()
	defer filesOnDiskMutex.Unlock()
	filesOnDisk[guid] = true
	log.Printf("guid %v registered on disk", guid)
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

func (server *Server) loadEntity(guid GUID) (interface{}, error) {
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
	decoder := gob.NewDecoder(reader)
	var entity interface{}
	err = decoder.Decode(&entity)
	return entity, err
}

func (server *Server) saveEntityAndThenReloadAsATest(guid GUID, entity interface{}) error {
	log.Printf("saveEntityAndThenReloadAsATest: guid: %v", guid)
	err := server.saveEntity(guid, entity)
	defer func() {
		log.Printf("Error: %v", err)
	}()
	if err != nil {
		return err
	}
	reloaded, err := server.loadEntity(guid)
	if err != nil {
		return err
	}
	if reflect.DeepEqual(reloaded, entity) {
		log.Printf("reloaded: [%v]", reloaded)
		log.Printf("entity:   [%v]", entity)
		return status.Errorf(codes.NotFound, "Reload check failed for %v", guid)
	}
	return nil
}

func (server *Server) saveEntity(guid GUID, entity interface{}) (errStatus error) {
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
		if e != nil {
			errStatus = e
		} else if err == nil {
			errStatus = os.Rename(inProgressPath, realPath)
			if errStatus == nil {
				registerToDisk(guid)
				log.Printf("guid %v has bee written to %v ", guid, realPath)
			}
		}
	}()
	writer := bufio.NewWriter(file)
	encoder := gob.NewEncoder(writer)
	err = encoder.Encode(&entity)
	if err != nil {
		return err
	}
	err = writer.Flush()
	return err
}

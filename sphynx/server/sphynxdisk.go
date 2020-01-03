package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"sync"
)

const inprogressSuffix = ".inprogress"

// To perform io operations in parallel. So that each field can have a different goroutine
// that performs the read or a write
func forEachField(fields []EntityField, dir string, fn func(string, interface{}) error) error {
	var wg sync.WaitGroup
	errors := make([]error, len(fields))
	for idx, f := range fields {
		dataFile := fmt.Sprintf("%v/%v", dir, f.fieldName)
		data := f.data
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errors[i] = fn(dataFile, data)
		}(idx)
	}
	wg.Wait()
	for _, e := range errors {
		if e != nil {
			return e
		}
	}
	return nil
}

func fieldSaver(path string, data interface{}) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	encoder := gob.NewEncoder(writer)
	err = encoder.Encode(data)
	if err != nil {
		return err
	}
	return writer.Flush()
}

func fieldLoader(path string, data interface{}) error {
	file, e := os.Open(path)
	if e != nil {
		return e
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	decoder := gob.NewDecoder(reader)
	return decoder.Decode(data)
}

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
	default:
		return nil, fmt.Errorf("Unknown entity to load: %v", typeName)
	}
}

func loadFromOrderedDisk(dataDir string, guid GUID) (Entity, error) {
	log.Printf("loadFromOrderedDisk: %v", guid)
	onDisk, err := hasOnDisk(dataDir, guid)
	if err != nil {
		return nil, err
	}
	if !onDisk {
		return nil, fmt.Errorf("Path is not present : %v", guid)
	}
	dir := fmt.Sprintf("%v/%v", dataDir, guid)

	var name string
	namePath := fmt.Sprintf("%v/%v", dir, "typename")
	err = fieldLoader(namePath, &name)
	if err != nil {
		log.Printf("Err: %v", err)
		return nil, err
	}
	log.Printf("Name: %v", name)
	entity, err := createEntity(name)
	if err != nil {
		return nil, err
	}
	err = forEachField(entity.fields(), dir, fieldLoader)
	return entity, err
}

func saveToOrderedDisk(e Entity, dataDir string, guid GUID) error {
	log.Printf(" saveToOrderedDisk guid %v", guid)
	onDisk, err := hasOnDisk(dataDir, guid)
	if err != nil {
		return err
	}
	if onDisk {
		log.Printf("guid %v is already on disk", guid)
		return nil
	}
	realDir := fmt.Sprintf("%v/%v", dataDir, guid)
	inProgressDir := fmt.Sprintf("%v%v", realDir, inprogressSuffix)
	err = os.Mkdir(inProgressDir, 0775)
	if err != nil {
		return err
	}
	typeName := e.typeName()
	ef := EntityField{fieldName: "typename", data: typeName}
	fields := append(e.fields(), ef)
	err = forEachField(fields, inProgressDir, fieldSaver)
	if err != nil {
		return err
	}
	err = os.Rename(inProgressDir, realDir)
	if err != nil {
		return err
	}
	return nil
}

func hasOnDisk(dataDir string, guid GUID) (bool, error) {
	filename := fmt.Sprintf("%v/%v", dataDir, guid)
	fi, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if !fi.IsDir() {
		return false, fmt.Errorf("%v should be a directory", filename)
	}
	return true, nil
}

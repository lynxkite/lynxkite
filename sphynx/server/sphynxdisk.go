package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
	"io/ioutil"
	"log"
	"os"
)

func loadField(path string, data interface{}) error {
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

func saveToOrderedDisk(e Entity, dataDir string, guid GUID) error {
	log.Printf("saveToOrderedDisk guid %v", guid)
	// e.writeOrdered(dataDir, guid)
	typeName := e.typeName()
	i := interface{}(e)
	entity, ok := i.(ParquetEntity)
	if ok {
		onDisk, err := hasOnDisk(dataDir, guid)
		if err != nil {
			return err
		}
		if onDisk {
			log.Printf("guid %v is already on disk", guid)
			return nil
		}
		const numGoRoutines int64 = 4
		dirName := fmt.Sprintf("%v/%v", dataDir, guid)
		_ = os.Mkdir(dirName, 0775)
		fname := fmt.Sprintf("%v/data.parquet", dirName)
		typeFName := fmt.Sprintf("%v/type_name", dirName)
		successFile := fmt.Sprintf("%v/_SUCCESS", dirName)
		fw, err := local.NewLocalFileWriter(fname)
		defer fw.Close()
		if err != nil {
			return fmt.Errorf("Failed to create file: %v", err)
		}
		pw, err := writer.NewParquetWriter(fw, entity.orderedRow(), numGoRoutines)
		if err != nil {
			return fmt.Errorf("Failed to create parquet writer: %v", err)
		}
		rows := entity.toOrderedRows()
		for _, row := range rows {
			if err := pw.Write(row); err != nil {
				return fmt.Errorf("Failed to write parquet file: %v", err)
			}
		}
		if err = pw.WriteStop(); err != nil {
			return fmt.Errorf("Parquet WriteStop error: %v", err)
		}
		typeFile, err := os.Create(typeFName)
		tfw := bufio.NewWriter(typeFile)
		if _, err := tfw.WriteString(string(typeName)); err != nil {
			return fmt.Errorf("Failed to create type file: %v", err)
		}
		tfw.Flush()
		err = ioutil.WriteFile(successFile, nil, 0775)
		if err != nil {
			return fmt.Errorf("Failed to write success file: %v", err)
		}
		return nil

	} else {
		log.Println(e)
		return nil
	}
}

func loadFromOrderedDisk(dataDir string, guid GUID) (Entity, error) {
	log.Printf("loadFromOrderedDisk: %v", guid)
	dirName := fmt.Sprintf("%v/%v", dataDir, guid)
	typeFName := fmt.Sprintf("%v/type_name", dirName)
	typeData, err := ioutil.ReadFile(typeFName)
	if err != nil {
		return nil, fmt.Errorf("Failed to read type file: %v", err)
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
			return nil, fmt.Errorf("Path is not present : %v", guid)
		}
		fr, err := local.NewLocalFileReader(fname)
		defer fr.Close()
		if err != nil {
			return nil, fmt.Errorf("Failed to open file: %v", err)
		}
		pr, err := reader.NewParquetReader(fr, e.orderedRow(), numGoRoutines)
		if err != nil {
			return nil, fmt.Errorf("Failed to create parquet reader: %v", err)
		}
		numRows := int(pr.GetNumRows())
		e.readFromOrdered(pr, numRows)
		pr.ReadStop()
	}
	return e, nil
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

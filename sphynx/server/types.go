// Types used by Sphynx.
package main

import (
	"encoding/json"
	"fmt"
	"sync"
)

type Server struct {
	sync.Mutex
	entities         map[GUID]Entity
	dataDir          string
	unorderedDataDir string
}
type GUID string
type OperationDescription struct {
	Class string
	Data  map[string]interface{}
}
type OperationInstance struct {
	GUID      GUID
	Inputs    map[string]GUID
	Outputs   map[string]GUID
	Operation OperationDescription
}

func (server *Server) get(guid GUID) (Entity, bool) {
	server.Lock()
	defer server.Unlock()
	entity, exists := server.entities[guid]
	return entity, exists
}

type EdgeBundle struct {
	Src         []int
	Dst         []int
	EdgeMapping []int64
}

func (es *EdgeBundle) Make(size int, maxSize int) {
	es.Src = make([]int, size, maxSize)
	es.Dst = make([]int, size, maxSize)
	es.EdgeMapping = make([]int64, size, maxSize)
}

type VertexSet struct {
	sync.Mutex
	MappingToUnordered []int64
	MappingToOrdered   map[int64]int
}

func (vs *VertexSet) GetMappingToOrdered() map[int64]int {
	vs.Lock()
	defer vs.Unlock()
	if vs.MappingToOrdered == nil {
		vs.MappingToOrdered = make(map[int64]int)
		for i, j := range vs.MappingToUnordered {
			vs.MappingToOrdered[j] = i
		}
	}
	return vs.MappingToOrdered
}

// A scalar is stored as its JSON encoding. If you need the real value, unmarshal it for yourself.
type Scalar []byte

func ScalarFrom(value interface{}) (Scalar, error) {
	jsonEncoding, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("Error while marshaling scalar: %v", err)
	}
	return Scalar(jsonEncoding), nil
}
func (scalar *Scalar) LoadTo(dst interface{}) error {
	if err := json.Unmarshal([]byte(*scalar), dst); err != nil {
		return fmt.Errorf("Error while unmarshaling scalar: %v", err)
	}
	return nil
}

type DoubleAttribute struct {
	Values  []float64
	Defined []bool
}

type LongAttribute struct {
	Values  []int64
	Defined []bool
}

type StringAttribute struct {
	Values  []string
	Defined []bool
}

type DoubleTuple2AttributeValue struct {
	X float64 `parquet:"name=x, type=DOUBLE"`
	Y float64 `parquet:"name=y, type=DOUBLE"`
}
type DoubleTuple2Attribute struct {
	Values  []DoubleTuple2AttributeValue
	Defined []bool
}

type DoubleVectorAttributeValue []float64
type DoubleVectorAttribute struct {
	Values  []DoubleVectorAttributeValue
	Defined []bool
}

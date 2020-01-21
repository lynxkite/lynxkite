// Types used by Sphynx.
package main

import (
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
	Data  interface{}
}
type OperationInstance struct {
	GUID      GUID
	Inputs    map[string]GUID
	Outputs   map[string]GUID
	Operation OperationDescription
}

type EntityField struct {
	fieldName string
	data      interface{}
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
type VertexSet struct {
	MappingToUnordered []int64
	MappingToOrdered   map[int64]int
}

func (vs *VertexSet) GetMappingToOrdered() map[int64]int {
	if vs.MappingToOrdered == nil {
		vs.MappingToOrdered = make(map[int64]int)
		for i, j := range vs.MappingToUnordered {
			vs.MappingToOrdered[j] = i
		}
	}
	return vs.MappingToOrdered
}

type Scalar struct {
	Value interface{}
}

type DoubleAttribute struct {
	Values  []float64
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

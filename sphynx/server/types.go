// Types used by Sphynx.
package main

import (
	"sync"
)

type Server struct {
	sync.Mutex
	entities         map[GUID]EntityPtr
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

type OperationOutput struct {
	outputs map[GUID]EntityPtr
}

type EntityPtr interface {
}

func (server *Server) get(guid GUID) (EntityPtr, bool) {
	server.Lock()
	defer server.Unlock()
	entity, exists := server.entities[guid]
	return entity, exists
}

type EdgeBundle struct {
	Src         []int64
	Dst         []int64
	EdgeMapping []int64
}
type VertexSet struct {
	Mapping []int64
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
type DoubleTuple2Attribute struct {
	Values1 []float64
	Values2 []float64
	Defined []bool
}

const (
	EdgeBundleCode            byte = iota
	VertexSetCode             byte = iota
	ScalarCode                byte = iota
	DoubleAttributeCode       byte = iota
	StringAttributeCode       byte = iota
	DoubleTuple2AttributeCode byte = iota
)

type Vertex struct {
	Id int64 `parquet:"name=id, type=INT64"`
}
type Edge struct {
	Id  int64 `parquet:"name=id, type=INT64"`
	Src int64 `parquet:"name=src, type=INT64"`
	Dst int64 `parquet:"name=dst, type=INT64"`
}
type SingleStringAttribute struct {
	Id    int64  `parquet:"name=id, type=INT64"`
	Value string `parquet:"name=value, type=UTF8"`
}
type SingleDoubleAttribute struct {
	Id    int64   `parquet:"name=id, type=INT64"`
	Value float64 `parquet:"name=value, type=DOUBLE"`
}
type SingleDoubleTuple2Attribute struct {
	Id     int64   `parquet:"name=id, type=INT64"`
	Value1 float64 `parquet:"name=value1, type=DOUBLE"`
	Value2 float64 `parquet:"name=value2, type=DOUBLE"`
}

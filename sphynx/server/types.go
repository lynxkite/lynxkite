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

type Entity interface {
	typeName() string      // This will help deserializing a serialized entity
	fields() []EntityField // Which fields should be serialized
}

func (e *Scalar) typeName() string {
	return "Scalar"
}
func (e *VertexSet) typeName() string {
	return "VertexSet"
}
func (e *EdgeBundle) typeName() string {
	return "EdgeBundle"
}
func (e *DoubleAttribute) typeName() string {
	return "DoubleAttribute"
}
func (e *StringAttribute) typeName() string {
	return "StringAttribute"
}
func (e *DoubleTuple2Attribute) typeName() string {
	return "DoubleTuple2Attribute"
}

func (e *Scalar) fields() []EntityField {
	return []EntityField{
		EntityField{fieldName: "Value", data: &e.Value},
	}
}
func (e *VertexSet) fields() []EntityField {
	return []EntityField{
		EntityField{fieldName: "MappingToUnordered", data: &e.MappingToUnordered},
		// MappingToOrdered is not here on purpose. This is used for writing out
		// data to Ordered Sphynx Disk. MappingToOrdered can be generated from MappingToUnordered
		// on demand.
	}
}
func (e *EdgeBundle) fields() []EntityField {
	return []EntityField{
		EntityField{fieldName: "Src", data: &e.Src},
		EntityField{fieldName: "Dst", data: &e.Dst},
		EntityField{fieldName: "EdgeMapping", data: &e.EdgeMapping},
	}
}
func (e *DoubleAttribute) fields() []EntityField {
	return []EntityField{
		EntityField{fieldName: "Values", data: &e.Values},
		EntityField{fieldName: "Defined", data: &e.Defined},
	}
}

func (e *StringAttribute) fields() []EntityField {
	return []EntityField{
		EntityField{fieldName: "Values", data: &e.Values},
		EntityField{fieldName: "Defined", data: &e.Defined},
	}
}
func (e *DoubleTuple2Attribute) fields() []EntityField {
	return []EntityField{
		EntityField{fieldName: "Values", data: &e.Values},
		EntityField{fieldName: "Defined", data: &e.Defined},
	}
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
	Id    int64                      `parquet:"name=id, type=INT64"`
	Value DoubleTuple2AttributeValue `parquet:"name=value"`
}

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
	name() string
	fields() []EntityField
}

type AttrPair struct {
	original AttrPtr
	created  AttrPtr
	copier   func(srcIdx int64, dstIdx int64)
}

type AttrPtr interface {
	twins() AttrPair
	entity() Entity
}

func (e *DoubleAttribute) entity() Entity {
	return e
}
func (e *StringAttribute) entity() Entity {
	return e
}
func (e *DoubleTuple2Attribute) entity() Entity {
	return e
}
func (e *DoubleAttribute) twins() AttrPair {
	v := make([]float64, len(e.Values))
	d := make([]bool, len(e.Defined))
	return AttrPair{
		original: e,
		created:  &DoubleAttribute{Values: v, Defined: d},
		copier: func(srcIdx int64, dstIdx int64) {
			d[dstIdx] = true
			v[dstIdx] = e.Values[srcIdx]
		},
	}
}

func (e *DoubleTuple2Attribute) twins() AttrPair {
	v1 := make([]float64, len(e.Values1))
	v2 := make([]float64, len(e.Values2))
	d := make([]bool, len(e.Defined))
	return AttrPair{
		original: e,
		created:  &DoubleTuple2Attribute{Values1: v1, Values2: v2, Defined: d},
		copier: func(srcIdx int64, dstIdx int64) {
			d[dstIdx] = true
			v1[dstIdx] = e.Values1[srcIdx]
			v2[dstIdx] = e.Values2[srcIdx]
		},
	}
}

func (e *StringAttribute) twins() AttrPair {
	v := make([]string, len(e.Values))
	d := make([]bool, len(e.Defined))
	return AttrPair{
		original: e,
		created:  &StringAttribute{Values: v, Defined: d},
		copier: func(srcIdx int64, dstIdx int64) {
			d[dstIdx] = true
			v[dstIdx] = e.Values[srcIdx]
		},
	}
}

func (e *Scalar) name() string {
	return "Scalar"
}
func (e *VertexSet) name() string {
	return "VertexSet"
}
func (e *EdgeBundle) name() string {
	return "EdgeBundle"
}
func (e *DoubleAttribute) name() string {
	return "DoubleAttribute"
}
func (e *StringAttribute) name() string {
	return "StringAttribute"
}
func (e *DoubleTuple2Attribute) name() string {
	return "DoubleTuple2Attribute"
}

func (e *Scalar) fields() []EntityField {
	return []EntityField{
		EntityField{fieldName: "Value", data: &e.Value},
	}
}
func (e *VertexSet) fields() []EntityField {
	return []EntityField{
		EntityField{fieldName: "Mapping", data: &e.Mapping},
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
		EntityField{fieldName: "Values1", data: &e.Values1},
		EntityField{fieldName: "Values2", data: &e.Values2},
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

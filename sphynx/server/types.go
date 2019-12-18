// Types used by Sphynx.
package main

import "sync"

type Server struct {
	entities         EntityMap
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
	vertexSets             map[GUID]*VertexSet
	edgeBundles            map[GUID]*EdgeBundle
	stringAttributes       map[GUID]*StringAttribute
	doubleAttributes       map[GUID]*DoubleAttribute
	doubleTuple2Attributes map[GUID]*DoubleTuple2Attribute
	scalars                map[GUID]Scalar
}

type EntityMap struct {
	sync.Mutex
	vertexSets             map[GUID]*VertexSet
	edgeBundles            map[GUID]*EdgeBundle
	stringAttributes       map[GUID]*StringAttribute
	doubleAttributes       map[GUID]*DoubleAttribute
	doubleTuple2Attributes map[GUID]*DoubleTuple2Attribute
	scalars                map[GUID]Scalar
}

func (em *EntityMap) get(guid GUID) interface{} {
	em.Lock()
	defer em.Unlock()
	var res interface{}
	if e, ok := em.vertexSets[guid]; ok {
		res = e
	} else if e, ok := em.edgeBundles[guid]; ok {
		res = e
	} else if e, ok := em.scalars[guid]; ok {
		res = e
	} else if e, ok := em.stringAttributes[guid]; ok {
		res = e
	} else if e, ok := em.doubleAttributes[guid]; ok {
		res = e
	} else if e, ok := em.doubleTuple2Attributes[guid]; ok {
		res = e
	}
	return res
}

type EdgeBundle struct {
	Src         []int64
	Dst         []int64
	EdgeMapping []int64
	VertexSet   GUID
}
type VertexSet struct {
	Mapping []int64
}
type Scalar struct {
	Value interface{}
}

type DoubleAttribute struct {
	Values        []float64
	Defined       []bool
	VertexSetGuid GUID
}
type StringAttribute struct {
	Values        []string
	Defined       []bool
	VertexSetGuid GUID
}
type DoubleTuple2Attribute struct {
	Values1       []float64
	Values2       []float64
	Defined       []bool
	VertexSetGuid GUID
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

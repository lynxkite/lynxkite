// Types used by Sphynx.
package main

import "sync"

type Server struct {
	sync.Mutex
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

type EntityMap struct {
	vertexSets             map[GUID]VertexSet
	edgeBundles            map[GUID]EdgeBundle
	scalars                map[GUID]Scalar
	stringAttributes       map[GUID]StringAttribute
	doubleAttributes       map[GUID]DoubleAttribute
	doubleTuple2Attributes map[GUID]DoubleTuple2Attribute
}

func (em *EntityMap) get(guid GUID) interface{} {
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
	src         []int64
	dst         []int64
	edgeMapping []int64
	vertexSet   *VertexSet
}
type VertexSet struct {
	mapping []int64
}
type Scalar interface{}
type DoubleAttribute struct {
	values    []float64
	defined   []bool
	vertexSet *VertexSet
}
type StringAttribute struct {
	values    []string
	defined   []bool
	vertexSet *VertexSet
}
type DoubleTuple2Attribute struct {
	values1   []float64
	values2   []float64
	defined   []bool
	vertexSet *VertexSet
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

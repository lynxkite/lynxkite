// Types used by Sphynx.
package main

import "sync"

type Server struct {
	sync.Mutex
	entities         map[GUID]interface{}
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
type EdgeBundle struct {
	src           []int64
	dst           []int64
	vertexMapping []int64
	edgeMapping   []int64
}
type VertexSet struct {
	vertexMapping []int64
}
type DoubleAttribute struct {
	values        []float64
	defined       []bool
	vertexMapping []int64
}
type StringAttribute struct {
	values        []string
	defined       []bool
	vertexMapping []int64
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

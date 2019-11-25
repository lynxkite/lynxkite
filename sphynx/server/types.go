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

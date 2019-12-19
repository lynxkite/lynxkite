// Types used by Sphynx.
package main

import (
	"sync"
)

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
	entities map[GUID]Entity
}

func (o *OperationOutput) addVertexSet(guid GUID, e *VertexSet) {
	o.entities[guid] = e
}
func (o *OperationOutput) addEdgeBundle(guid GUID, e *EdgeBundle) {
	o.entities[guid] = e
}
func (o *OperationOutput) addStringAttribute(guid GUID, e *StringAttribute) {
	o.entities[guid] = e
}
func (o *OperationOutput) addDoubleAttribute(guid GUID, e *DoubleAttribute) {
	o.entities[guid] = e
}
func (o *OperationOutput) addDoubleTuple2Attribute(guid GUID, e *DoubleTuple2Attribute) {
	o.entities[guid] = e
}
func (o *OperationOutput) addScalar(guid GUID, e *Scalar) {
	o.entities[guid] = e
}

type EntityMap struct {
	sync.Mutex
	vertexSets             map[GUID]*VertexSet
	edgeBundles            map[GUID]*EdgeBundle
	stringAttributes       map[GUID]*StringAttribute
	doubleAttributes       map[GUID]*DoubleAttribute
	doubleTuple2Attributes map[GUID]*DoubleTuple2Attribute
	scalars                map[GUID]*Scalar
}

type Entity interface {
	addToEntityMap(entityMap *EntityMap, guid GUID)
}

func (e *VertexSet) addToEntityMap(entityMap *EntityMap, guid GUID) {
	entityMap.Lock()
	entityMap.vertexSets[guid] = e
	entityMap.Unlock()
}
func (e *EdgeBundle) addToEntityMap(entityMap *EntityMap, guid GUID) {
	entityMap.Lock()
	entityMap.edgeBundles[guid] = e
	entityMap.Unlock()
}
func (e *StringAttribute) addToEntityMap(entityMap *EntityMap, guid GUID) {
	entityMap.Lock()
	entityMap.stringAttributes[guid] = e
	entityMap.Unlock()
}
func (e *DoubleAttribute) addToEntityMap(entityMap *EntityMap, guid GUID) {
	entityMap.Lock()
	entityMap.doubleAttributes[guid] = e
	entityMap.Unlock()
}
func (e *DoubleTuple2Attribute) addToEntityMap(entityMap *EntityMap, guid GUID) {
	entityMap.Lock()
	entityMap.doubleTuple2Attributes[guid] = e
	entityMap.Unlock()
}
func (e *Scalar) addToEntityMap(entityMap *EntityMap, guid GUID) {
	entityMap.Lock()
	entityMap.scalars[guid] = e
	entityMap.Unlock()
}

func (em *EntityMap) get(guid GUID) (Entity, bool) {
	em.Lock()
	defer em.Unlock()
	if e, ok := em.vertexSets[guid]; ok {
		return e, true
	} else if e, ok := em.edgeBundles[guid]; ok {
		return e, true
	} else if e, ok := em.scalars[guid]; ok {
		return e, true
	} else if e, ok := em.stringAttributes[guid]; ok {
		return e, true
	} else if e, ok := em.doubleAttributes[guid]; ok {
		return e, true
	} else if e, ok := em.doubleTuple2Attributes[guid]; ok {
		return e, true
	}
	return nil, false
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

var EdgeBundleCode byte = 0
var VertexSetCode byte = 1
var ScalarCode byte = 2
var DoubleAttributeCode byte = 3
var StringAttributeCode byte = 4
var DoubleTuple2AttributeCode byte = 5

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

// Implementations of Sphynx operations.

package main

import (
	"log"
)

type EntityAccess interface {
	add(name string, entity EntityPtr)
	getVertexSet(name string) (*VertexSet, error)
	getEdgeBundle(name string) (*EdgeBundle, error)
	getScalar(name string) (*Scalar, error)
	getDoubleAttribute(name string) (*DoubleAttribute, error)
	getStringAttribute(name string) (*StringAttribute, error)
	getDoubleTuple2Attribute(name string) (*DoubleTuple2Attribute, error)
}

type EntityAccessor struct {
	outputs map[GUID]EntityPtr
	opInst  *OperationInstance
	server  *Server
}

func (ea *EntityAccessor) add(name string, entity EntityPtr) {
	guid := ea.opInst.Outputs[name]
	ea.outputs[guid] = entity
}
func (ea *EntityAccessor) getVertexSet(name string) (*VertexSet, error) {
	return nil, nil
}
func (ea *EntityAccessor) getEdgeBundle(name string) (*EdgeBundle, error) {
	return nil, nil
}
func (ea *EntityAccessor) getScalar(name string) (*Scalar, error) {
	return nil, nil
}
func (ea *EntityAccessor) getDoubleAttribute(name string) (*DoubleAttribute, error) {
	return nil, nil
}
func (ea *EntityAccessor) getStringAttribute(name string) (*StringAttribute, error) {
	return nil, nil
}
func (ea *EntityAccessor) getDoubleTuple2Attribute(name string) (*DoubleTuple2Attribute, error) {
	return nil, nil
}

type Operation struct {
	execute func(ea *EntityAccessor) error
}

var operations = map[string]Operation{
	"ExampleGraph": exampleGraph,
}

var exampleGraph = Operation{
	execute: func(ea *EntityAccessor) error {
		log.Printf("exampleGraph execute called")
		vertexSet := VertexSet{Mapping: []int64{0, 1, 2, 3}}
		ea.add("vertices", &vertexSet)
		eb := &EdgeBundle{
			Src:         []int64{0, 1, 2, 2},
			Dst:         []int64{1, 0, 0, 1},
			EdgeMapping: []int64{0, 1, 2, 3},
		}
		ea.add("edges", eb)
		name := &StringAttribute{
			Values:  []string{"Adam", "Eve", "Bob", "Isolated Joe"},
			Defined: []bool{true, true, true, true},
		}
		ea.add("name", name)
		age := &DoubleAttribute{
			Values:  []float64{20.3, 18.2, 50.3, 2.0},
			Defined: []bool{true, true, true, true},
		}
		ea.add("age", age)
		gender := &StringAttribute{
			Values:  []string{"Male", "Female", "Male", "Male"},
			Defined: []bool{true, true, true, true},
		}
		ea.add("gender", gender)
		income := &DoubleAttribute{
			Values:  []float64{1000, 0, 0, 2000},
			Defined: []bool{true, false, false, true},
		}
		ea.add("income", income)
		location := &DoubleTuple2Attribute{
			Values1: []float64{40.71448, 47.5269674, 1.352083, -33.8674869},
			Values2: []float64{-74.00598, 19.0323968, 103.819836, 151.2069902},
			Defined: []bool{true, true, true, true},
		}
		ea.add("location", location)
		comment := &StringAttribute{
			Values: []string{"Adam loves Eve", "Eve loves Adam",
				"Bob envies Adam", "Bob loves Eve"},
			Defined: []bool{true, true, true, true},
		}
		ea.add("comment", comment)
		weight := &DoubleAttribute{
			Values:  []float64{1, 2, 3, 4},
			Defined: []bool{true, true, true, true},
		}
		ea.add("weight", weight)
		greeting := Scalar{Value: "Hello world! 😀 "}
		ea.add("greeting", &greeting)
		return nil
	},
}

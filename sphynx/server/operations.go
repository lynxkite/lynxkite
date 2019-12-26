// Implementations of Sphynx operations.

package main

import (
	"fmt"
	"log"
)

type EntityAccess interface {
	add(name string, entity EntityPtr) error
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

func (ea *EntityAccessor) add(name string, entity EntityPtr) error {
	guid, exists := ea.opInst.Outputs[name]
	if !exists {
		return fmt.Errorf("Could not find '%v' among output names", name)
	}
	ea.outputs[guid] = entity
	return nil
}

func getEntity(name string, server *Server, opInst *OperationInstance) (EntityPtr, error) {
	guid, exists := opInst.Inputs[name]
	if !exists {
		return nil, fmt.Errorf("Could not find '%v' among input names", name)
	}
	server.Lock()
	entity, exists := server.entities[guid]
	server.Unlock()
	if !exists {
		return nil, fmt.Errorf("Could not find %v ('%v') among entities", guid, name)
	}
	return entity, nil
}

func (ea *EntityAccessor) getVertexSet(name string) (*VertexSet, error) {
	entity, err := getEntity(name, ea.server, ea.opInst)
	if err != nil {
		return nil, err
	}
	switch e := entity.(type) {
	case *VertexSet:
		return e, nil
	default:
		return nil, fmt.Errorf("Input %v is a %T, not a VertexSet", name, e)
	}
}

func (ea *EntityAccessor) getEdgeBundle(name string) (*EdgeBundle, error) {
	entity, err := getEntity(name, ea.server, ea.opInst)
	if err != nil {
		return nil, err
	}
	switch e := entity.(type) {
	case *EdgeBundle:
		return e, nil
	default:
		return nil, fmt.Errorf("Input %v is a %T, not an EdgeBundle", name, e)
	}
}
func (ea *EntityAccessor) getScalar(name string) (*Scalar, error) {
	entity, err := getEntity(name, ea.server, ea.opInst)
	if err != nil {
		return nil, err
	}
	switch e := entity.(type) {
	case *Scalar:
		return e, nil
	default:
		return nil, fmt.Errorf("Input %v is a %T, not a Scalar", name, e)
	}
}

func (ea *EntityAccessor) getDoubleAttribute(name string) (*DoubleAttribute, error) {
	entity, err := getEntity(name, ea.server, ea.opInst)
	if err != nil {
		return nil, err
	}
	switch e := entity.(type) {
	case *DoubleAttribute:
		return e, nil
	default:
		return nil, fmt.Errorf("Input %v is a %T, not a DoubleAttribute", name, e)
	}
}

func (ea *EntityAccessor) getStringAttribute(name string) (*StringAttribute, error) {
	entity, err := getEntity(name, ea.server, ea.opInst)
	if err != nil {
		return nil, err
	}
	switch e := entity.(type) {
	case *StringAttribute:
		return e, nil
	default:
		return nil, fmt.Errorf("Input %v is a %T, not a StringAttribute", name, e)
	}
}

func (ea *EntityAccessor) getDoubleTuple2Attribute(name string) (*DoubleTuple2Attribute, error) {
	entity, err := getEntity(name, ea.server, ea.opInst)
	if err != nil {
		return nil, err
	}
	switch e := entity.(type) {
	case *DoubleTuple2Attribute:
		return e, nil
	default:
		return nil, fmt.Errorf("Input %v is a %T, not a DoubleTuple2Attribute", name, e)
	}

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

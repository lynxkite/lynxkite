// Implementations of Sphynx operations.

package main

import (
	"fmt"
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

var operationRepository map[string]Operation = nil

func registerOperation(opName string, operation Operation) bool {
	if operationRepository == nil {
		operationRepository = make(map[string]Operation)
	}
	operationRepository[opName] = operation
	return true
}

// Implementations of Sphynx operations.

package main

import (
	"fmt"
)

type EntityAccess interface {
	add(name string, entity EntityPtr) error
	getVertexSet(name string) *VertexSet
	getEdgeBundle(name string) *EdgeBundle
	getScalar(name string) *Scalar
	getDoubleAttribute(name string) *DoubleAttribute
	getStringAttribute(name string) *StringAttribute
	getDoubleTuple2Attribute(name string) *DoubleTuple2Attribute
	getAttr(name string) AttrPtr
	getError() error
}

type EntityAccessor struct {
	outputs map[GUID]EntityPtr
	opInst  *OperationInstance
	server  *Server
	err     error
}

func (ea *EntityAccessor) getError() error {
	return ea.err
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
	entity, exists := server.get(guid)
	if !exists {
		return nil, fmt.Errorf("Could not find %v ('%v') among entities", guid, name)
	}
	return entity, nil
}

func (ea *EntityAccessor) getAttr(name string) AttrPtr {
	if ea.err != nil {
		return nil
	}
	entity, err := getEntity(name, ea.server, ea.opInst)
	if err != nil {
		ea.err = err
		return nil
	}
	switch e := entity.(type) {
	case *DoubleTuple2Attribute:
		return e
	case *DoubleAttribute:
		return e
	case *StringAttribute:
		return e
	default:
		ea.err = fmt.Errorf("Input %v is a %T, not an Attribute", name, e)
		return nil
	}
}

func (ea *EntityAccessor) getVertexSet(name string) *VertexSet {
	if ea.err != nil {
		return nil
	}
	entity, err := getEntity(name, ea.server, ea.opInst)
	if err != nil {
		ea.err = err
		return nil
	}
	switch e := entity.(type) {
	case *VertexSet:
		return e
	default:
		ea.err = fmt.Errorf("Input %v is a %T, not a VertexSet", name, e)
		return nil
	}
}

func (ea *EntityAccessor) getEdgeBundle(name string) *EdgeBundle {
	if ea.err != nil {
		return nil
	}
	entity, err := getEntity(name, ea.server, ea.opInst)
	if err != nil {
		ea.err = err
		return nil
	}
	switch e := entity.(type) {
	case *EdgeBundle:
		return e
	default:
		ea.err = fmt.Errorf("Input %v is a %T, not an EdgeBundle", name, e)
		return nil
	}
}
func (ea *EntityAccessor) getScalar(name string) *Scalar {
	if ea.err != nil {
		return nil
	}
	entity, err := getEntity(name, ea.server, ea.opInst)
	if err != nil {
		ea.err = err
		return nil
	}
	switch e := entity.(type) {
	case *Scalar:
		return e
	default:
		ea.err = fmt.Errorf("Input %v is a %T, not a Scalar", name, e)
		return nil
	}
}

func (ea *EntityAccessor) getDoubleAttribute(name string) *DoubleAttribute {
	if ea.err != nil {
		return nil
	}
	entity, err := getEntity(name, ea.server, ea.opInst)
	if err != nil {
		ea.err = err
		return nil
	}
	switch e := entity.(type) {
	case *DoubleAttribute:
		return e
	default:
		ea.err = fmt.Errorf("Input %v is a %T, not a DoubleAttribute", name, e)
		return nil
	}
}

func (ea *EntityAccessor) getStringAttribute(name string) *StringAttribute {
	if ea.err != nil {
		return nil
	}
	entity, err := getEntity(name, ea.server, ea.opInst)
	if err != nil {
		ea.err = err
		return nil
	}
	switch e := entity.(type) {
	case *StringAttribute:
		return e
	default:
		ea.err = fmt.Errorf("Input %v is a %T, not a StringAttribute", name, e)
		return nil
	}
}

func (ea *EntityAccessor) getDoubleTuple2Attribute(name string) *DoubleTuple2Attribute {
	if ea.err != nil {
		return nil
	}
	entity, err := getEntity(name, ea.server, ea.opInst)
	if err != nil {
		ea.err = err
		return nil
	}
	switch e := entity.(type) {
	case *DoubleTuple2Attribute:
		return e
	default:
		ea.err = fmt.Errorf("Input %v is a %T, not a DoubleTuple2Attribute", name, e)
		return nil
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

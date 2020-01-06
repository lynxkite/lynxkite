// Implementations of Sphynx operations.

package main

import (
	"fmt"
)

type EntityAccessor struct {
	inputs  map[string]Entity
	outputs map[GUID]Entity
	opInst  *OperationInstance
	server  *Server
}

func collectInputs(server *Server, opInst *OperationInstance) (map[string]Entity, error) {
	inputs := make(map[string]Entity, len(opInst.Inputs))
	for name, guid := range opInst.Inputs {
		entity, exists := server.get(guid)
		if !exists {
			return nil, fmt.Errorf("Guid %v corresponding to name: '%v' was not found in cache", guid, name)
		}
		inputs[name] = entity
	}
	return inputs, nil
}

func (ea *EntityAccessor) output(name string, entity Entity) error {
	guid, exists := ea.opInst.Outputs[name]
	if !exists {
		return fmt.Errorf("Could not find '%v' among output names", name)
	}
	ea.outputs[guid] = entity
	return nil
}

func (ea *EntityAccessor) getVertexSet(name string) *VertexSet {
	return ea.inputs[name].(*VertexSet)
}

func (ea *EntityAccessor) getEdgeBundle(name string) *EdgeBundle {
	return ea.inputs[name].(*EdgeBundle)
}
func (ea *EntityAccessor) getScalar(name string) *Scalar {
	return ea.inputs[name].(*Scalar)
}

func (ea *EntityAccessor) getDoubleAttribute(name string) *DoubleAttribute {
	return ea.inputs[name].(*DoubleAttribute)
}

func (ea *EntityAccessor) getStringAttribute(name string) *StringAttribute {
	return ea.inputs[name].(*StringAttribute)
}

func (ea *EntityAccessor) getDoubleTuple2Attribute(name string) *DoubleTuple2Attribute {
	return ea.inputs[name].(*DoubleTuple2Attribute)
}

type Operation struct {
	execute func(ea *EntityAccessor) error
}

var operationRepository = map[string]Operation{}

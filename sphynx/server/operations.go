// Implementations of Sphynx operations.

package main

import (
	"fmt"
	"reflect"
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

func (ea *EntityAccessor) GetStringParam(name string) string {
	v := reflect.ValueOf(ea.opInst.Operation.Data)
	if v.Kind() != reflect.String {
		return ""
	} else {
		return v.FieldByName(name).String()
	}
}

func (ea *EntityAccessor) GetFloatParam(name string) float64 {
	v := reflect.ValueOf(ea.opInst.Operation.Data)
	if v.Kind() != reflect.Float64 {
		return 0
	} else {
		return v.FieldByName(name).Float()
	}
}

func (ea *EntityAccessor) WriteToDisk(name string) (string, error) {
	err := saveToOrderedDisk(ea.inputs[name], ea.server.dataDir, ea.opInst.Inputs[name])
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%v/%v/", ea.server.dataDir, ea.opInst.Inputs[name]), nil
}

func (ea *EntityAccessor) NameOnDisk(name string) string {
	return fmt.Sprintf("%v/%v", ea.server.dataDir, ea.opInst.Outputs[name])
}

func (ea *EntityAccessor) OutputOnDisk(name string) error {
	entity, err := loadFromOrderedDisk(ea.server.dataDir, ea.opInst.Outputs[name])
	if err != nil {
		return err
	}
	ea.output(name, entity)
	return nil
}

type Operation struct {
	execute func(ea *EntityAccessor) error
}

var operationRepository = map[string]Operation{}

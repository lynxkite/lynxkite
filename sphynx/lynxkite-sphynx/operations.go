// Implementations of Sphynx operations.

package main

import (
	"encoding/json"
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
		entity, exists := server.entityCache.Get(guid)
		if !exists {
			return nil, NotInCacheError("input", guid)
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

func (ea *EntityAccessor) outputScalar(name string, value interface{}) error {
	s, err := ScalarFrom(value)
	if err != nil {
		return err
	}
	return ea.output(name, &s)
}

func (ea *EntityAccessor) getOutput(name string) Entity {
	guid := ea.opInst.Outputs[name]
	return ea.outputs[guid]
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

// May return nil.
func (ea *EntityAccessor) getDoubleAttributeOpt(name string) *DoubleAttribute {
	_, exists := ea.inputs[name]
	if exists {
		return ea.getDoubleAttribute(name)
	} else {
		return nil
	}
}

func (ea *EntityAccessor) getStringAttribute(name string) *StringAttribute {
	return ea.inputs[name].(*StringAttribute)
}

func (ea *EntityAccessor) getLongAttribute(name string) *LongAttribute {
	return ea.inputs[name].(*LongAttribute)
}

func (ea *EntityAccessor) getDoubleVectorAttribute(name string) *DoubleVectorAttribute {
	return ea.inputs[name].(*DoubleVectorAttribute)
}

func (ea *EntityAccessor) getLongVectorAttribute(name string) *LongVectorAttribute {
	return ea.inputs[name].(*LongVectorAttribute)
}

func (ea *EntityAccessor) GetFloatParam(name string) float64 {
	return ea.opInst.Operation.Data[name].(float64)
}

func (ea *EntityAccessor) GetBoolParam(name string) bool {
	return ea.opInst.Operation.Data[name].(bool)
}

func (ea *EntityAccessor) GetStringParam(name string) string {
	return ea.opInst.Operation.Data[name].(string)
}

func (ea *EntityAccessor) GetMapParam(name string) map[string]interface{} {
	return ea.opInst.Operation.Data[name].(map[string]interface{})
}

func (ea *EntityAccessor) GetStringVectorParam(name string) []string {
	interfaceSlice := ea.opInst.Operation.Data[name].([]interface{})
	stringSlice := make([]string, len(interfaceSlice))
	for i, elem := range interfaceSlice {
		stringSlice[i] = elem.(string)
	}
	return stringSlice
}

func (ea *EntityAccessor) GetBoolParamWithDefault(name string, dflt bool) bool {
	field, exists := ea.opInst.Operation.Data[name]
	if exists {
		return field.(bool)
	}
	return dflt
}

// TODO: Replace this with writing Parquet from Python.
func (ea *EntityAccessor) OutputJson(raw []byte) error {
	type JsonEntity struct {
		TypeName string
		Data     json.RawMessage
	}
	var outputs map[string]JsonEntity
	if err := json.Unmarshal(raw, &outputs); err != nil {
		return fmt.Errorf("could not parse %#v: %v", raw, err)
	}
	for name := range ea.opInst.Outputs {
		o, ok := outputs[name]
		if !ok {
			continue
		}
		e, err := createEntity(o.TypeName)
		if err != nil {
			return fmt.Errorf("could not parse %#v: %v", raw, err)
		}
		if err := json.Unmarshal(o.Data, &e); err != nil {
			return fmt.Errorf("could not parse %#v: %v", raw, err)
		}
		ea.output(name, e)
	}
	return nil
}

type Operation struct {
	execute    func(ea *EntityAccessor) error
	canCompute func(operationDescription OperationDescription) bool
}

type DiskOperation struct {
	execute func(dataDir string, op *OperationInstance) error
}

var operationRepository = map[string]Operation{}
var diskOperationRepository = map[string]DiskOperation{}

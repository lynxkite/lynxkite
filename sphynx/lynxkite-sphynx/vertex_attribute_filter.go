// Implements some of the filters in VertexAttributeFilter
package main

import (
	"fmt"
	"strconv"
)

type filterType struct {
	typeName string // E.g., "Double"
	opCode   string // E.g, "EQ"
}

type filterJobDescription struct {
	fType     filterType
	baseValue string // E.g., "16.0", or "Adam"
}

// Parser for the format "bound" uses
func parseBoundDescription(opCode string, filterDesc map[string]interface{}) (filterJobDescription, bool) {
	bound, ok := filterDesc["bound"]
	if !ok {
		return filterJobDescription{}, false
	}
	boundString := fmt.Sprintf("%v", bound)
	typ, ok := filterDesc["type"]
	if !ok {
		return filterJobDescription{}, false
	}
	typIntf, ok := typ.(map[string]interface{})
	if !ok {
		return filterJobDescription{}, false
	}
	typeName, ok := typIntf["typename"]
	if !ok {
		return filterJobDescription{}, false
	}
	typeNameString, ok := typeName.(string)
	if !ok {
		return filterJobDescription{}, false
	}
	return filterJobDescription{
		fType: filterType{
			typeName: typeNameString,
			opCode:   opCode,
		},
		baseValue: boundString,
	}, true
}

// Parser for the format "exact" uses
func parseExactDescription(opCode string, filterDesc map[string]interface{}) (filterJobDescription, bool) {
	exact, ok := filterDesc["exact"]
	if !ok {
		return filterJobDescription{}, false
	}
	inner, ok := exact.(map[string]interface{})
	if !ok {
		return filterJobDescription{}, false
	}
	typeName, ok := inner["class"]
	if !ok {
		return filterJobDescription{}, false
	}
	typeNameString, ok := typeName.(string)
	if !ok {
		return filterJobDescription{}, false
	}

	dataName, ok := inner["data"]
	if !ok {
		return filterJobDescription{}, false
	}
	dataNameString := fmt.Sprintf("%v", dataName)
	if !ok {
		return filterJobDescription{}, false
	}

	return filterJobDescription{
		fType: filterType{
			typeName: typeNameString,
			opCode:   opCode,
		},
		baseValue: dataNameString,
	}, true
}

type doubleFilterFn func(float64) func(float64) bool
type stringFilterFn func(string) func(string) bool
type paramExtractorFun func(string, map[string]interface{}) (filterJobDescription, bool)

var parameterParsers = make(map[string]paramExtractorFun)
var supportedFilters = make(map[filterType]interface{})

func registerDoubleFilter(opCode string, fn doubleFilterFn, parser paramExtractorFun) {
	filterType := filterType{
		typeName: "Double",
		opCode:   opCode,
	}
	parameterParsers[opCode] = parser
	supportedFilters[filterType] = fn
}

func registerStringFilter(opCode string, fn stringFilterFn, parser paramExtractorFun) {
	filterType := filterType{
		typeName: "String",
		opCode:   opCode,
	}
	parameterParsers[opCode] = parser
	supportedFilters[filterType] = fn
}

// The first init() function
func init() {
	registerDoubleFilter("GE", func(base float64) func(float64) bool {
		return func(val float64) bool {
			return base <= val
		}
	}, parseBoundDescription)
	registerDoubleFilter("GT", func(base float64) func(float64) bool {
		return func(val float64) bool {
			return base < val
		}
	}, parseBoundDescription)
	registerDoubleFilter("LE", func(base float64) func(float64) bool {
		return func(val float64) bool {
			return base >= val
		}
	}, parseBoundDescription)
	registerDoubleFilter("LT", func(base float64) func(float64) bool {
		return func(val float64) bool {
			return base > val
		}
	}, parseBoundDescription)
	registerDoubleFilter("EQ", func(base float64) func(float64) bool {
		return func(val float64) bool {
			return base == val
		}
	}, parseExactDescription)

	registerStringFilter("EQ", func(base string) func(string) bool {
		return func(val string) bool {
			return base == val
		}
	}, parseExactDescription)

}

func doVertexAttributeFilter(job filterJobDescription, vs *VertexSet, attr TabularEntity) (fvs *VertexSet,
	identity *EdgeBundle) {

	fvs = &VertexSet{
		MappingToUnordered: make([]int64, 0, len(vs.MappingToUnordered)),
	}
	identity = NewEdgeBundle(0, len(vs.MappingToUnordered))
	switch a := attr.(type) {
	case *DoubleAttribute:
		base, _ := strconv.ParseFloat(job.baseValue, 64)
		filterer := supportedFilters[job.fType].(doubleFilterFn)(base)
		for i := 0; i < len(a.Values); i++ {
			if a.Defined[i] && filterer(a.Values[i]) {
				fvs.MappingToUnordered = append(fvs.MappingToUnordered, vs.MappingToUnordered[i])
				identity.Src = append(identity.Src, SphynxId(len(identity.Src)))
				identity.Dst = append(identity.Dst, SphynxId(i))
				identity.EdgeMapping = append(identity.EdgeMapping, vs.MappingToUnordered[i])
			}
		}
	case *StringAttribute:
		filterer := supportedFilters[job.fType].(stringFilterFn)(job.baseValue)
		for i := 0; i < len(a.Values); i++ {
			if a.Defined[i] && filterer(a.Values[i]) {
				fvs.MappingToUnordered = append(fvs.MappingToUnordered, vs.MappingToUnordered[i])
				identity.Src = append(identity.Src, SphynxId(len(identity.Src)))
				identity.Dst = append(identity.Dst, SphynxId(i))
				identity.EdgeMapping = append(identity.EdgeMapping, vs.MappingToUnordered[i])
			}
		}
	default:
		panic("Not implemented")
	}
	return
}

func extractFilterJob(params map[string]interface{}) (filterJobDescription, bool) {
	filter := params["filter"].(map[string]interface{})
	longOpCode := filter["class"].(string)
	opCode := longOpCode[len("com.lynxanalytics.lynxkite.graph_operations."):]
	parser, ok := parameterParsers[opCode]
	if !ok {
		return filterJobDescription{}, false
	}
	job, ok := parser(opCode, filter["data"].(map[string]interface{}))
	if !ok {
		return filterJobDescription{}, false
	}
	_, exists := supportedFilters[job.fType]
	if !exists {
		return filterJobDescription{}, false
	}
	return job, true
}

// The VertexAttributeFilter API requires us to produce this (as a Scalar)
func getFilteredAttribute(guid GUID, filter map[string]interface{}) map[string]interface{} {
	data := map[string]interface{}{
		"filter":        filter,
		"attributeGUID": guid,
	}
	filteredAttribute := map[string]interface{}{
		"class": "com.lynxanalytics.lynxkite.graph_operations.FilteredAttribute",
		"data":  data,
	}
	return filteredAttribute
}

func init() {
	operationRepository["VertexAttributeFilter"] = Operation{
		execute: func(ea *EntityAccessor) error {
			job, ok := extractFilterJob(ea.opInst.Operation.Data)
			if !ok {
				panic(fmt.Sprintf("Cannot handle job: %v", ea.opInst.Operation.Data))
			}
			attr := ea.inputs["attr"].(TabularEntity)
			vs := ea.getVertexSet("vs")
			fvs, identity := doVertexAttributeFilter(job, vs, attr)
			guid := ea.opInst.Inputs["attr"]
			filter := ea.opInst.Operation.Data["filter"].(map[string]interface{})
			filteredAttribute, err := ScalarFrom(getFilteredAttribute(guid, filter))
			if err != nil {
				return err
			}
			ea.output("fvs", fvs)
			ea.output("identity", identity)
			ea.output("filteredAttribute", &filteredAttribute)
			return nil
		},
		canCompute: func(operationDescription OperationDescription) bool {
			_, can := extractFilterJob(operationDescription.Data)
			return can
		},
	}
}

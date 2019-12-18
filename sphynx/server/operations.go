// Implementations of Sphynx operations.

package main

import "log"

type Operation struct {
	execute func(*Server, OperationInstance) OperationOutput
}

var operations = map[string]Operation{
	"ExampleGraph": exampleGraph,
}

func NewOperationOutput() OperationOutput {
	return OperationOutput{
		vertexSets:             make(map[GUID]*VertexSet),
		edgeBundles:            make(map[GUID]*EdgeBundle),
		scalars:                make(map[GUID]Scalar),
		stringAttributes:       make(map[GUID]*StringAttribute),
		doubleAttributes:       make(map[GUID]*DoubleAttribute),
		doubleTuple2Attributes: make(map[GUID]*DoubleTuple2Attribute),
	}
}

var exampleGraph = Operation{
	execute: func(s *Server, opInst OperationInstance) OperationOutput {
		log.Printf("exampleGraph execute called")
		outputs := NewOperationOutput()
		vertexSet := VertexSet{Mapping: []int64{0, 1, 2, 3}}
		nameToGUID := opInst.Outputs
		vsGUID := nameToGUID["vertices"]
		outputs.vertexSets[vsGUID] = &vertexSet
		eb := &EdgeBundle{
			Src:         []int64{0, 1, 2, 2},
			Dst:         []int64{1, 0, 0, 1},
			VertexSet:   vsGUID,
			EdgeMapping: []int64{0, 1, 2, 3},
		}
		outputs.edgeBundles[nameToGUID["edges"]] = eb
		name := &StringAttribute{
			Values:        []string{"Adam", "Eve", "Bob", "Isolated Joe"},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		outputs.stringAttributes[nameToGUID["name"]] = name
		age := &DoubleAttribute{
			Values:        []float64{20.3, 18.2, 50.3, 2.0},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		outputs.doubleAttributes[nameToGUID["age"]] = age
		gender := &StringAttribute{
			Values:        []string{"Male", "Female", "Male", "Male"},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		outputs.stringAttributes[nameToGUID["gender"]] = gender
		income := &DoubleAttribute{
			Values:        []float64{1000, 0, 0, 2000},
			Defined:       []bool{true, false, false, true},
			VertexSetGuid: vsGUID,
		}
		outputs.doubleAttributes[nameToGUID["income"]] = income
		location := &DoubleTuple2Attribute{
			Values1:       []float64{40.71448, 47.5269674, 1.352083, -33.8674869},
			Values2:       []float64{-74.00598, 19.0323968, 103.819836, 151.2069902},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		outputs.doubleTuple2Attributes[nameToGUID["location"]] = location
		comment := &StringAttribute{
			Values: []string{"Adam loves Eve", "Eve loves Adam",
				"Bob envies Adam", "Bob loves Eve"},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		outputs.stringAttributes[nameToGUID["comment"]] = comment
		weight := &DoubleAttribute{
			Values:        []float64{1, 2, 3, 4},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		outputs.doubleAttributes[nameToGUID["weight"]] = weight
		greeting := Scalar{Value: "Hello world! 😀 "}
		outputs.scalars[nameToGUID["greeting"]] = greeting
		return outputs
	},
}

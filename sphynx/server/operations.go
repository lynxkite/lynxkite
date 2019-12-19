// Implementations of Sphynx operations.

package main

import "log"

type Operation struct {
	execute func(*Server, OperationInstance) OperationOutput
}

var operations = map[string]Operation{
	"ExampleGraph": exampleGraph,
}

var exampleGraph = Operation{
	execute: func(s *Server, opInst OperationInstance) OperationOutput {
		log.Printf("exampleGraph execute called")
		outputs := OperationOutput{entities: make(map[GUID]Entity)}
		vertexSet := VertexSet{Mapping: []int64{0, 1, 2, 3}}
		nameToGUID := opInst.Outputs
		vsGUID := nameToGUID["vertices"]
		outputs.addVertexSet(vsGUID, &vertexSet)
		eb := &EdgeBundle{
			Src:         []int64{0, 1, 2, 2},
			Dst:         []int64{1, 0, 0, 1},
			VertexSet:   vsGUID,
			EdgeMapping: []int64{0, 1, 2, 3},
		}
		outputs.addEdgeBundle(nameToGUID["edges"], eb)
		name := &StringAttribute{
			Values:        []string{"Adam", "Eve", "Bob", "Isolated Joe"},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		outputs.addStringAttribute(nameToGUID["name"], name)
		age := &DoubleAttribute{
			Values:        []float64{20.3, 18.2, 50.3, 2.0},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		outputs.addDoubleAttribute(nameToGUID["age"], age)
		gender := &StringAttribute{
			Values:        []string{"Male", "Female", "Male", "Male"},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		outputs.addStringAttribute(nameToGUID["gender"], gender)
		income := &DoubleAttribute{
			Values:        []float64{1000, 0, 0, 2000},
			Defined:       []bool{true, false, false, true},
			VertexSetGuid: vsGUID,
		}
		outputs.addDoubleAttribute(nameToGUID["income"], income)
		location := &DoubleTuple2Attribute{
			Values1:       []float64{40.71448, 47.5269674, 1.352083, -33.8674869},
			Values2:       []float64{-74.00598, 19.0323968, 103.819836, 151.2069902},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		outputs.addDoubleTuple2Attribute(nameToGUID["location"], location)
		comment := &StringAttribute{
			Values: []string{"Adam loves Eve", "Eve loves Adam",
				"Bob envies Adam", "Bob loves Eve"},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		outputs.addStringAttribute(nameToGUID["comment"], comment)
		weight := &DoubleAttribute{
			Values:        []float64{1, 2, 3, 4},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		outputs.addDoubleAttribute(nameToGUID["weight"], weight)
		greeting := Scalar{Value: "Hello world! 😀 "}
		outputs.addScalar(nameToGUID["greeting"], &greeting)
		return outputs
	},
}

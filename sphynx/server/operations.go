// Implementations of Sphynx operations.

package main

type Operation struct {
	execute func(*Server, OperationInstance)
}

var operations = map[string]Operation{
	"ExampleGraph": exampleGraph,
}

var exampleGraph = Operation{
	execute: func(s *Server, opInst OperationInstance) {
		s.entities.Lock()
		defer s.entities.Unlock()
		vertexSet := VertexSet{mapping: []int64{0, 1, 2, 3}}
		nameToGUID := opInst.Outputs
		s.entities.vertexSets[nameToGUID["vertices"]] = vertexSet
		s.entities.edgeBundles[nameToGUID["edges"]] = EdgeBundle{
			src:         []int64{0, 1, 2, 2},
			dst:         []int64{1, 0, 0, 1},
			vertexSet:   &vertexSet,
			edgeMapping: []int64{0, 1, 2, 3},
		}
		s.entities.stringAttributes[nameToGUID["name"]] = StringAttribute{
			values:    []string{"Adam", "Eve", "Bob", "Isolated Joe"},
			defined:   []bool{true, true, true, true},
			vertexSet: &vertexSet,
		}
		s.entities.doubleAttributes[nameToGUID["age"]] = DoubleAttribute{
			values:    []float64{20.3, 18.2, 50.3, 2.0},
			defined:   []bool{true, true, true, true},
			vertexSet: &vertexSet,
		}
		s.entities.stringAttributes[nameToGUID["gender"]] = StringAttribute{
			values:    []string{"Male", "Female", "Male", "Male"},
			defined:   []bool{true, true, true, true},
			vertexSet: &vertexSet,
		}
		s.entities.doubleAttributes[nameToGUID["income"]] = DoubleAttribute{
			values:    []float64{1000, 0, 0, 2000},
			defined:   []bool{true, false, false, true},
			vertexSet: &vertexSet,
		}
		s.entities.doubleTuple2Attributes[nameToGUID["location"]] = DoubleTuple2Attribute{
			values1:   []float64{40.71448, 47.5269674, 1.352083, -33.8674869},
			values2:   []float64{-74.00598, 19.0323968, 103.819836, 151.2069902},
			defined:   []bool{true, true, true, true},
			vertexSet: &vertexSet,
		}
		s.entities.stringAttributes[nameToGUID["comment"]] = StringAttribute{
			values: []string{"Adam loves Eve", "Eve loves Adam",
				"Bob envies Adam", "Bob loves Eve"},
			defined:   []bool{true, true, true, true},
			vertexSet: &vertexSet,
		}
		s.entities.doubleAttributes[nameToGUID["weight"]] = DoubleAttribute{
			values:    []float64{1, 2, 3, 4},
			defined:   []bool{true, true, true, true},
			vertexSet: &vertexSet,
		}
		s.entities.scalars[nameToGUID["greeting"]] = Scalar("Hello world! 😀 ")
	},
}

// Implementations of Sphynx operations.

package main

type Operation struct {
	execute func(*Server, OperationInstance)
}

var operations = map[string]Operation{
	"ExampleGraph": exampleGraph,
	// "PageRank":     exampleGraph
}

var exampleGraph = Operation{
	execute: func(s *Server, opInst OperationInstance) {
		s.Lock()
		defer s.Unlock()
		vertexMapping := []int64{0, 1, 2, 3}
		s.entities[opInst.Outputs["vertices"]] = VertexSet{vertexMapping}
		s.entities[opInst.Outputs["edges"]] = EdgeBundle{
			src:           []int64{0, 1, 2, 2},
			dst:           []int64{1, 0, 0, 1},
			vertexMapping: vertexMapping,
			edgeMapping:   []int64{0, 1, 2, 3},
		}
		s.entities[opInst.Outputs["age"]] = Attribute{
			attribute: []float64{20.3, 18.2, 50.3, 2.0},
			defined:   []bool{true, true, true, true},
		}
		s.entities[opInst.Outputs["gender"]] = Attribute{
			attribute: []string{"Male", "Female", "Male", "Male"},
			defined:   []bool{true, true, true, true},
		}
		s.entities[opInst.Outputs["income"]] = Attribute{
			attribute: []float64{1000, 0, 0, 2000},
			defined:   []bool{true, false, false, true},
		}
		s.entities[opInst.Outputs["location"]] = Attribute{
			attribute: []struct {
				x float64
				y float64
			}{
				{x: 40.71448, y: -74.00598},      // New York
				{x: 47.5269674, y: 19.0323968},   // Budapest
				{x: 1.352083, y: 103.819836},     // Singapore
				{x: -33.8674869, y: 151.2069902}, // Sydney
			},
			defined: []bool{true, true, true, true},
		}
		s.entities[opInst.Outputs["comment"]] = Attribute{
			attribute: []string{"Adam loves Eve", "Eve loves Adam",
				"Bob envies Adam", "Bob loves Eve"},
			defined: []bool{true, true, true, true},
		}
		s.entities[opInst.Outputs["weight"]] = Attribute{
			attribute: []float64{1, 2, 3, 4},
			defined:   []bool{true, true, true, true},
		}
		s.entities[opInst.Outputs["greeting"]] = "Hello world! 😀 "
	},
}

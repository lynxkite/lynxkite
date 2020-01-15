// Implements the ExampleGraph operation

package main

func init() {
	operationRepository["ExampleGraph"] = Operation{
		execute: func(ea *EntityAccessor) error {
			vertexSet := VertexSet{MappingToUnordered: []int64{0, 1, 2, 3}}
			ea.output("vertices", &vertexSet)
			eb := &EdgeBundle{
				Src:         []int{0, 1, 2, 2},
				Dst:         []int{1, 0, 0, 1},
				EdgeMapping: []int64{0, 1, 2, 3},
			}
			ea.output("edges", eb)
			name := &StringAttribute{
				Values:  []string{"Adam", "Eve", "Bob", "Isolated Joe"},
				Defined: []bool{true, true, true, true},
			}
			ea.output("name", name)
			age := &DoubleAttribute{
				Values:  []float64{20.3, 18.2, 50.3, 2.0},
				Defined: []bool{true, true, true, true},
			}
			ea.output("age", age)
			gender := &StringAttribute{
				Values:  []string{"Male", "Female", "Male", "Male"},
				Defined: []bool{true, true, true, true},
			}
			ea.output("gender", gender)
			income := &DoubleAttribute{
				Values:  []float64{1000, 0, 0, 2000},
				Defined: []bool{true, false, false, true},
			}
			ea.output("income", income)
			location := &DoubleTuple2Attribute{
				Values1: []float64{40.71448, 47.5269674, 1.352083, -33.8674869},
				Values2: []float64{-74.00598, 19.0323968, 103.819836, 151.2069902},
				Defined: []bool{true, true, true, true},
			}
			ea.output("location", location)
			comment := &StringAttribute{
				Values: []string{"Adam loves Eve", "Eve loves Adam",
					"Bob envies Adam", "Bob loves Eve"},
				Defined: []bool{true, true, true, true},
			}
			ea.output("comment", comment)
			weight := &DoubleAttribute{
				Values:  []float64{1, 2, 3, 4},
				Defined: []bool{true, true, true, true},
			}
			ea.output("weight", weight)
			greeting := Scalar{Value: "Hello world! 😀 "}
			ea.output("greeting", &greeting)
			return nil
		},
	}
}

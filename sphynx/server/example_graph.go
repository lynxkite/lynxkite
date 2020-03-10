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
				Values:  []float64{1000, 0, 2000, 0},
				Defined: []bool{true, false, true, false},
			}
			ea.output("income", income)
			location := &DoubleTuple2Attribute{
				Values: []DoubleTuple2AttributeValue{{40.71448, -74.00598},
					{47.5269674, 19.0323968},
					{1.352083, 103.819836},
					{-33.8674869, 151.2069902}},
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
			greeting, err := ScalarFrom("Hello world! 😀 ")
			if err != nil {
				return err
			}
			ea.output("greeting", &greeting)
			return nil
		},
	}
}

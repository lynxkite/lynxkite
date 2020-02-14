package main

import (
	"reflect"
	"testing"
)

func TestPulledOverVertexAttribute(t *testing.T) {
	edges := EdgeBundle{
		Src:         []int{1, 2, 3},
		Dst:         []int{1, 3, 3},
		EdgeMapping: nil,
	}
	orig := DoubleAttribute{
		Values:  []float64{10, -1, 12, 13},
		Defined: []bool{true, false, true, true},
	}
	dst := VertexSet{
		MappingToUnordered: []int64{0, 1, 2, 3},
		MappingToOrdered:   nil,
	}
	ea := EntityAccessor{inputs: map[string]Entity{
		"originalAttr":  &orig,
		"destinationVS": &dst,
		"function":      &edges,
	}, outputs: make(map[GUID]Entity), opInst: &OperationInstance{
		GUID:      "guid",
		Inputs:    map[string]GUID{},
		Outputs:   map[string]GUID{"pulledAttr": "pa"},
		Operation: OperationDescription{},
	}, server: nil}
	err := operationRepository["PulledOverVertexAttribute"].execute(&ea)
	if err != nil {
		t.Errorf("PulledOverVertexAttribute failed: %v", err)
	}
	expected := DoubleAttribute{
		Values:  []float64{0, 0, 13, 13},
		Defined: []bool{false, false, true, true},
	}
	res := ea.getOutput("pulledAttr")
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("PulledOverVertexAttribute returned %v instead of %v", res, expected)
	}
}

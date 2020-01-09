package dapcstp

import (
	"testing"
)

func ButterflyBowGraph() *Graph {
	g := &Graph{
		Arcs: 6,
		Cost: []Value{1, 2, 0, 4, 4, 0},
		Src:  []int{0, 1, 2, 0, 3, 4},
		Dst:  []int{1, 2, 0, 3, 4, 0},

		Root:     0,
		Nodes:    5,
		Prize:    []Value{0, 0, 3.5, 0, 2},
		Fixed:    []bool{false, false, false, true, false, false},
		Incoming: [][]int{[]int{2, 5}, []int{0}, []int{1}, []int{3}, []int{4}},
		Outgoing: [][]int{[]int{0, 3}, []int{1}, []int{2}, []int{4}, []int{5}},
	}
	return g
}

func TestActiveComponent(t *testing.T) {
	g := ButterflyBowGraph()
	tests := []struct {
		cr       []Value
		expected []bool
		v        int
	}{
		{[]Value{0, 0, 0, 0, 0, 0}, []bool{true, true, true, true, true}, 0},
		{[]Value{1, 1, 1, 1, 1, 1}, []bool{true, false, false, false, false}, 0},
		{[]Value{1, 0, 1, 0, 1, 0}, []bool{true, false, false, false, true}, 0},
		{[]Value{0, 0, 0, 1, 1, 1}, []bool{true, true, true, false, false}, 0},
		{[]Value{0, 0, 0, 4, 4, 0}, []bool{true, true, true, false, true}, 2},
	}
	for _, test := range tests {
		component := activeComponent(g, test.cr, test.v)
		for i, c := range component {
			expected := test.expected[i]
			if c != expected {
				t.Errorf("activeComponent(g, cr = %v, v = %v)[%v] = %v, want %v.",
					test.cr, test.v, i, c, expected)
			}
		}
	}
}

func TestDualAscent(t *testing.T) {
	g := ButterflyBowGraph()
	dummyFeasiblePrimal := make([]bool, g.Arcs)
	lb, cr, pi := DualAscent(g, dummyFeasiblePrimal)
	expectedLB := Value(9)
	expectedCR := []Value{0, 0, 0, 0, 2, 0}
	expectedPi := []Value{0, 0, 0.5, 0, 0}
	if lb != expectedLB {
		t.Errorf("DualAscent returns lower bound %v, want %v.",
			lb, expectedLB)
	}
	for arc, c := range cr {
		expected := expectedCR[arc]
		if c != expected {
			t.Errorf("DualAscent returns reduced cost %v for arc %v, want %v.",
				c, arc, expected)
		}
	}
	for node, p := range pi {
		expected := expectedPi[node]
		if p != expected {
			t.Errorf("DualAscent returns pi %v for node %v, want %v.",
				p, node, expected)
		}
	}
}

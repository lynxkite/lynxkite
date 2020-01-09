/*
An implementation of the algorithm described in "A Dual Ascent-Based Branch-and-Bound
Framework for the Prize-Collecting Steiner Tree and Related Problems" by Leitner et al.
*/
package dapcstp

import (
	"math"
	"strconv"
)

// Value is the type for representing costs and prizes.
type Value float64

func (v Value) ToString() string {
	return strconv.FormatFloat(float64(v), 'f', -1, 64)
}

// ValueMax is the maximal value for Value
const ValueMax = Value(math.MaxFloat64)

// Graph represents the problem statement.
type Graph struct {
	Arcs int
	Cost []Value
	Src  []int
	Dst  []int

	Root     int
	Nodes    int
	Terminal []bool
	Prize    []Value
	Fixed    []bool
	Incoming [][]int
	Outgoing [][]int
}

func NewSolution(g *Graph) *Solution {
	return &Solution{Graph: g, Profit: 0,
		Arcs: make([]bool, g.Arcs), Nodes: make([]bool, g.Nodes)}
}

type Solution struct {
	Arcs   []bool
	Nodes  []bool
	Graph  *Graph
	Profit Value
}

// BuildArc maintains the profit and the nodes when building an arc.
func (s *Solution) BuildArc(arcID int) {
	if s.Arcs[arcID] {
		panic("Already built")
	}
	dst := s.Graph.Dst[arcID]
	s.Nodes[dst] = true // Assume src is already connected
	s.Arcs[arcID] = true
	s.Profit += s.Graph.Prize[dst] - s.Graph.Cost[arcID]
}

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
	// Edges
	Arcs int     // The number of edges in the graph
	Cost []Value // The cost for including the given edge
	Src  []int   // The source vertex id for the edge
	Dst  []int   // The dst vertex id for the edge

	// Vertices
	Root     int     // The vertex (the id) of the root of the solution tree
	Nodes    int     // The number of vertices
	Prize    []Value // The reward for including the given vertex in the tree.
	Fixed    []bool  // True if the given vertex must be part of the solution
	Terminal []bool  // The Prize is positive or Fixed is true
	Incoming [][]int // For a given vertex, the list of incoming edge ids
	Outgoing [][]int // For a given vertex, the list of outgoing edge ids
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

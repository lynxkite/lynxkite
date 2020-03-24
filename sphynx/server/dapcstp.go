// Implements the backend for the "Find Steiner tree" operation.
// The simpler Dapcstp name is retained here.

package main

import (
	"dapcstp"
)

// The function graph will convert the problem specification described in
// LK's attribute terms to a dapcstp.Graph. The dapcstp interface expects
// exactly one root, so we add one node to play the role of this single root
// and add edges from this single root to those nodes whose rootCosts attribute
// is non-negative. The extra edges and the extra node are allocated from
// the end of the id space, so it is easy to go back to the original graph,
// once dapcstp has done its thing.
func graph(
	gain *DoubleAttribute,
	rootCosts *DoubleAttribute,
	edgeCosts *DoubleAttribute,
	edges *EdgeBundle) (dapcstp.Graph, error) {

	roots := make([]int, 0)
	for idx, v := range rootCosts.Values {
		if rootCosts.Defined[idx] && v >= 0 {
			roots = append(roots, idx)
		}
	}

	origNArcs := len(edgeCosts.Values)
	origNNodes := len(gain.Values)
	nnodes := origNNodes + 1
	narcs := origNArcs + len(roots)
	g := dapcstp.Graph{
		Arcs:     narcs,
		Cost:     make([]dapcstp.Value, narcs),
		Src:      make([]int, narcs),
		Dst:      make([]int, narcs),
		Root:     origNNodes,
		Nodes:    nnodes,
		Terminal: make([]bool, nnodes),
		Prize:    make([]dapcstp.Value, nnodes),
		Fixed:    make([]bool, nnodes),
		Incoming: make([][]int, nnodes),
		Outgoing: make([][]int, nnodes),
	}

	for i := 0; i < origNNodes; i++ {
		prize := 0.0
		if gain.Defined[i] && gain.Values[i] > 0 {
			prize = gain.Values[i]
		}
		g.Prize[i] = dapcstp.Value(prize)
		g.Terminal[i] = prize > 0.0
	}

	for i := 0; i < origNArcs; i++ {
		src := int(edges.Src[i])
		dst := int(edges.Dst[i])
		g.Src[i] = src
		g.Dst[i] = dst
		c := 0.0
		if edgeCosts.Defined[i] && edgeCosts.Values[i] > 0 {
			c = edgeCosts.Values[i]
		}
		g.Cost[i] = dapcstp.Value(c)
		g.Incoming[dst] = append(g.Incoming[dst], i)
		g.Outgoing[src] = append(g.Outgoing[src], i)
	}

	// Setup the hidden root node
	g.Fixed[g.Root] = true
	g.Prize[g.Root] = 0.0
	g.Terminal[g.Root] = false

	// Setup the edges going from the root node to the potential tree roots
	for i, vid := range roots {
		eid := i + origNArcs
		g.Src[eid] = g.Root
		g.Dst[eid] = vid
		g.Cost[eid] = dapcstp.Value(rootCosts.Values[vid])
		g.Outgoing[g.Root] = append(g.Outgoing[g.Root], eid)
		g.Incoming[vid] = append(g.Incoming[vid], eid)
	}

	return g, nil
}

type SolutionWrapper struct {
	Arcs   DoubleAttribute
	Nodes  DoubleAttribute
	Roots  DoubleAttribute
	Profit Scalar
}

func doDapcstp(
	gain *DoubleAttribute,
	rootCosts *DoubleAttribute,
	edgeCosts *DoubleAttribute,
	edges *EdgeBundle) (SolutionWrapper, error) {
	graph, err := graph(gain, rootCosts, edgeCosts, edges)
	if err != nil {
		return SolutionWrapper{}, err
	}
	panic("PANIC!!!!")
	solution := dapcstp.PrimalHeuristic(&graph)
	// Behind the graph and the solution, there is actually
	// a greater graph than our original input, because
	// we appended a hidden root node and some edges
	// going out from this hidden root node.

	// But this wrapper should be of the same size
	// as the original input
	profitScalar, err := ScalarFrom(float64(solution.Profit))
	if err != nil {
		return SolutionWrapper{}, err
	}
	wrapper := SolutionWrapper{
		Arcs: DoubleAttribute{
			Values:  make([]float64, len(edges.Dst)),
			Defined: make([]bool, len(edges.Dst)),
		},
		Nodes: DoubleAttribute{
			Values:  make([]float64, len(gain.Values)),
			Defined: make([]bool, len(gain.Values)),
		},
		Roots: DoubleAttribute{
			Values:  make([]float64, len(gain.Values)),
			Defined: make([]bool, len(gain.Values)),
		},
		Profit: profitScalar,
	}

	for i := 0; i < len(wrapper.Arcs.Values); i++ {
		if solution.Arcs[i] {
			wrapper.Arcs.Values[i] = 1.0
			wrapper.Arcs.Defined[i] = true
		}
	}

	for i := 0; i < len(gain.Values); i++ {
		if solution.Nodes[i] {
			wrapper.Nodes.Values[i] = 1.0
			wrapper.Nodes.Defined[i] = true
		}
	}

	// The last len(solution.Arcs)  - len(edges.Dst) edges were appended to
	// mark edges going from the hidden root node to the visible root nodes
	for i := len(edges.Dst); i < len(solution.Arcs); i++ {
		if solution.Arcs[i] {
			vid := graph.Dst[i]
			wrapper.Roots.Values[vid] = 1.0
			wrapper.Roots.Defined[vid] = true
		}
	}
	return wrapper, nil
}

func init() {
	operationRepository["Dapcstp"] = Operation{
		execute: func(ea *EntityAccessor) error {
			es := ea.getEdgeBundle("es")
			gain := ea.getDoubleAttribute("gain")
			rootCosts := ea.getDoubleAttribute("root_costs")
			edgeCosts := ea.getDoubleAttribute("edge_costs")
			solution, err := doDapcstp(gain, rootCosts, edgeCosts, es)
			if err != nil {
				return err
			}
			ea.output("arcs", &solution.Arcs)
			ea.output("nodes", &solution.Nodes)
			ea.output("profit", &solution.Profit)
			ea.output("roots", &solution.Roots)
			return nil
		},
	}
}

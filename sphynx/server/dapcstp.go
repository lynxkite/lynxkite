package main

import (
	"dapcstp"
	"fmt"
)

func graph(gain *DoubleAttribute, root *DoubleAttribute, cost *DoubleAttribute, edges *EdgeBundle) (dapcstp.Graph, error) {
	narcs := len(cost.Values)
	nnodes := len(gain.Values)
	g := dapcstp.Graph{
		Arcs:     narcs,
		Cost:     make([]dapcstp.Value, narcs),
		Src:      make([]int, narcs),
		Dst:      make([]int, narcs),
		Root:     0,
		Nodes:    nnodes,
		Terminal: make([]bool, nnodes),
		Prize:    make([]dapcstp.Value, nnodes),
		Fixed:    make([]bool, nnodes),
		Incoming: make([][]int, nnodes),
		Outgoing: make([][]int, nnodes),
	}
	rootId := -1
	for i, v := range root.Values {
		if v == 1.0 && root.Defined[i] {
			if rootId != -1 {
				return g, fmt.Errorf("Multiple root nodes specified")
			}
			rootId = i
		}
	}
	if rootId == -1 {
		return g, fmt.Errorf("No root node specified")
	}
	g.Root = rootId

	for i := 0; i < nnodes; i++ {
		prize := 0.0
		if gain.Defined[i] {
			prize = gain.Values[i]
		}
		g.Prize[i] = dapcstp.Value(prize)
		g.Terminal[i] = prize > 0.0
		g.Fixed[i] = i == rootId
	}

	for i := 0; i < narcs; i++ {
		src := int(edges.Src[i])
		dst := int(edges.Dst[i])
		g.Src[i] = src
		g.Dst[i] = dst
		c := 0.0
		if cost.Defined[i] {
			c = cost.Values[i]
		}
		g.Cost[i] = dapcstp.Value(c)
		g.Incoming[dst] = append(g.Incoming[dst], i)
		g.Outgoing[src] = append(g.Outgoing[src], i)
	}
	return g, nil
}

type SolutionWrapper struct {
	Arcs   DoubleAttribute
	Nodes  DoubleAttribute
	Profit Scalar
}

func doDapcstp(
	gain *DoubleAttribute,
	root *DoubleAttribute,
	cost *DoubleAttribute,
	edges *EdgeBundle) (SolutionWrapper, error) {
	graph, err := graph(gain, root, cost, edges)
	if err != nil {
		return SolutionWrapper{}, err
	}
	solution := dapcstp.PrimalHeuristic(&graph)

	wrapper := SolutionWrapper{
		Arcs: DoubleAttribute{
			Values:  make([]float64, len(edges.Dst)),
			Defined: make([]bool, len(edges.Dst)),
		},
		Nodes: DoubleAttribute{
			Values:  make([]float64, len(gain.Values)),
			Defined: make([]bool, len(gain.Values)),
		},
		Profit: Scalar{float64(solution.Profit)},
	}

	for i := range solution.Arcs {
		if solution.Arcs[i] {
			wrapper.Arcs.Values[i] = 1.0
		} else {
			wrapper.Arcs.Values[i] = 0.0
		}
		wrapper.Arcs.Defined[i] = true
	}

	for i := range solution.Nodes {
		if solution.Nodes[i] {
			wrapper.Nodes.Values[i] = 1.0
		} else {
			wrapper.Nodes.Values[i] = 0.0
		}
		wrapper.Nodes.Defined[i] = true
	}

	return wrapper, nil
}

func init() {
	operationRepository["Dapcstp"] = Operation{
		execute: func(ea *EntityAccessor) error {
			es := ea.getEdgeBundle("es")
			gain := ea.getDoubleAttribute("gain")
			root := ea.getDoubleAttribute("root")
			cost := ea.getDoubleAttribute("cost")
			solution, err := doDapcstp(gain, root, cost, es)
			if err != nil {
				return err
			}
			ea.output("arcs", &solution.Arcs)
			ea.output("nodes", &solution.Nodes)
			ea.output("profit", &solution.Profit)
			return nil
		},
	}
}

package dapcstp

// PrimalHeuristic ...
func PrimalHeuristic(g *Graph) *Solution {
	emptyGraph := make([]bool, g.Arcs)
	_, reducedCost, _ := DualAscent(g, emptyGraph)
	steinerSolution := shortestPath(g, reducedCost)
	netWorth := make([]Value, g.Nodes)
	strongPrune(steinerSolution, g, g.Root, netWorth)
	return steinerSolution
}

// Iteratively connect the closest terminal to the already built solution.
func shortestPath(g *Graph, reducedCost []Value) *Solution {
	s := NewSolution(g)
	q := PriorityQueue{}
	distances := make([]Value, g.Nodes)
	backroad := make([]int, g.Nodes)
	for i := range distances {
		distances[i] = ValueMax
	}

	q.Push(&Item{value: g.Root, priority: 0})
	s.Nodes[g.Root] = true
	distances[g.Root] = 0

	for q.Len() > 0 {
		src := q.Pop().value
		if g.Terminal[src] && !s.Nodes[src] {
			s.Profit += g.Prize[src]
			for !s.Nodes[src] {
				s.Nodes[src] = true
				distances[src] = 0

				arcID := backroad[src]
				s.Arcs[arcID] = true
				s.Profit -= g.Cost[arcID]

				q.Push(&Item{value: src, priority: 0})

				src = g.Src[arcID]
			}
		} else {
			for _, arcID := range g.Outgoing[src] {
				if !(reducedCost[arcID] == 0) {
					continue
				}
				dst := g.Dst[arcID]
				cost := g.Cost[arcID]
				newTotal := distances[src] + cost

				if newTotal < distances[dst] {
					distances[dst] = newTotal
					backroad[dst] = arcID
					q.Push(&Item{value: dst, priority: newTotal})
				}
			}
		}
	}

	return s
}

// Cut off subtrees where cost exceeds gain.
func strongPrune(s *Solution, g *Graph, src int, netWorth []Value) {
	netWorth[src] = g.Prize[src]
	for _, arcID := range g.Outgoing[src] {
		if !s.Arcs[arcID] {
			continue
		}
		dst := g.Dst[arcID]
		strongPrune(s, g, dst, netWorth)
		cost := g.Cost[arcID]
		if cost >= netWorth[dst] {
			removeArc(s, arcID)
			removeSubtree(s, g, dst)
		} else {
			netWorth[src] = netWorth[src] + netWorth[dst] - cost
		}
	}
}

func removeSubtree(s *Solution, g *Graph, src int) {
	s.Nodes[src] = false
	for _, arcID := range g.Outgoing[src] {
		if s.Arcs[arcID] {
			removeArc(s, arcID)
			dst := g.Dst[arcID]
			removeSubtree(s, g, dst)
		}
	}
}

// removeArc maintains the profit and the nodes when removing an arc.
func removeArc(s *Solution, arcID int) {
	if !s.Arcs[arcID] {
		panic("Not built")
	}
	dst := s.Graph.Dst[arcID]
	s.Nodes[dst] = false // Assume that src is still connected.
	s.Arcs[arcID] = false
	s.Profit += -s.Graph.Prize[dst] + s.Graph.Cost[arcID]
}

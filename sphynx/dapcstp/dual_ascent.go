package dapcstp

// Implements the dual ascent algorithm from page 6. (Algorithm 1.)

// Puts each terminal into a priority queue with constant 1 priority.
func getInitialQueue(g *Graph) PriorityQueue {
	pq := PriorityQueue{}
	for i := 0; i < g.Nodes; i++ {
		if g.Fixed[i] || g.Prize[i] > 0 {
			pq.Push(&Item{value: i, priority: 1})
		}
	}
	return pq
}

// Returns a slice of length g.Nodes. Its elements are 1 for vertices from which
// v is reachable on edges with cr = 0 and 0 otherwise.
func activeComponent(g *Graph, cr []Value, v int) []bool {
	seen := make([]bool, g.Nodes)
	seen[v] = true
	pq := PriorityQueue{}
	pq.Push(&Item{value: v})
	for pq.Len() != 0 {
		toExplore := pq.Pop().value
		for _, uv := range g.Incoming[toExplore] {
			if cr[uv] == 0 {
				u := g.Src[uv]
				if seen[u] == false {
					pq.Push(&Item{value: u})
					seen[u] = true
				}
			}
		}
	}
	return seen
}

// Return the edges entering component.
func componentInArcs(g *Graph, component []bool) map[int]bool {
	inArcs := make(map[int]bool)
	for v := 0; v < g.Nodes; v++ {
		if component[v] {
			for _, ij := range g.Incoming[v] {
				i := g.Src[ij]
				if !component[i] {
					inArcs[ij] = true
				}
			}
		}
	}
	return inArcs
}

// Returns the minimum reduced cost among edges in componentInArcs.
func maxPossibleIncrease(g *Graph, cr []Value, componentInArcs map[int]bool) Value {
	min := ValueMax
	for arc := range componentInArcs {
		if cr[arc] < min {
			min = cr[arc]
		}
	}
	return min
}

// Update the score of the chosen active terminal.
func updateActiveQueue(g *Graph, cr []Value, activeQueue *PriorityQueue, v int,
	activeComponent []bool, feasiblePrimal []bool, inArcs map[int]bool) {
	score := 0
	size := 0
	for i := 0; i < g.Nodes; i++ {
		if activeComponent[i] {
			score += len(g.Incoming[i])
			size += 1
		}
	}
	score -= (size - 1)
	augmentation := 0
	for arc := range inArcs {
		if feasiblePrimal[arc] {
			augmentation += 1
		}
	}
	augmentation = (augmentation - 1) * g.Nodes
	if augmentation > 0 {
		score += augmentation
	}
	activeQueue.Push(&Item{value: int(v), priority: Value(score)})
	return
}

func DualAscent(g *Graph, feasiblePrimal []bool) (lowerBound Value, cr []Value, pi []Value) {
	lowerBound = 0
	cr = make([]Value, g.Arcs)
	copy(cr, g.Cost)
	pi = make([]Value, g.Nodes)
	copy(pi, g.Prize)
	activeQueue := getInitialQueue(g)
	for activeQueue.Len() > 0 {
		k := activeQueue.Pop().value
		w := activeComponent(g, cr, k)
		if w[g.Root] {
			continue
		}
		inArcs := componentInArcs(g, w)
		delta := maxPossibleIncrease(g, cr, inArcs)
		if !g.Fixed[k] {
			if delta > pi[k] {
				delta = pi[k]
			}
			pi[k] -= delta
		}
		for arc := range inArcs {
			cr[arc] -= delta
		}
		if pi[k] != 0 {
			updateActiveQueue(g, cr, &activeQueue, k, w, feasiblePrimal, inArcs)
		}
		lowerBound += delta
	}
	return
}

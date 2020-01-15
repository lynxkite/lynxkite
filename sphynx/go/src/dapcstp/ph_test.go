package dapcstp

import (
	"github.com/juju/errors"
	"os"
	"testing"
)

func ReadTestGraph(t *testing.T) *Graph {
	prefix := os.Getenv("TESTDATA")

	g, err := ReadGraph(prefix+"_vertices.csv", prefix+"_edges.csv")
	if err != nil {
		t.Error(errors.ErrorStack(err))
	}
	return g
}

func ReadForkingGraph(t *testing.T) *Graph {
	g, err := ReadGraph("graphs/forking_vertices.csv", "graphs/forking_edges.csv")
	if err != nil {
		t.Error(errors.ErrorStack(err))
	}
	return g
}

func TestLargeGraph(t *testing.T) {
	g := ReadTestGraph(t)
	s := shortestPath(g, make([]Value, g.Arcs))
	for i := range g.Terminal {
		if g.Terminal[i] && !s.Nodes[i] {
			t.Error("Not all terminals are built")
		}
	}

	// TODO: check if this is the actual best solution :)
	if int(s.Profit) != -41829 {
		t.Error("Profit does not match best solution's")
	}

	netWorth := make([]Value, g.Nodes)
	strongPrune(s, g, 0, netWorth)
	if int(s.Profit) != 38573 {
		t.Errorf("Unprofitable subtrees not pruned")
	}
}

func TestSmallGraph(t *testing.T) {
	g := ReadForkingGraph(t)
	s := shortestPath(g, make([]Value, g.Arcs))
	trapEdge := -1
	for i, cost := range g.Cost {
		if cost > 140 {
			trapEdge = i
		}
	}
	if s.Arcs[trapEdge] {
		t.Errorf("Non-optimal solution")
	}

	netWorth := make([]Value, g.Nodes)
	strongPrune(s, g, g.Root, netWorth)
	if !(0.69 < s.Profit && s.Profit < 0.72) {
		t.Errorf("Prune not working")
	}
}

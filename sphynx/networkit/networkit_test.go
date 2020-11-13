package networkit

import (
	"testing"
)

func ExampleGraph() Graph {
	eb := EdgeBundle{[]SphynxId{0, 1, 2, 3, 4}, []SphynxId{1, 2, 3, 4, 1}, nil}
	builder := NewGraphBuilder(uint64(5))
	for i := range eb.Src {
		builder.AddHalfEdge(uint64(eb.Src[i]), uint64(eb.Dst[i]))
	}
	return builder.ToGraph(true)
}

func TestBasicOps(t *testing.T) {
	c := NewBarabasiAlbertGenerator(uint64(10), uint64(50))
	g := c.Generate()
	b := NewBetweenness(g)
	b.Run()
	if b.Maximum() != 1176 {
		t.Errorf("Max betweenness is %v, expected 1176.", b.Maximum())
	}
}

func TestGraphToNetworKit(t *testing.T) {
	b := NewBetweenness(ExampleGraph())
	b.Run()
	if b.Maximum() != 6 {
		t.Errorf("Max betweenness is %v, expected 6.", b.Maximum())
	}
}

func TestNewVertexAttribute(t *testing.T) {
	b := NewBetweenness(ExampleGraph())
	b.Run()
	s := ToSlice(b.Scores())
	expected := []float64{0, 7, 2, 1, 2}
	if len(s) != len(expected) {
		t.Errorf("Result is %v, expected %v.", s, expected)
	}
	for i := range s {
		if s[i] != expected[i] {
			t.Errorf("Result is %v, expected %v.", s, expected)
			break
		}
	}
}

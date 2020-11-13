package networkit

import (
	"testing"
)

type SphynxId uint32
type EdgeBundle struct {
	Src         []SphynxId
	Dst         []SphynxId
	EdgeMapping []int64
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
	sphynxEB := EdgeBundle{[]SphynxId{0, 1, 2, 3, 4}, []SphynxId{1, 2, 3, 4, 1}, nil}
	builder := NewGraphBuilder(uint64(5))
	for i := range sphynxEB.Src {
		builder.AddHalfEdge(uint64(sphynxEB.Src[i]), uint64(sphynxEB.Dst[i]))
	}
	g := builder.ToGraph(true)
	b := NewBetweenness(g)
	b.Run()
	if b.Maximum() != 6 {
		t.Errorf("Max betweenness is %v, expected 6.", b.Maximum())
	}

}

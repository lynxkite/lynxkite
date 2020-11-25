package main

import (
	"github.com/lynxkite/lynxkite/sphynx/networkit"
	"testing"
)

func ExampleGraph() networkit.Graph {
	vs := VertexSet{MappingToUnordered: []int64{0, 1, 2, 3, 4}}
	es := EdgeBundle{Src: []SphynxId{0, 1, 2, 3, 4}, Dst: []SphynxId{1, 2, 3, 4, 1}}
	return ToNetworKit(&vs, &es)
}

func TestBasicOps(t *testing.T) {
	c := networkit.NewBarabasiAlbertGenerator(uint64(10), uint64(50))
	defer networkit.DeleteBarabasiAlbertGenerator(c)
	g := c.Generate()
	b := networkit.NewBetweenness(g)
	defer networkit.DeleteBetweenness(b)
	b.Run()
	if b.Maximum() != 1176 {
		t.Errorf("Max betweenness is %v, expected 1176.", b.Maximum())
	}
}

func TestGraphToNetworKit(t *testing.T) {
	b := networkit.NewBetweenness(ExampleGraph())
	defer networkit.DeleteBetweenness(b)
	b.Run()
	if b.Maximum() != 6 {
		t.Errorf("Max betweenness is %v, expected 6.", b.Maximum())
	}
}

func TestNewVertexAttribute(t *testing.T) {
	b := networkit.NewBetweenness(ExampleGraph())
	defer networkit.DeleteBetweenness(b)
	b.Run()
	s := ToDoubleSlice(b.Scores())
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

func TestGraphToSphynx(t *testing.T) {
	c := networkit.NewBarabasiAlbertGenerator(uint64(2), uint64(5))
	defer networkit.DeleteBarabasiAlbertGenerator(c)
	vs, es := ToSphynx(c.Generate())
	if len(vs.MappingToUnordered) != 5 {
		t.Errorf("Vertex set is %v, expected 5.", vs.MappingToUnordered)
	}
	expectedSrc := []SphynxId{1, 1, 2, 2, 3, 3, 4, 4}
	if len(es.Src) != len(expectedSrc) {
		t.Errorf("Source list is %v, expected %v.", es.Src, expectedSrc)
	}
	if len(es.Dst) != len(expectedSrc) {
		t.Errorf("Destination list is %v, expected length %v.", es.Dst, len(expectedSrc))
	}
	for i := range es.Src {
		if es.Src[i] != expectedSrc[i] {
			t.Errorf("Source list is %v, expected %v.", es.Src, expectedSrc)
			break
		}
	}
}

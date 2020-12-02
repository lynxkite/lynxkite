package main

import (
	"testing"

	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func ExampleGraph() networkit.Graph {
	vs := VertexSet{MappingToUnordered: []int64{0, 1, 2, 3, 4}}
	es := EdgeBundle{Src: []SphynxId{0, 1, 2, 3, 4}, Dst: []SphynxId{1, 2, 3, 4, 1}}
	return ToNetworKit(&vs, &es, true)
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
	if b.Maximum() != 12 {
		t.Errorf("Max betweenness is %v, expected 6.", b.Maximum())
	}
}

func TestNewVertexAttribute(t *testing.T) {
	b := networkit.NewBetweenness(ExampleGraph())
	defer networkit.DeleteBetweenness(b)
	b.Run()
	s := ToDoubleSlice(b.Scores())
	expected := []float64{0, 6, 5, 4, 3}
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
	g := c.Generate()
	defer networkit.DeleteGraph(g)
	vs, es := ToSphynx(g)
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

func TestVectorVector(t *testing.T) {
	c := networkit.NewBarabasiAlbertGenerator(uint64(3), uint64(10))
	defer networkit.DeleteBarabasiAlbertGenerator(c)
	g := c.Generate()
	defer networkit.DeleteGraph(g)
	v := networkit.NewPivotMDS(g, uint64(2), uint64(3))
	defer networkit.DeletePivotMDS(v)
	v.Run()
	points := v.GetCoordinates()
	defer networkit.DeletePointVector(points)
	for i := 0; i < int(points.Size()); i += 1 {
		x := points.Get(i).At(0)
		y := points.Get(i).At(1)
		if x < -2 || x > 2 || x == 0 || y < -2 || y > 2 || y == 0 {
			t.Errorf("Unexpected coordinates: %v %v", x, y)
		}
	}
}

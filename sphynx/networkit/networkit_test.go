package networkit

import (
	"testing"
)

func TestNetworKit(t *testing.T) {
	c := NewBarabasiAlbertGenerator(uint64(10), uint64(50))
	g := c.Generate()
	b := NewBetweenness(g)
	b.Run()
	if b.Maximum() != 1176 {
		t.Errorf("Max betweenness is %v, expected 1176.", b.Maximum())
	}

}

// Additional code to make it easier to work with NetworKit.
package main

import (
	"unsafe"

	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func ToNetworKit(vs *VertexSet, es *EdgeBundle, weight *DoubleAttribute, directed bool) networkit.Graph {
	builder := networkit.NewGraphBuilder(uint64(len(vs.MappingToUnordered)), weight != nil, directed)
	defer networkit.DeleteGraphBuilder(builder)
	for i := range es.Src {
		w := 1.0
		if weight != nil && weight.Defined[i] {
			w = weight.Values[i]
		}
		builder.AddHalfEdge(uint64(es.Src[i]), uint64(es.Dst[i]), w)
	}
	return builder.ToGraph(true)
}

func ToSphynx(g networkit.Graph) (vs *VertexSet, es *EdgeBundle) {
	vs = &VertexSet{}
	vs.MappingToUnordered = make([]int64, g.NumberOfNodes())
	for i := range vs.MappingToUnordered {
		vs.MappingToUnordered[i] = int64(i)
	}
	es = &EdgeBundle{}
	es.Src = make([]SphynxId, g.NumberOfEdges())
	es.Dst = make([]SphynxId, g.NumberOfEdges())
	// We want to copy directly into EdgeBundle from networkit.Graph.
	// But the networkit package doesn't know the SphynxId type.
	// Rather than merge the two packages or copy each element (again),
	// we use this unsafe cast.
	uint32Src := *(*[]uint32)(unsafe.Pointer(&es.Src))
	uint32Dst := *(*[]uint32)(unsafe.Pointer(&es.Dst))
	networkit.GraphToEdgeList(g, uint32Src, uint32Dst)
	es.EdgeMapping = make([]int64, len(es.Src))
	for i := range es.EdgeMapping {
		es.EdgeMapping[i] = int64(i)
	}
	return
}

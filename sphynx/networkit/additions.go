// Additional code to make it easier to work with NetworKit.
package networkit

type SphynxId uint32
type EdgeBundle struct {
	Src         []SphynxId
	Dst         []SphynxId
	EdgeMapping []int64
}

type VertexSet struct {
	MappingToUnordered []int64
	MappingToOrdered   map[int64]SphynxId
}

func ToDoubleSlice(v DoubleVector) []float64 {
	s := make([]float64, v.Size())
	for i := range s {
		s[i] = v.Get(i)
	}
	return s
}

func ToIdSlice(v IdVector) []SphynxId {
	s := make([]SphynxId, v.Size())
	for i := range s {
		s[i] = SphynxId(v.Get(i))
	}
	return s
}

func ToNetworKit(vs VertexSet, es EdgeBundle) Graph {
	builder := NewGraphBuilder(uint64(len(vs.MappingToUnordered)))
	for i := range es.Src {
		builder.AddHalfEdge(uint64(es.Src[i]), uint64(es.Dst[i]))
	}
	return builder.ToGraph(true)
}

func ToSphynx(g Graph) (vs VertexSet, es EdgeBundle) {
	vs = VertexSet{}
	vs.MappingToUnordered = make([]int64, g.NumberOfNodes())
	for i := range vs.MappingToUnordered {
		vs.MappingToUnordered[i] = int64(i)
	}
	es = EdgeBundle{}
	es.Src = make([]SphynxId, g.NumberOfEdges())
	es.Dst = make([]SphynxId, g.NumberOfEdges())
	GraphToEdgeList(g, es.Src, es.Dst)
	es.EdgeMapping = make([]int64, len(es.Src))
	for i := range es.EdgeMapping {
		es.EdgeMapping[i] = int64(i)
	}
	return
}

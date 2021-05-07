// Implements the StripDuplicateEdgesFromBundle operation
// See the Spark implementation for details

package main

func doStripDuplicateEdgesFromBundle(es *EdgeBundle) *EdgeBundle {
	type EdgeKey struct {
		src SphynxId
		dst SphynxId
	}

	uniqueEdges := make(map[EdgeKey]int64, len(es.Src))
	for i := 0; i < len(es.Src); i++ {
		k := EdgeKey{
			src: es.Src[i],
			dst: es.Dst[i],
		}
		uniqueEdges[k] = es.EdgeMapping[i]
	}
	uniqueBundle := NewEdgeBundle(len(uniqueEdges), len(uniqueEdges))
	i := 0
	for key, id := range uniqueEdges {
		uniqueBundle.Src[i] = key.src
		uniqueBundle.Dst[i] = key.dst
		uniqueBundle.EdgeMapping[i] = id
		i++
	}
	uniqueBundle.Sort()
	return uniqueBundle
}

func init() {
	operationRepository["StripDuplicateEdgesFromBundle"] = Operation{
		execute: func(ea *EntityAccessor) error {
			es := ea.getEdgeBundle("es")
			unique := doStripDuplicateEdgesFromBundle(es)
			ea.output("unique", unique)
			return nil
		},
	}
}

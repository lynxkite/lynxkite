// Implements the ReverseEdges operation

package main

func doReverseEdges(esAB *EdgeBundle) (esBA *EdgeBundle, injection *EdgeBundle) {
	numEdges := len(esAB.Dst)
	esBA = NewEdgeBundle(numEdges, numEdges)
	injection = NewEdgeBundle(numEdges, numEdges)
	for i := 0; i < numEdges; i++ {
		esBA.EdgeMapping[i] = esAB.EdgeMapping[i]
		esBA.Dst[i] = esAB.Src[i]
		esBA.Src[i] = esAB.Dst[i]
		injection.EdgeMapping[i] = esBA.EdgeMapping[i]
		injection.Src[i] = VertexID(i)
		injection.Dst[i] = VertexID(i)
	}
	return
}

func init() {
	operationRepository["ReverseEdges"] = Operation{
		execute: func(ea *EntityAccessor) error {
			esAB := ea.getEdgeBundle("esAB")
			esBA, injection := doReverseEdges(esAB)
			ea.output("esBA", esBA)
			ea.output("injection", injection)
			return nil
		},
	}
}

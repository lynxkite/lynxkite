// Implements the ReverseEdges operation
// See the Spark implementation for details

package main

func doReverseEdges(esAB *EdgeBundle) (esBA *EdgeBundle, injection *EdgeBundle) {
	numEdges := len(esAB.Dst)
	esBA = NewEdgeBundle(numEdges, numEdges)
	injection = NewEdgeBundle(numEdges, numEdges)
	n := VERTEX_ID(numEdges)
	for i := VERTEX_ID(0); i < n; i++ {
		esBA.EdgeMapping[i] = esAB.EdgeMapping[i]
		esBA.Dst[i] = esAB.Src[i]
		esBA.Src[i] = esAB.Dst[i]
		injection.EdgeMapping[i] = esBA.EdgeMapping[i]
		injection.Src[i] = i
		injection.Dst[i] = i
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

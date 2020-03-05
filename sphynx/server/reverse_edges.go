// Implements the ReverseEdges operation
// See the Spark implementation for details

package main

func doReverseEdges(esAB *EdgeBundle) (esBA *EdgeBundle, injection *EdgeBundle) {
	numEdges := len(esAB.Dst)
	esBA = &EdgeBundle{
		Src:         make([]int, numEdges),
		Dst:         make([]int, numEdges),
		EdgeMapping: make([]int64, numEdges),
	}
	injection = &EdgeBundle{
		Src:         make([]int, numEdges),
		Dst:         make([]int, numEdges),
		EdgeMapping: make([]int64, numEdges),
	}
	for i := 0; i < numEdges; i++ {
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

// Implements the AddReversedEdges operation

package main

// Creates a set id set.
// TODO: Check if these should be random ids similar to our
// Scala/Spark randomNumbered implementation
func newEdgeMapping(numIds int) []int64 {
	m := make([]int64, numIds)
	for i := 0; i < numIds; i++ {
		m[i] = int64(i)
	}
	return m
}

func doAddReversedEdges(edges *EdgeBundle,
	addIsNewAttr bool) (esPlus *EdgeBundle, newToOriginal *EdgeBundle, isNew *DoubleAttribute) {
	numOldEdges := len(edges.Dst)
	numNewEdges := numOldEdges * 2
	esPlus = &EdgeBundle{
		Src:         make([]VertexID, numNewEdges),
		Dst:         make([]VertexID, numNewEdges),
		EdgeMapping: newEdgeMapping(numNewEdges),
	}
	newToOriginal = &EdgeBundle{
		Src:         make([]VertexID, numNewEdges),
		Dst:         make([]VertexID, numNewEdges),
		EdgeMapping: newEdgeMapping(numNewEdges),
	}

	if addIsNewAttr {
		defined := make([]bool, numNewEdges)
		for i := range defined {
			defined[i] = true
		}
		isNew = &DoubleAttribute{
			Values:  make([]float64, numNewEdges),
			Defined: defined,
		}
	} else {
		isNew = nil
	}
	j := VertexID(0)
	n := VertexID(numOldEdges)
	for i := VertexID(0); i < n; i++ {
		esPlus.Src[j] = edges.Src[i]
		esPlus.Dst[j] = edges.Dst[i]
		esPlus.Src[j+1] = edges.Dst[i]
		esPlus.Dst[j+1] = edges.Src[i]
		newToOriginal.Src[j] = j
		newToOriginal.Src[j+1] = j + 1
		newToOriginal.Dst[j] = i
		newToOriginal.Dst[j+1] = i
		if addIsNewAttr {
			isNew.Values[j] = 0.0
			isNew.Values[j+1] = 1.0
		}
		j += 2
	}
	return
}

func init() {
	operationRepository["AddReversedEdges"] = Operation{
		execute: func(ea *EntityAccessor) error {
			es := ea.getEdgeBundle("es")
			addIsNewAttr := ea.GetBoolParamWithDefault("addIsNewAttr", false)
			esPlus, newToOriginal, isNew := doAddReversedEdges(es, addIsNewAttr)
			ea.output("esPlus", esPlus)
			ea.output("newToOriginal", newToOriginal)
			ea.output("isNew", isNew)
			return nil
		},
	}
}

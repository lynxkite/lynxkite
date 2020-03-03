package main

func doOutDegree(es *EdgeBundle, src *VertexSet) *DoubleAttribute {

	w := make(map[int]int, len(src.MappingToUnordered))
	for i := 0; i < len(es.Src); i++ {
		w[es.Src[i]]++
	}
	outDegree := &DoubleAttribute{
		Values:  make([]float64, len(src.MappingToUnordered)),
		Defined: make([]bool, len(src.MappingToUnordered)),
	}
	for i, cnt := range w {
		outDegree.Values[i] = float64(cnt)
		outDegree.Defined[i] = true
	}
	return outDegree
}

func init() {
	operationRepository["OutDegree"] = Operation{
		execute: func(ea *EntityAccessor) error {
			es := ea.getEdgeBundle("es")
			src := ea.getVertexSet("src")
			outDegree := doOutDegree(es, src)
			ea.output("outDegree", outDegree)
			return nil
		},
	}
}

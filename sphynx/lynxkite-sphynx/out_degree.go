// Implements the OutDegree operation
// See the Spark implementation for details

package main

func doOutDegree(es *EdgeBundle, src *VertexSet) *DoubleAttribute {

	outDegree := &DoubleAttribute{
		Values:  make([]float64, len(src.MappingToUnordered)),
		Defined: make([]bool, len(src.MappingToUnordered)),
	}
	for i := 0; i < len(src.MappingToUnordered); i++ {
		outDegree.Defined[i] = true
	}

	degree := make(map[SphynxId]SphynxId, len(src.MappingToUnordered))
	for i := 0; i < len(es.Src); i++ {
		degree[es.Src[i]]++
	}
	for i, cnt := range degree {
		outDegree.Values[i] = float64(cnt)
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

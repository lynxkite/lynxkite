// All NetworKit ops that compute a vertex attribute on a graph.
package main

import (
	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func ToNetworKit(vs *VertexSet, es *EdgeBundle) networkit.Graph {
	builder := networkit.NewGraphBuilder(uint64(len(vs.MappingToUnordered)))
	for i := range es.Src {
		builder.AddHalfEdge(uint64(es.Src[i]), uint64(es.Dst[i]))
	}
	return builder.ToGraph(true)
}

func init() {
	operationRepository["NetworKitComputeAttribute"] = Operation{
		execute: func(ea *EntityAccessor) error {
			vs := ea.getVertexSet("vs")
			es := ea.getEdgeBundle("es")
			g := ToNetworKit(vs, es)
			attr := &DoubleAttribute{
				Values:  make([]float64, len(vs.MappingToUnordered)),
				Defined: make([]bool, len(vs.MappingToUnordered)),
			}
			var result networkit.DoubleVector
			switch ea.GetStringParam("op") {
			case "betweenness":
				b := networkit.NewBetweenness(g)
				b.Run()
				result = b.Scores()
			}
			attr.Values = networkit.ToDoubleSlice(result)
			for i := range attr.Defined {
				attr.Defined[i] = true
			}
			ea.output("attr", attr)
			return nil
		},
	}
}

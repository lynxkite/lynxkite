// All NetworKit ops that compute a Double edge attribute on a graph.
package main

import (
	"math"

	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func init() {
	operationRepository["NetworKitComputeDoubleEdgeAttribute"] = Operation{
		execute: func(ea *EntityAccessor) (err error) {
			h := NewNetworKitHelper(ea)
			o := h.Options
			defer func() {
				e := h.Cleanup()
				if err == nil {
					err = e
				}
			}()
			vs := ea.getVertexSet("vs")
			es := ea.getEdgeBundle("es")
			g := h.GetGraph()
			g.IndexEdges()
			var result networkit.DoubleVector
			switch h.Op {
			case "ForestFireScore":
				c := networkit.NewForestFireScore(
					g, o.Double("spread_prob"), o.Double("burn_ratio"))
				defer networkit.DeleteForestFireScore(c)
				c.Run()
				result = c.Scores()
			case "RandomMaximumSpanningForest":
				c := networkit.NewRandomMaximumSpanningForest(g)
				defer networkit.DeleteRandomMaximumSpanningForest(c)
				c.Run()
				bools := c.GetAttribute(true)
				defer networkit.DeleteBoolVector(bools)
				result = networkit.NewDoubleVector(int64(len(es.Src)))
				for i := range es.Src {
					if bools.Get(i) {
						result.Set(i, 1.0)
					} else {
						result.Set(i, math.NaN())
					}
				}
			}
			// The NetworKit edge IDs don't correspond to the Sphynx edge IDs.
			// We build a map to match them up by the src/dst vertex IDs.
			type SrcDst struct {
				src SphynxId
				dst SphynxId
			}
			sdToKN := make(map[SrcDst]uint64)
			for src := range vs.MappingToUnordered {
				for deg, i := g.Degree(uint64(src)), uint64(0); i < deg; i += 1 {
					dst := g.GetIthNeighbor(uint64(src), i)
					sdToKN[SrcDst{SphynxId(src), SphynxId(dst)}] = g.GetOutEdgeId(uint64(src), i)
				}
			}
			attr := &DoubleAttribute{
				Values:  make([]float64, len(es.Src)),
				Defined: make([]bool, len(es.Src)),
			}
			for i := range es.Src {
				id := sdToKN[SrcDst{es.Src[i], es.Dst[i]}]
				attr.Values[i] = result.Get(int(id))
				attr.Defined[i] = !math.IsNaN(attr.Values[i])
			}
			return ea.output("attr", attr)
		},
	}
}

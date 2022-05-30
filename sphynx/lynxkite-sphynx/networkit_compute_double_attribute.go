// All NetworKit ops that compute a Double vertex attribute on a graph.
package main

import (
	"fmt"
	"math"

	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func init() {
	operationRepository["NetworKitComputeDoubleAttribute"] = Operation{
		execute: func(ea *EntityAccessor) (err error) {
			h := NewNetworKitHelper(ea)
			o := h.Options
			defer func() {
				e := h.Cleanup()
				if err == nil {
					err = e
				}
			}()
			g := h.GetGraph()
			vs := ea.getVertexSet("vs")
			attr := &DoubleAttribute{
				Values:  make([]float64, len(vs.MappingToUnordered)),
				Defined: make([]bool, len(vs.MappingToUnordered)),
			}
			var result networkit.DoubleVector
			switch h.Op {
			case "ApproxCloseness":
				samples := o.Count("samples")
				if samples > uint64(len(vs.MappingToUnordered)) {
					samples = uint64(len(vs.MappingToUnordered))
				}
				c := networkit.NewApproxCloseness(g, samples)
				defer networkit.DeleteApproxCloseness(c)
				c.Run()
				result = c.Scores()
			case "Betweenness":
				c := networkit.NewBetweenness(g)
				defer networkit.DeleteBetweenness(c)
				c.Run()
				result = c.Scores()
			case "CoreDecomposition":
				g.RemoveSelfLoops()
				c := networkit.NewCoreDecomposition(g)
				defer networkit.DeleteCoreDecomposition(c)
				c.Run()
				result = c.Scores()
			case "EigenvectorCentrality":
				c := networkit.NewEigenvectorCentrality(g)
				defer networkit.DeleteEigenvectorCentrality(c)
				c.Run()
				result = c.Scores()
			case "EstimateBetweenness":
				samples := o.Count("samples")
				if samples > uint64(len(vs.MappingToUnordered)) {
					samples = uint64(len(vs.MappingToUnordered))
				}
				c := networkit.NewEstimateBetweenness(g, samples)
				defer networkit.DeleteEstimateBetweenness(c)
				c.Run()
				result = c.Scores()
			case "HarmonicCloseness":
				c := networkit.NewHarmonicCloseness(g)
				defer networkit.DeleteHarmonicCloseness(c)
				c.Run()
				result = c.Scores()
			case "KatzCentrality":
				c := networkit.NewKatzCentrality(g)
				defer networkit.DeleteKatzCentrality(c)
				c.Run()
				result = c.Scores()
			case "KPathCentrality":
				c := networkit.NewKPathCentrality(g)
				defer networkit.DeleteKPathCentrality(c)
				c.Run()
				result = c.Scores()
			case "LaplacianCentrality":
				c := networkit.NewLaplacianCentrality(g)
				defer networkit.DeleteLaplacianCentrality(c)
				c.Run()
				result = c.Scores()
			case "Sfigality":
				c := networkit.NewSfigality(g)
				defer networkit.DeleteSfigality(c)
				c.Run()
				result = c.Scores()
			default:
				return fmt.Errorf("Unsupported operation: %v", h.Op)
			}
			for i := range attr.Defined {
				attr.Values[i] = result.Get(i)
				attr.Defined[i] = !math.IsNaN(attr.Values[i])
			}
			ea.output("attr", attr)
			return nil
		},
		canCompute: ifCudaDoesNotTakePrecedence,
	}
}

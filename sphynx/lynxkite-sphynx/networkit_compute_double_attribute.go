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
			defer func() {
				if e := recover(); e != nil {
					err = fmt.Errorf("%v", e)
				}
			}()
			vs := ea.getVertexSet("vs")
			es := ea.getEdgeBundle("es")
			weight := ea.getDoubleAttributeOpt("weight")
			o := &NetworKitOptions{ea.GetMapParam("options")}
			seed := uint64(1)
			if s, exists := o.Options["seed"]; exists {
				seed = uint64(s.(float64))
			}
			networkit.SetSeed(seed, true)
			networkit.SetThreadsFromEnv()
			// The caller can set "directed" to false to create an undirected graph.
			g := ToNetworKit(vs, es, weight, o.Options["directed"] != false)
			defer networkit.DeleteGraph(g)
			attr := &DoubleAttribute{
				Values:  make([]float64, len(vs.MappingToUnordered)),
				Defined: make([]bool, len(vs.MappingToUnordered)),
			}
			var result networkit.DoubleVector
			switch ea.GetStringParam("op") {
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
			}
			for i := range attr.Defined {
				attr.Values[i] = result.Get(i)
				attr.Defined[i] = !math.IsNaN(attr.Values[i])
			}
			ea.output("attr", attr)
			return nil
		},
	}
}

// All NetworKit ops that compute a Double edge attribute on a graph.
package main

import (
	"fmt"
	"log"
	"math"
	"runtime/debug"

	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func init() {
	operationRepository["NetworKitComputeDoubleEdgeAttribute"] = Operation{
		execute: func(ea *EntityAccessor) (err error) {
			defer func() {
				if e := recover(); e != nil {
					err = fmt.Errorf("%v", e)
					log.Printf("%v\n%v", e, string(debug.Stack()))
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
			g.IndexEdges()
			attr := &DoubleAttribute{
				Values:  make([]float64, len(vs.MappingToUnordered)),
				Defined: make([]bool, len(vs.MappingToUnordered)),
			}
			var result networkit.DoubleVector
			switch ea.GetStringParam("op") {
			case "ForestFireScore":
				c := networkit.NewForestFireScore(
					g, o.Double("spread_prob"), o.Double("burn_ratio"))
				defer networkit.DeleteForestFireScore(c)
				c.Run()
				result = c.Scores()
			}
			for i := range attr.Defined {
				attr.Values[i] = result.Get(i)
				attr.Defined[i] = !math.IsNaN(attr.Values[i])
			}
			return ea.output("attr", attr)
		},
	}
}

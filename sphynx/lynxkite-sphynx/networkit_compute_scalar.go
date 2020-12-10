// All NetworKit ops that compute a scalar on a graph.
package main

import (
	"fmt"
	"log"
	"runtime/debug"

	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func init() {
	operationRepository["NetworKitComputeScalar"] = Operation{
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
			attr := ea.getDoubleAttributeOpt("attr")
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
			scalar1 := 0.0
			scalar2 := 0.0
			switch ea.GetStringParam("op") {
			case "Diameter":
				maxError := o.Double("max_error")
				var c networkit.Diameter
				if maxError > 0 {
					c = networkit.NewDiameter(g, networkit.EstimatedRange, maxError)
				} else {
					c = networkit.NewDiameter(g, networkit.Exact)
				}
				defer networkit.DeleteDiameter(c)
				c.Run()
				scalar1 = networkit.DiameterLower(c)
				scalar2 = networkit.DiameterUpper(c)
			case "EffectiveDiameter":
				c := networkit.NewEffectiveDiameter(g, o.Double("ratio"))
				defer networkit.DeleteEffectiveDiameter(c)
				c.Run()
				scalar1 = c.GetEffectiveDiameter()
			case "EffectiveDiameterApproximation":
				c := networkit.NewEffectiveDiameterApproximation(
					g, o.Double("ratio"), o.Count("approximations"), o.Count("bits"))
				defer networkit.DeleteEffectiveDiameterApproximation(c)
				c.Run()
				scalar1 = c.GetEffectiveDiameter()
			case "Assortativity":
				vec := networkit.NewDoubleVector(len(attr.Values))
				defer networkit.DeleteDoubleVector(vec)
				for i, v := range attr.Values {
					vec.Set(i, v)
				}
				c := networkit.NewAssortativity(g, vec)
				defer networkit.DeleteAssortativity(c)
				c.Run()
				scalar1 = c.GetCoefficient()
			}
			if err = ea.outputScalar("scalar1", scalar1); err != nil {
				return
			}
			if err = ea.outputScalar("scalar2", scalar2); err != nil {
				return
			}
			return
		},
	}
}

// All NetworKit ops that compute a Vector[number] vertex attribute on a graph.
package main

import (
	"fmt"
	"log"
	"runtime/debug"

	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func PointsToAttribute(dim int, points networkit.PointVector, attr *DoubleVectorAttribute) {
	for i := range attr.Values {
		p := points.Get(i)
		attr.Defined[i] = true
		attr.Values[i] = make(DoubleVectorAttributeValue, dim)
		for j := 0; j < dim; j += 1 {
			attr.Values[i][j] = p.At(uint64(j))
		}
	}
}

func init() {
	operationRepository["NetworKitComputeVectorAttribute"] = Operation{
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
			attr := &DoubleVectorAttribute{
				Values:  make([]DoubleVectorAttributeValue, len(vs.MappingToUnordered)),
				Defined: make([]bool, len(vs.MappingToUnordered)),
			}
			switch ea.GetStringParam("op") {
			case "PivotMDS":
				dim := o.Count("dimensions")
				pivots := o.Count("pivots")
				if pivots > uint64(len(vs.MappingToUnordered)) {
					pivots = uint64(len(vs.MappingToUnordered))
				}
				c := networkit.NewPivotMDS(g, dim, pivots)
				defer networkit.DeletePivotMDS(c)
				c.Run()
				points := c.GetCoordinates()
				defer networkit.DeletePointVector(points)
				PointsToAttribute(int(dim), points, attr)
			case "MaxentStress":
				dim := o.Count("dimensions")
				c := networkit.NewMaxentStress(g, dim, o.Count("radius"), o.Double("tolerance"))
				defer networkit.DeleteMaxentStress(c)
				c.Run()
				points := c.GetCoordinates()
				defer networkit.DeletePointVector(points)
				PointsToAttribute(int(dim), points, attr)
			}
			ea.output("attr", attr)
			return nil
		},
	}
}

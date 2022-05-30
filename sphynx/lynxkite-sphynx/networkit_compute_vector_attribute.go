// All NetworKit ops that compute a Vector[number] vertex attribute on a graph.
package main

import (
	"fmt"
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
			attr := &DoubleVectorAttribute{
				Values:  make([]DoubleVectorAttributeValue, len(vs.MappingToUnordered)),
				Defined: make([]bool, len(vs.MappingToUnordered)),
			}
			switch h.Op {
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
			default:
				return fmt.Errorf("Unsupported operation: %v", h.Op)
			}
			ea.output("attr", attr)
			return nil
		},
		canCompute: ifCudaDoesNotTakePrecedence,
	}
}

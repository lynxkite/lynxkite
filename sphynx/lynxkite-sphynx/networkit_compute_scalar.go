// All NetworKit ops that compute a scalar on a graph.
package main

import (
	"fmt"
	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func init() {
	operationRepository["NetworKitComputeScalar"] = Operation{
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
			attr := ea.getDoubleAttributeOpt("attr")
			scalar1 := 0.0
			scalar2 := 0.0
			switch h.Op {
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
				vec := networkit.NewDoubleVector(int64(len(attr.Values)))
				defer networkit.DeleteDoubleVector(vec)
				for i, v := range attr.Values {
					vec.Set(i, v)
				}
				c := networkit.NewAssortativity(g, vec)
				defer networkit.DeleteAssortativity(c)
				c.Run()
				scalar1 = c.GetCoefficient()
			default:
				return fmt.Errorf("Unsupported operation: %v", h.Op)
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

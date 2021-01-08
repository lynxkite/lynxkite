// All NetworKit ops that compute a Double scalar on a segmentation.
package main

import (
	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func init() {
	operationRepository["NetworKitComputeSegmentationScalar"] = Operation{
		execute: func(ea *EntityAccessor) (err error) {
			h := NewNetworKitHelper(ea)
			defer func() {
				e := h.Cleanup()
				if err == nil {
					err = e
				}
			}()
			var scalar float64
			switch h.Op {
			case "Coverage":
				p := h.GetPartition()
				defer networkit.DeletePartition(p)
				c := networkit.NewCoverage()
				defer networkit.DeleteCoverage(c)
				scalar = c.GetQuality(p, h.GetGraph())
			}
			return ea.outputScalar("sc", scalar)
		},
	}
}

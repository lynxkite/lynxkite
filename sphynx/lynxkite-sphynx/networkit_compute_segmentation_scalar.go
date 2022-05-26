// All NetworKit ops that compute a Double scalar on a segmentation.
package main

import (
	"fmt"
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
				c := networkit.NewCoverage()
				defer networkit.DeleteCoverage(c)
				scalar = c.GetQuality(h.GetPartition(), h.GetGraph())
			case "EdgeCut":
				c := networkit.NewEdgeCut()
				defer networkit.DeleteEdgeCut(c)
				scalar = c.GetQuality(h.GetPartition(), h.GetGraph())
			case "Modularity":
				c := networkit.NewModularity()
				defer networkit.DeleteModularity(c)
				scalar = c.GetQuality(h.GetPartition(), h.GetGraph())
			default:
				return fmt.Errorf("Unsupported operation: %v", h.Op)
			}
			return ea.outputScalar("sc", scalar)
		},
	}
}

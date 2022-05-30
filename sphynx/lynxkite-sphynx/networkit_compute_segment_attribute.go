// All NetworKit ops that compute a Double vertex attribute on a segmentation.
package main

import (
	"fmt"
	"math"

	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func init() {
	operationRepository["NetworKitComputeSegmentAttribute"] = Operation{
		execute: func(ea *EntityAccessor) (err error) {
			h := NewNetworKitHelper(ea)
			defer func() {
				e := h.Cleanup()
				if err == nil {
					err = e
				}
			}()
			g := h.GetGraph()
			seg := ea.getVertexSet("segments")
			attr := &DoubleAttribute{
				Values:  make([]float64, len(seg.MappingToUnordered)),
				Defined: make([]bool, len(seg.MappingToUnordered)),
			}
			copyResults := func(c networkit.LocalCommunityEvaluation) {
				for i := range attr.Defined {
					attr.Values[i] = c.GetValue(uint64(i))
					attr.Defined[i] = !math.IsNaN(attr.Values[i])
				}
			}

			switch h.Op {
			case "CoverHubDominance":
				c := networkit.NewCoverHubDominance(g, h.GetCover())
				defer networkit.DeleteCoverHubDominance(c)
				c.Run()
				copyResults(c)
			case "IntrapartitionDensity":
				c := networkit.NewIntrapartitionDensity(g, h.GetPartition())
				defer networkit.DeleteIntrapartitionDensity(c)
				c.Run()
				copyResults(c)
			case "PartitionFragmentation":
				c := networkit.NewPartitionFragmentation(g, h.GetPartition())
				defer networkit.DeletePartitionFragmentation(c)
				c.Run()
				copyResults(c)
			case "IsolatedInterpartitionConductance":
				c := networkit.NewIsolatedInterpartitionConductance(g, h.GetPartition())
				defer networkit.DeleteIsolatedInterpartitionConductance(c)
				c.Run()
				copyResults(c)
			case "IsolatedInterpartitionExpansion":
				c := networkit.NewIsolatedInterpartitionExpansion(g, h.GetPartition())
				defer networkit.DeleteIsolatedInterpartitionExpansion(c)
				c.Run()
				copyResults(c)
			case "StablePartitionNodes":
				c := networkit.NewStablePartitionNodes(g, h.GetPartition())
				defer networkit.DeleteStablePartitionNodes(c)
				c.Run()
				copyResults(c)
			default:
				return fmt.Errorf("Unsupported operation: %v", h.Op)
			}
			ea.output("attr", attr)
			return nil
		},
	}
}

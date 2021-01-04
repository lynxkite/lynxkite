// All NetworKit ops that compute a Double vertex attribute on a segmentation.
package main

import (
	"fmt"
	"log"
	"math"
	"runtime/debug"

	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func init() {
	operationRepository["NetworKitComputeSegmentAttribute"] = Operation{
		execute: func(ea *EntityAccessor) (err error) {
			defer func() {
				if e := recover(); e != nil {
					err = fmt.Errorf("%v", e)
					log.Printf("%v\n%v", e, string(debug.Stack()))
				}
			}()
			vs := ea.getVertexSet("vs")
			es := ea.getEdgeBundle("es")
			seg := ea.getVertexSet("segments")
			belongsTo := ea.getEdgeBundle("belongsTo")
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
				Values:  make([]float64, len(seg.MappingToUnordered)),
				Defined: make([]bool, len(seg.MappingToUnordered)),
			}
			copyResults := func(c networkit.LocalCommunityEvaluation) {
				for i := range attr.Defined {
					attr.Values[i] = c.GetValue(uint64(i))
					attr.Defined[i] = !math.IsNaN(attr.Values[i])
				}
			}

			// The caller has to call networkit.DeletePartition.
			partition := func() networkit.Partition {
				p := networkit.NewPartition(uint64(len(vs.MappingToUnordered)))
				p.SetUpperBound(uint64(len(seg.MappingToUnordered)))
				log.Printf("counts: %v %v %v", len(vs.MappingToUnordered), len(seg.MappingToUnordered))
				for i := range belongsTo.EdgeMapping {
					log.Printf("%v %v %v", i, belongsTo.Dst[i], belongsTo.Src[i])
					p.AddToSubset(uint64(belongsTo.Dst[i]), uint64(belongsTo.Src[i]))
				}
				return p
			}
			switch ea.GetStringParam("op") {
			case "StablePartitionNodes":
				p := partition()
				defer networkit.DeletePartition(p)
				c := networkit.NewStablePartitionNodes(g, p)
				defer networkit.DeleteStablePartitionNodes(c)
				c.Run()
				copyResults(c)
			}
			ea.output("attr", attr)
			return nil
		},
	}
}

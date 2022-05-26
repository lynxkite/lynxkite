// All NetworKit ops that make a partition for a graph.
package main

import (
	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func init() {
	operationRepository["NetworKitCommunityDetection"] = Operation{
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
			var p networkit.Partition
			switch h.Op {
			case "LPDegreeOrdered":
				c := networkit.NewLPDegreeOrdered(g)
				defer networkit.DeleteLPDegreeOrdered(c)
				c.Run()
				p = c.GetPartition()
			case "PLM":
				c := networkit.NewPLM(g, false, o.Double("resolution"))
				defer networkit.DeletePLM(c)
				c.Run()
				p = c.GetPartition()
			case "PLP":
				c := networkit.NewPLP(g)
				defer networkit.DeletePLP(c)
				c.Run()
				p = c.GetPartition()
			}
			vs := &VertexSet{}
			vs.MappingToUnordered = make([]int64, p.NumberOfSubsets())
			mappingToOrdered := make(map[int64]SphynxId)
			ss := p.GetSubsetIdsVector()
			defer networkit.DeleteUint64Vector(ss)
			for i := range vs.MappingToUnordered {
				vs.MappingToUnordered[i] = int64(ss.Get(i))
				mappingToOrdered[int64(ss.Get(i))] = SphynxId(i)
			}
			es := &EdgeBundle{}
			es.EdgeMapping = make([]int64, p.NumberOfElements())
			es.Src = make([]SphynxId, p.NumberOfElements())
			es.Dst = make([]SphynxId, p.NumberOfElements())
			v := p.GetVector()
			defer networkit.DeleteUint64Vector(v)
			for i := range es.EdgeMapping {
				es.EdgeMapping[i] = int64(i)
				es.Src[i] = SphynxId(i)
				es.Dst[i] = SphynxId(mappingToOrdered[int64(v.Get(i))])
			}
			ea.output("partitions", vs)
			ea.output("belongsTo", es)
			return
		},
		canCompute: ifCudaDoesNotTakePrecedence,
	}
}

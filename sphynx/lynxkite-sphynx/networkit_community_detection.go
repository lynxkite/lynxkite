// All NetworKit ops that make a partition for a graph.
package main

import (
	"fmt"
	"log"
	"runtime/debug"

	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func init() {
	operationRepository["NetworKitCommunityDetection"] = Operation{
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
			var p networkit.Partition
			switch ea.GetStringParam("op") {
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
			defer networkit.DeletePartition(p)
			vs = &VertexSet{}
			vs.MappingToUnordered = make([]int64, p.NumberOfSubsets())
			vs.MappingToOrdered = make(map[int64]SphynxId)
			ss := p.GetSubsetIdsVector()
			defer networkit.DeleteUint64Vector(ss)
			for i := range vs.MappingToUnordered {
				vs.MappingToUnordered[i] = int64(ss.Get(i))
				vs.MappingToOrdered[int64(ss.Get(i))] = SphynxId(i)
			}
			es = &EdgeBundle{}
			es.EdgeMapping = make([]int64, p.NumberOfElements())
			es.Src = make([]SphynxId, p.NumberOfElements())
			es.Dst = make([]SphynxId, p.NumberOfElements())
			v := p.GetVector()
			defer networkit.DeleteUint64Vector(v)
			for i := range es.EdgeMapping {
				es.EdgeMapping[i] = int64(i)
				es.Src[i] = SphynxId(i)
				es.Dst[i] = SphynxId(vs.MappingToOrdered[int64(v.Get(i))])
			}
			ea.output("partitions", vs)
			ea.output("belongsTo", es)
			return
		},
	}
}

// All NetworKit ops that create a graph from nothing.
package main

import (
	"fmt"
	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func init() {
	operationRepository["NetworKitCreateGraph"] = Operation{
		execute: func(ea *EntityAccessor) error {
			options := ea.GetMapParam("options")
			getOpt := func(k string) interface{} {
				o := options[k]
				if o == nil {
					panic(fmt.Sprintf("%#v not found in %#v", k, options))
				}
				return o
			}
			double := func(k string) float64 {
				return getOpt(k).(float64)
			}
			count := func(k string) uint64 {
				return uint64(double(k))
			}
			var result networkit.Graph
			networkit.SetSeed(count("seed"), true)
			switch ea.GetStringParam("op") {
			case "BarabasiAlbertGenerator":
				g := networkit.NewBarabasiAlbertGenerator(
					count("attachments_per_node"),
					count("size"),
					count("connected_at_start"))
				defer networkit.DeleteBarabasiAlbertGenerator(g)
				result = g.Generate()
			case "ClusteredRandomGraphGenerator":
				g := networkit.NewClusteredRandomGraphGenerator(
					count("size"),
					count("clusters"),
					double("probability_in"),
					double("probability_out"))
				defer networkit.DeleteClusteredRandomGraphGenerator(g)
				result = g.Generate()
			}
			vs, es := ToSphynx(result)
			ea.output("vs", vs)
			ea.output("es", es)
			return nil
		},
	}
}

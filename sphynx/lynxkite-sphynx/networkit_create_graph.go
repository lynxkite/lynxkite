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
			opt64 := func(k string) uint64 {
				o := options[k]
				if o == nil {
					panic(fmt.Sprintf("%#v not found in %#v", k, options))
				}
				return uint64(o.(float64))
			}
			var result networkit.Graph
			networkit.SetSeed(opt64("seed"), true)
			switch ea.GetStringParam("op") {
			case "BarabasiAlbertGenerator":
				g := networkit.NewBarabasiAlbertGenerator(
					opt64("attachments_per_node"),
					opt64("size"),
					opt64("connected_at_start"))
				defer networkit.DeleteBarabasiAlbertGenerator(g)
				result = g.Generate()
			}
			vs, es := ToSphynx(result)
			ea.output("vs", vs)
			ea.output("es", es)
			return nil
		},
	}
}

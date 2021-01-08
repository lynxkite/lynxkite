// All NetworKit ops that create a graph from nothing.
package main

import (
	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func init() {
	operationRepository["NetworKitCreateGraph"] = Operation{
		execute: func(ea *EntityAccessor) (err error) {
			h := NewNetworKitHelper(ea)
			o := h.Options
			defer func() {
				e := h.Cleanup()
				if err == nil {
					err = e
				}
			}()
			var result networkit.Graph
			switch h.Op {
			case "BarabasiAlbertGenerator":
				g := networkit.NewBarabasiAlbertGenerator(
					o.Count("attachments_per_vertex"),
					o.Count("size"),
					o.Count("connected_at_start"))
				defer networkit.DeleteBarabasiAlbertGenerator(g)
				result = g.Generate()
			case "StaticDegreeSequenceGenerator":
				degs := o.DegreeVector("degrees")
				defer networkit.DeleteUint64Vector(degs)
				switch o.Get("algorithm").(string) {
				case "Chung–Lu":
					g := networkit.NewChungLuGenerator(degs)
					defer networkit.DeleteChungLuGenerator(g)
					result = g.Generate()
				case "Edge switching Markov chain":
					g := networkit.NewEdgeSwitchingMarkovChainGenerator(degs, true)
					defer networkit.DeleteEdgeSwitchingMarkovChainGenerator(g)
					result = g.Generate()
				case "Haveli–Hakimi":
					g := networkit.NewHavelHakimiGenerator(degs, true)
					defer networkit.DeleteHavelHakimiGenerator(g)
					result = g.Generate()
				}
			case "ClusteredRandomGraphGenerator":
				g := networkit.NewClusteredRandomGraphGenerator(
					o.Count("size"),
					o.Count("clusters"),
					o.Double("probability_in"),
					o.Double("probability_out"))
				defer networkit.DeleteClusteredRandomGraphGenerator(g)
				result = g.Generate()
			case "DorogovtsevMendesGenerator":
				g := networkit.NewDorogovtsevMendesGenerator(
					o.Count("size"))
				defer networkit.DeleteDorogovtsevMendesGenerator(g)
				result = g.Generate()
			case "ErdosRenyiGenerator":
				g := networkit.NewErdosRenyiGenerator(
					o.Count("size"),
					o.Double("probability"),
					true, // directed
					true) // allow self-loops
				defer networkit.DeleteErdosRenyiGenerator(g)
				result = g.Generate()
			case "HyperbolicGenerator":
				g := networkit.NewHyperbolicGenerator(
					o.Count("size"),
					o.Double("avg_degree"),
					o.Double("exponent"),
					o.Double("temperature"))
				defer networkit.DeleteHyperbolicGenerator(g)
				result = g.Generate()
			case "LFRGenerator":
				g := networkit.NewLFRGenerator(o.Count("size"))
				defer networkit.DeleteLFRGenerator(g)
				g.GeneratePowerlawDegreeSequence(
					o.Count("avg_degree"), o.Count("max_degree"), -o.Double("degree_exponent"))
				g.GeneratePowerlawCommunitySizeSequence(
					o.Count("min_community"), o.Count("max_community"), -o.Double("community_exponent"))
				g.SetMuWithBinomialDistribution(o.Double("avg_mixing"))
				result = g.Generate()
			case "MocnikGenerator":
				g := networkit.NewMocnikGenerator(
					o.Count("dimension"), o.Count("size"), o.Double("density"))
				defer networkit.DeleteMocnikGenerator(g)
				result = g.Generate()
			case "PubWebGenerator":
				g := networkit.NewPubWebGenerator(
					o.Count("size"),
					o.Count("dense_areas"),
					o.Double("neighborhood_radius"),
					o.Count("max_degree"))
				defer networkit.DeletePubWebGenerator(g)
				result = g.Generate()
			}
			vs, es := h.ToSphynx(result)
			ea.output("vs", vs)
			ea.output("es", es)
			return nil
		},
	}
}

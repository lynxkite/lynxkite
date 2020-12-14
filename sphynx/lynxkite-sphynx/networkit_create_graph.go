// All NetworKit ops that create a graph from nothing.
package main

import (
	"fmt"
	"log"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

type NetworKitOptions struct {
	Options map[string]interface{}
}

func (self *NetworKitOptions) Get(k string) interface{} {
	o := self.Options[k]
	if o == nil {
		panic(fmt.Sprintf("%#v not found in %#v", k, self.Options))
	}
	return o
}

func (self *NetworKitOptions) Double(k string) float64 {
	return self.Get(k).(float64)
}
func (self *NetworKitOptions) Count(k string) uint64 {
	return uint64(self.Double(k))
}
func (self *NetworKitOptions) Numbers(k string) []uint64 {
	raw := self.Get(k).(string)
	split := strings.Split(raw, ",")
	nums := make([]uint64, len(split))
	var err error
	for i, v := range split {
		nums[i], err = strconv.ParseUint(strings.Trim(v, " "), 10, 64)
		if err != nil {
			panic(fmt.Sprintf("Could not parse %#v: %v", raw, err))
		}
	}
	return nums
}

// The caller has to call DeleteUint64Vector.
func (self *NetworKitOptions) DegreeVector(k string) networkit.Uint64Vector {
	nums := self.Numbers(k)
	cv := networkit.NewUint64Vector()
	size := int(self.Count("size"))
	// Instead of requiring the user to type "1,2,3" hundreds of times,
	// we repeat the sequence to reach the target count ("size").
	for i := 0; i < size; i += 1 {
		cv.Add(nums[i%len(nums)])
	}
	return cv
}

func init() {
	operationRepository["NetworKitCreateGraph"] = Operation{
		execute: func(ea *EntityAccessor) (err error) {
			// Convert NetworKit exceptions to errors.
			defer func() {
				if e := recover(); e != nil {
					err = fmt.Errorf("%v", e)
					log.Printf("%v\n%v", e, string(debug.Stack()))
				}
			}()
			o := &NetworKitOptions{ea.GetMapParam("options")}
			var result networkit.Graph
			networkit.SetSeed(o.Count("seed"), true)
			networkit.SetThreadsFromEnv()
			switch ea.GetStringParam("op") {
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
			vs, es := ToSphynx(result)
			ea.output("vs", vs)
			ea.output("es", es)
			return nil
		},
	}
}

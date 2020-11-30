// All NetworKit ops that create a graph from nothing.
package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func init() {
	operationRepository["NetworKitCreateGraph"] = Operation{
		execute: func(ea *EntityAccessor) (err error) {
			// Convert NetworKit exceptions to errors.
			defer func() {
				if e := recover(); e != nil {
					err = fmt.Errorf("%v", e)
				}
			}()
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
			numbers := func(k string) []uint64 {
				raw := getOpt(k).(string)
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
			degreeVector := func(k string) networkit.Uint64Vector {
				nums := numbers(k)
				cv := networkit.NewUint64Vector()
				size := int(count("size"))
				// Instead of requiring the user to type "1,2,3" hundreds of times,
				// we repeat the sequence to reach the target count ("size").
				for i := 0; i < size; i += 1 {
					cv.Add(nums[i%len(nums)])
				}
				return cv
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
			case "StaticDegreeSequenceGenerator":
				degs := degreeVector("degrees")
				defer networkit.DeleteUint64Vector(degs)
				switch getOpt("algorithm").(string) {
				case "Chung–Lu":
					g := networkit.NewChungLuGenerator(degs)
					defer networkit.DeleteChungLuGenerator(g)
					result = g.Generate()
				case "Edge switching Markov chain":
					g := networkit.NewEdgeSwitchingMarkovChainGenerator(degs)
					defer networkit.DeleteEdgeSwitchingMarkovChainGenerator(g)
					result = g.Generate()
				case "Haveli–Hakimi":
					g := networkit.NewHavelHakimiGenerator(degs)
					defer networkit.DeleteHavelHakimiGenerator(g)
					result = g.Generate()
				}
			case "ClusteredRandomGraphGenerator":
				g := networkit.NewClusteredRandomGraphGenerator(
					count("size"),
					count("clusters"),
					double("probability_in"),
					double("probability_out"))
				defer networkit.DeleteClusteredRandomGraphGenerator(g)
				result = g.Generate()
			case "DorogovtsevMendesGenerator":
				g := networkit.NewDorogovtsevMendesGenerator(
					count("size"))
				defer networkit.DeleteDorogovtsevMendesGenerator(g)
				result = g.Generate()
			case "ErdosRenyiGenerator":
				g := networkit.NewErdosRenyiGenerator(
					count("size"),
					double("probability"),
					true, // directed
					true) // allow self-loops
				defer networkit.DeleteErdosRenyiGenerator(g)
				result = g.Generate()
			case "HyperbolicGenerator":
				g := networkit.NewHyperbolicGenerator(
					count("size"),
					double("avg_degree"),
					double("exponent"),
					double("temperature"))
				defer networkit.DeleteHyperbolicGenerator(g)
				result = g.Generate()
			case "LFRGenerator":
				g := networkit.NewLFRGenerator(count("size"))
				defer networkit.DeleteLFRGenerator(g)
				g.GeneratePowerlawDegreeSequence(
					count("avg_degree"), count("max_degree"), -double("degree_exponent"))
				g.GeneratePowerlawCommunitySizeSequence(
					count("min_community"), count("max_community"), -double("community_exponent"))
				g.SetMuWithBinomialDistribution(double("avg_mixing"))
				result = g.Generate()
			case "MocnikGenerator":
				g := networkit.NewMocnikGenerator(
					count("dimension"), count("size"), double("density"))
				defer networkit.DeleteMocnikGenerator(g)
				result = g.Generate()
			case "PubWebGenerator":
				g := networkit.NewPubWebGenerator(
					count("size"),
					count("dense_areas"),
					double("neighborhood_radius"),
					count("max_degree"))
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

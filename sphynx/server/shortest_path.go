// Implements the backend for the FindShortestPath

package main

import (
	"math"
)

func doShortestPath(
	edges *EdgeBundle,
	edgeDistance *DoubleAttribute,
	startingDistance *DoubleAttribute,
	maxIterations int) *DoubleAttribute {
	numVertices := len(startingDistance.Values)
	numEdges := len(edges.Dst)
	distance := DoubleAttribute{
		Values:  make([]float64, numVertices),
		Defined: make([]bool, numVertices),
	}

	for i := 0; i < numVertices; i++ {
		if startingDistance.Defined[i] {
			distance.Values[i] = startingDistance.Values[i]
		} else {
			distance.Values[i] = math.MaxFloat64
		}
	}

	for moreWork := true; moreWork && maxIterations > 0; maxIterations-- {
		moreWork = false
		for e := 0; e < numEdges; e++ {
			src := edges.Src[e]
			dst := edges.Dst[e]
			srcCurrentBest := distance.Values[src]
			maybeBetter := srcCurrentBest + edgeDistance.Values[e]
			if maybeBetter < distance.Values[dst] {
				distance.Values[dst] = maybeBetter
				moreWork = true
			}
		}
	}
	for i := 0; i < numVertices; i++ {
		distance.Defined[i] = distance.Values[i] != math.MaxFloat64
	}
	return &distance
}

func init() {
	operationRepository["ShortestPath"] = Operation{
		execute: func(ea *EntityAccessor) error {
			es := ea.getEdgeBundle("es")
			edgeDistance := ea.getDoubleAttribute("edgeDistance")
			startingDistance := ea.getDoubleAttribute("startingDistance")
			maxIterations := int(ea.GetFloatParam("maxIterations"))
			distance := doShortestPath(es, edgeDistance, startingDistance, maxIterations)
			ea.output("distance", distance)
			return nil
		},
	}
}

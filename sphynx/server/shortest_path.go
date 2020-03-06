// Implements the backend for the FindShortestPath

package main

import (
	"math"
)

type EdgeInfo struct {
	src VertexID
	dst VertexID
	d   float64
}

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
			distance.Values[i] = math.Inf(1)
		}
	}

	compressedEdges := make([]EdgeInfo, 0, numEdges)
	for i := 0; i < numEdges; i++ {
		if edgeDistance.Defined[i] {
			e := EdgeInfo{
				src: edges.Src[i],
				dst: edges.Dst[i],
				d:   edgeDistance.Values[i],
			}
			compressedEdges = append(compressedEdges, e)
		}
	}

	for moreWork := true; moreWork && maxIterations > 0; maxIterations-- {
		moreWork = false
		for _, e := range compressedEdges {
			src := e.src
			dst := e.dst
			srcCurrentBest := distance.Values[src]
			maybeBetter := srcCurrentBest + e.d
			if maybeBetter < distance.Values[dst] {
				distance.Values[dst] = maybeBetter
				moreWork = true
			}
		}
	}
	for i := 0; i < numVertices; i++ {
		distance.Defined[i] = distance.Values[i] != math.Inf(1)
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

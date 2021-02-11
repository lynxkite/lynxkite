// Implements the VertexSetIntersection operation

package main

import (
	"fmt"
)

type MergeVertexEntry struct {
	id    int64
	count int
}

func doVertexSetIntersection(vertexSets []*VertexSet) (intersection *VertexSet, firstEmbedding *EdgeBundle) {
	mergeVertices := make([]MergeVertexEntry, len(vertexSets[0].MappingToUnordered))
	vs0 := vertexSets[0]
	for idx, id := range vs0.MappingToUnordered {
		mergeVertices[idx].id = id
	}
	for i := 1; i < len(vertexSets); i++ {
		w := vertexSets[i].MappingToUnordered
		for j, k := 0, 0; j < len(mergeVertices) && k < len(w); {
			if mergeVertices[j].id == w[k] {
				mergeVertices[j].count++
				j++
				k++
			} else if mergeVertices[j].id < w[k] {
				j++
			} else {
				k++
			}
		}
	}
	allHaveIt := make([]int64, 0, len(vertexSets[0].MappingToUnordered))
	for _, entry := range mergeVertices {
		if entry.count == len(vertexSets)-1 {
			allHaveIt = append(allHaveIt, entry.id)
		}
	}
	intersection = &VertexSet{
		MappingToUnordered: allHaveIt,
	}

	firstEmbedding = NewEdgeBundle(len(allHaveIt), len(allHaveIt))
	for j, k := 0, 0; j < len(mergeVertices) && k < len(allHaveIt); {
		if mergeVertices[j].id == allHaveIt[k] {
			firstEmbedding.Src[k] = SphynxId(k)
			firstEmbedding.Dst[k] = SphynxId(j)
			firstEmbedding.EdgeMapping[k] = allHaveIt[k]
			j++
			k++
		} else {
			j++
		}
	}
	return
}

func init() {
	operationRepository["VertexSetIntersection"] = Operation{
		execute: func(ea *EntityAccessor) error {
			numVertexSets := int(ea.GetFloatParam("numVertexSets"))
			if numVertexSets < 1 {
				return fmt.Errorf("Cannot take intersection of %d vertexSets", numVertexSets)
			}
			vertexSets := make([]*VertexSet, numVertexSets)
			for i := 0; i < numVertexSets; i++ {
				vsName := fmt.Sprintf("vs%d", i)
				vertexSets[i] = ea.getVertexSet(vsName)
			}
			intersection, firstEmbedding := doVertexSetIntersection(vertexSets)
			ea.output("intersection", intersection)
			ea.output("firstEmbedding", firstEmbedding)
			return nil
		},
	}
}

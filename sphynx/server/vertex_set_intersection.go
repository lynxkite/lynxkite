// Implements the VertexSetIntersection operation

package main

import (
	"fmt"
	"sort"
	"sync"
)

type MergeVertexEntry struct {
	id    int64
	count int
}
type MergeVertexEntrySlice []MergeVertexEntry

func (a MergeVertexEntrySlice) Len() int {
	return len(a)
}
func (a MergeVertexEntrySlice) Less(i, j int) bool {
	return a[i].id < a[j].id
}
func (a MergeVertexEntrySlice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

type Int64Slice []int64

func (a Int64Slice) Len() int {
	return len(a)
}
func (a Int64Slice) Less(i, j int) bool {
	return a[i] < a[j]
}
func (a Int64Slice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func doVertexSetIntersection(vertexSets []*VertexSet) (intersection *VertexSet, firstEmbedding *EdgeBundle) {
	mergeVertices := make(MergeVertexEntrySlice, len(vertexSets[0].MappingToUnordered))
	vs0 := vertexSets[0]
	for idx, id := range vs0.MappingToUnordered {
		mergeVertices[idx].id = id
	}
	sort.Sort(mergeVertices)
	for i := 1; i < len(vertexSets); i++ {
		w := make([]int64, len(vertexSets[i].MappingToUnordered))
		copy(w, vertexSets[i].MappingToUnordered)
		sort.Sort(Int64Slice(w))
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
		Mutex:              sync.Mutex{},
		MappingToUnordered: allHaveIt,
	}

	firstEmbedding = NewEdgeBundle(len(allHaveIt), len(allHaveIt))
	mapping := vs0.GetMappingToOrdered()
	for idx, id := range allHaveIt {
		firstEmbedding.Src[idx] = SphynxId(idx)
		firstEmbedding.Dst[idx] = mapping[id]
		firstEmbedding.EdgeMapping[idx] = id
	}
	vs0.MappingToOrdered = nil // There's no need to keep it around
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
				fmt.Printf("Got vertex set %s, len: %d\n", vsName, len(vertexSets[i].MappingToUnordered))
			}
			intersection, firstEmbedding := doVertexSetIntersection(vertexSets)
			ea.output("intersection", intersection)
			ea.output("firstEmbedding", firstEmbedding)
			return nil
		},
	}
}

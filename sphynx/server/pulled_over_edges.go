// Implements the PulledOverEdges operation
// See the Spark implementation for details

package main

func doPulledOverEdges(
	destinationVS *VertexSet,
	originalEB *EdgeBundle,
	injection *EdgeBundle) *EdgeBundle {

	origIds := make(map[SphynxId]SphynxId, len(injection.Dst))
	for i := 0; i < len(injection.Src); i++ {
		origIds[injection.Src[i]] = injection.Dst[i]
	}
	pulledEB := NewEdgeBundle(len(injection.Src), len(injection.Src))
	j := 0
	n := SphynxId(len(destinationVS.MappingToUnordered))
	for i := SphynxId(0); i < n; i++ {
		origId, exists := origIds[i]
		if exists {
			pulledEB.Src[j] = originalEB.Src[origId]
			pulledEB.Dst[j] = originalEB.Dst[origId]
			pulledEB.EdgeMapping[j] = originalEB.EdgeMapping[origId]
			j++
		}
	}
	return pulledEB
}

func init() {
	operationRepository["PulledOverEdges"] = Operation{
		execute: func(ea *EntityAccessor) error {
			destinationVS := ea.getVertexSet("destinationVS")
			originalEB := ea.getEdgeBundle("originalEB")
			injection := ea.getEdgeBundle("injection")
			pulledEB :=
				doPulledOverEdges(destinationVS, originalEB, injection)
			ea.output("pulledEB", pulledEB)
			return nil
		},
	}
}

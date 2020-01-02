// Implements the PulledOverVertexAttribute operation

package main

var _ = registerOperation("PulledOverVertexAttribute.Dummy", Operation{
	execute: func(ea *EntityAccessor) error {
		originalVS := ea.getVertexSet("originalVS")
		destinationVS := ea.getVertexSet("destinationVS")
		function := ea.getEdgeBundle("function")
		attr := ea.getAttr("originalAttr")
		err := ea.getError()
		if err != nil {
			return err
		}
		pair := attr.twins()
		edgeMapping := make(map[int64]int64, len(function.Src))
		for i, _ := range function.Src {
			edgeMapping[function.Src[i]] = function.Dst[i]
		}
		vertexMapping := make(map[int64]int64, len(originalVS.Mapping))
		for i, k := range originalVS.Mapping {
			vertexMapping[k] = int64(i)
		}
		for i, k := range destinationVS.Mapping {
			a := edgeMapping[k]
			b := vertexMapping[a]
			srcIdx := b
			dstIdx := int64(i)
			pair.copier(srcIdx, dstIdx)
		}
		ea.add("pulledAttr", pair.created.entity())
		return nil
	},
})

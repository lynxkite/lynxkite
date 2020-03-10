// Implements the InducedEdgeBundle operation
// See the Spark implementation for details

package main

func init() {
	operationRepository["InducedEdgeBundle"] = Operation{
		execute: func(ea *EntityAccessor) error {
			induceSrc := ea.GetBoolParam("induceSrc")
			induceDst := ea.GetBoolParam("induceDst")
			var srcMapping map[VERTEX_ID]VERTEX_ID
			if induceSrc {
				srcMappingEB := ea.getEdgeBundle("srcMapping")
				srcMapping = make(map[VERTEX_ID]VERTEX_ID, len(srcMappingEB.Src))
				for i := range srcMappingEB.Src {
					srcMapping[srcMappingEB.Src[i]] = srcMappingEB.Dst[i]
				}
			}
			var dstMapping map[VERTEX_ID]VERTEX_ID
			if induceDst {
				dstMappingEB := ea.getEdgeBundle("dstMapping")
				dstMapping = make(map[VERTEX_ID]VERTEX_ID, len(dstMappingEB.Src))
				for i := range dstMappingEB.Src {
					dstMapping[dstMappingEB.Src[i]] = dstMappingEB.Dst[i]
				}
			}
			es := ea.getEdgeBundle("edges")
			approxLen := len(es.Src)
			induced := NewEdgeBundle(0, approxLen)
			embedding := NewEdgeBundle(0, approxLen)
			numInducedEdges := VERTEX_ID(0)
			for i, src := range es.Src {
				dst := es.Dst[i]
				mappedSrc := src
				mappedDst := dst
				srcExists := true
				dstExists := true
				if induceSrc {
					mappedSrc, srcExists = srcMapping[src]
				}
				if induceDst {
					mappedDst, dstExists = dstMapping[dst]
				}
				if srcExists && dstExists {
					induced.Src = append(induced.Src, mappedSrc)
					induced.Dst = append(induced.Dst, mappedDst)
					induced.EdgeMapping = append(induced.EdgeMapping, es.EdgeMapping[i])
					embedding.Src = append(embedding.Src, numInducedEdges)
					embedding.Dst = append(embedding.Dst, VERTEX_ID(i))
					embedding.EdgeMapping = append(embedding.EdgeMapping, es.EdgeMapping[i])
					numInducedEdges += 1
				}
			}
			ea.output("induced", induced)
			ea.output("embedding", embedding)
			return nil
		},
	}
}

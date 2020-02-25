package main

func init() {
	operationRepository["InducedEdgeBundle"] = Operation{
		execute: func(ea *EntityAccessor) error {
			induceSrc := ea.GetBoolParam("induceSrc", true)
			induceDst := ea.GetBoolParam("induceDst", true)
			var srcMapping map[int]int
			if induceSrc {
				srcMappingEB := ea.getEdgeBundle("srcMapping")
				srcMapping = make(map[int]int, len(srcMappingEB.Src))
				for i := range srcMappingEB.Src {
					srcMapping[srcMappingEB.Src[i]] = srcMappingEB.Dst[i]
				}
			}
			var dstMapping map[int]int
			if induceDst {
				dstMappingEB := ea.getEdgeBundle("dstMapping")
				dstMapping = make(map[int]int, len(dstMappingEB.Src))
				for i := range dstMappingEB.Src {
					dstMapping[dstMappingEB.Src[i]] = dstMappingEB.Dst[i]
				}
			}
			es := ea.getEdgeBundle("edges")
			induced := &EdgeBundle{}
			embedding := &EdgeBundle{}
			approxLen := len(es.Src)
			induced.Init(approxLen)
			embedding.Init(approxLen)
			numInducedEdges := 0
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
					induced.EdgeMapping = append(induced.EdgeMapping, int64(numInducedEdges))
					embedding.Src = append(embedding.Src, i)
					embedding.Dst = append(embedding.Dst, numInducedEdges)
					embedding.EdgeMapping = append(embedding.EdgeMapping, int64(numInducedEdges))
					numInducedEdges += 1
				}
			}
			ea.output("induced", induced)
			ea.output("embedding", embedding)
			return nil
		},
	}
}

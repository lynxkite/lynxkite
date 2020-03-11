// Implements the InducedEdgeBundle operation
// See the Spark implementation for details

package main

func init() {
	operationRepository["InducedEdgeBundle"] = Operation{
		execute: func(ea *EntityAccessor) error {
			induceSrc := ea.GetBoolParam("induceSrc")
			induceDst := ea.GetBoolParam("induceDst")
			var srcMapping [][]SphynxId
			if induceSrc {
				srcMappingEB := ea.getEdgeBundle("srcMapping")
				numVS := len(ea.getVertexSet("src").MappingToUnordered)
				srcMapping = make([][]SphynxId, numVS)
				for i := range srcMappingEB.Src {
					orig := srcMappingEB.Src[i]
					srcMapping[orig] = append(srcMapping[orig], srcMappingEB.Dst[i])
				}
			}
			var dstMapping [][]SphynxId
			if induceDst {
				dstMappingEB := ea.getEdgeBundle("dstMapping")
				numVS := len(ea.getVertexSet("dst").MappingToUnordered)
				dstMapping = make([][]SphynxId, numVS)
				for i := range dstMappingEB.Src {
					orig := dstMappingEB.Src[i]
					dstMapping[orig] = append(dstMapping[orig], dstMappingEB.Dst[i])
				}
			}
			es := ea.getEdgeBundle("edges")
			approxLen := len(es.Src)
			induced := NewEdgeBundle(0, approxLen)
			embedding := NewEdgeBundle(0, approxLen)
			numInducedEdges := SphynxId(0)
			// We try to keep the IDs of the edges, so the embedding edge bundle is IdPreserving
			// if it's expected to be. If the srcMapping or the dstMapping is not a function, then
			// this won't work, we need new IDs.
			newIDsNeeded := false
			for i, src := range es.Src {
				dst := es.Dst[i]
				mappedSrcs := []SphynxId{src}
				mappedDsts := []SphynxId{dst}
				thisSrcExists := true
				thisDstExists := true
				if induceSrc {
					mappedSrcs = srcMapping[src]
					thisSrcExists = len(mappedSrcs) > 0
				}
				if induceDst {
					mappedDsts = dstMapping[dst]
					thisDstExists = len(mappedDsts) > 0
				}
				if thisSrcExists && thisDstExists {
					for j, mappedSrc := range mappedSrcs {
						for k, mappedDst := range mappedDsts {
							if j != 0 || k != 0 {
								newIDsNeeded = true
							}
							induced.Src = append(induced.Src, mappedSrc)
							induced.Dst = append(induced.Dst, mappedDst)
							induced.EdgeMapping = append(induced.EdgeMapping, es.EdgeMapping[i])
							embedding.Src = append(embedding.Src, numInducedEdges)
							embedding.Dst = append(embedding.Dst, SphynxId(i))
							embedding.EdgeMapping = append(embedding.EdgeMapping, es.EdgeMapping[i])
							numInducedEdges += 1
						}
					}
				}
			}
			if newIDsNeeded {
				for i := range induced.EdgeMapping {
					induced.EdgeMapping[i] = int64(i)
					embedding.EdgeMapping[i] = int64(i)
				}
			}
			ea.output("induced", induced)
			ea.output("embedding", embedding)
			return nil
		},
	}
}

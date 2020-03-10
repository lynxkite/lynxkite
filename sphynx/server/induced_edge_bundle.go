// Implements the InducedEdgeBundle operation
// See the Spark implementation for details

package main

func init() {
	operationRepository["InducedEdgeBundle"] = Operation{
		execute: func(ea *EntityAccessor) error {
			induceSrc := ea.GetBoolParam("induceSrc")
			induceDst := ea.GetBoolParam("induceDst")
			var srcMapping [][]int
			var srcExists []bool
			if induceSrc {
				srcMappingEB := ea.getEdgeBundle("srcMapping")
				numVS := len(ea.getVertexSet("src").MappingToUnordered)
				srcExists = make([]bool, numVS)
				srcMapping = make([][]int, numVS)
				for i := range srcMappingEB.Src {
					orig := srcMappingEB.Src[i]
					srcMapping[orig] = append(srcMapping[orig], srcMappingEB.Dst[i])
					srcExists[orig] = true
				}
			}
			var dstMapping [][]int
			var dstExists []bool
			if induceDst {
				dstMappingEB := ea.getEdgeBundle("dstMapping")
				numVS := len(ea.getVertexSet("dst").MappingToUnordered)
				dstExists = make([]bool, numVS)
				dstMapping = make([][]int, numVS)
				for i := range dstMappingEB.Src {
					orig := dstMappingEB.Src[i]
					dstMapping[orig] = append(dstMapping[orig], dstMappingEB.Dst[i])
					dstExists[orig] = true
				}
			}
			es := ea.getEdgeBundle("edges")
			induced := &EdgeBundle{}
			embedding := &EdgeBundle{}
			approxLen := len(es.Src)
			induced.Make(0, approxLen)
			embedding.Make(0, approxLen)
			numInducedEdges := 0
			newIndicesNeeded := false
			for i, src := range es.Src {
				dst := es.Dst[i]
				mappedSrcs := []int{src}
				mappedDsts := []int{dst}
				thisSrcExists := true
				thisDstExists := true
				if induceSrc {
					thisSrcExists = srcExists[src]
					if thisSrcExists {
						mappedSrcs = srcMapping[src]
					}
				}
				if induceDst {
					thisDstExists = dstExists[dst]
					if thisDstExists {
						mappedDsts = dstMapping[dst]
					}
				}
				if thisSrcExists && thisDstExists {
					for j, mappedSrc := range mappedSrcs {
						for k, mappedDst := range mappedDsts {
							if j != 0 || k != 0 {
								newIndicesNeeded = true
							}
							induced.Src = append(induced.Src, mappedSrc)
							induced.Dst = append(induced.Dst, mappedDst)
							induced.EdgeMapping = append(induced.EdgeMapping, es.EdgeMapping[i])
							embedding.Src = append(embedding.Src, numInducedEdges)
							embedding.Dst = append(embedding.Dst, i)
							embedding.EdgeMapping = append(embedding.EdgeMapping, es.EdgeMapping[i])
							numInducedEdges += 1
						}
					}
				}
			}
			if newIndicesNeeded {
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

// Implements the PulledOverVertexAttribute operation

package main

import "fmt"

func init() {
	operationRepository["PulledOverVertexAttribute.Dummy"] = Operation{
		execute: func(ea *EntityAccessor) error {
			originalVS := ea.getVertexSet("originalVS")
			destinationVS := ea.getVertexSet("destinationVS")
			function := ea.getEdgeBundle("function")
			attributeEntity := ea.inputs["originalAttr"]

			dstIdToSrcId := make(map[int64]int64, len(function.Src))
			for i, _ := range function.Src {
				dstIdToSrcId[function.Src[i]] = function.Dst[i]
			}
			srcIdToIndex := make(map[int64]int, len(originalVS.Mapping))
			for i, k := range originalVS.Mapping {
				srcIdToIndex[k] = i
			}
			switch attr := attributeEntity.(type) {
			case *DoubleAttribute:
				dst := DoubleAttribute{
					make([]float64, len(destinationVS.Mapping)),
					make([]bool, len(destinationVS.Mapping)),
				}
				for dstIdx, dstId := range destinationVS.Mapping {
					srcId := dstIdToSrcId[dstId]
					srcIdx := srcIdToIndex[srcId]
					dst.Values[dstIdx] = attr.Values[srcIdx]
					dst.Defined[dstIdx] = true
				}
				ea.output("pulledAttr", &dst)
			case *StringAttribute:
				dst := StringAttribute{
					make([]string, len(destinationVS.Mapping)),
					make([]bool, len(destinationVS.Mapping)),
				}
				for dstIdx, dstId := range destinationVS.Mapping {
					srcId := dstIdToSrcId[dstId]
					srcIdx := srcIdToIndex[srcId]
					dst.Values[dstIdx] = attr.Values[srcIdx]
					dst.Defined[dstIdx] = true
				}
				ea.output("pulledAttr", &dst)
			case *DoubleTuple2Attribute:
				dst := DoubleTuple2Attribute{
					make([]float64, len(destinationVS.Mapping)),
					make([]float64, len(destinationVS.Mapping)),
					make([]bool, len(destinationVS.Mapping)),
				}
				for dstIdx, dstId := range destinationVS.Mapping {
					srcId := dstIdToSrcId[dstId]
					srcIdx := srcIdToIndex[srcId]
					dst.Values1[dstIdx] = attr.Values1[srcIdx]
					dst.Values2[dstIdx] = attr.Values2[srcIdx]
					dst.Defined[dstIdx] = true
				}
				ea.output("pulledAttr", &dst)
			default:
				return fmt.Errorf("Not attribute type: %v", attr)
			}
			return nil
		},
	}
}

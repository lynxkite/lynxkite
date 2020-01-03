// Implements the PulledOverVertexAttribute operation

package main

import "fmt"

func init() {
	operationRepository["PulledOverVertexAttribute.Dummy"] = Operation{
		execute: func(ea *EntityAccessor) error {
			originalVS := ea.getVertexSet("originalVS")
			destinationVS := ea.getVertexSet("destinationVS")
			function := ea.getEdgeBundle("function")
			attributeEntity := ea.getEntity("originalAttr")

			edgeMapping := make(map[int64]int64, len(function.Src))
			for i, _ := range function.Src {
				edgeMapping[function.Src[i]] = function.Dst[i]
			}
			vertexMapping := make(map[int64]int64, len(originalVS.Mapping))
			for i, k := range originalVS.Mapping {
				vertexMapping[k] = int64(i)
			}
			switch attr := attributeEntity.(type) {
			case *DoubleAttribute:
				dst := DoubleAttribute{
					make([]float64, len(destinationVS.Mapping)),
					make([]bool, len(destinationVS.Mapping)),
				}
				for i, k := range destinationVS.Mapping {
					a := edgeMapping[k]
					b := vertexMapping[a]
					srcIdx := b
					dstIdx := int64(i)
					dst.Values[dstIdx] = attr.Values[srcIdx]
					dst.Defined[dstIdx] = true
				}
				ea.output("pulledAttr", &dst)
			case *StringAttribute:
				dst := StringAttribute{
					make([]string, len(destinationVS.Mapping)),
					make([]bool, len(destinationVS.Mapping)),
				}
				for i, k := range destinationVS.Mapping {
					a := edgeMapping[k]
					b := vertexMapping[a]
					srcIdx := b
					dstIdx := int64(i)
					dst.Values[dstIdx] = attr.Values[srcIdx]
					dst.Defined[dstIdx] = true
				}
			case *DoubleTuple2Attribute:
				dst := DoubleTuple2Attribute{
					make([]float64, len(destinationVS.Mapping)),
					make([]float64, len(destinationVS.Mapping)),
					make([]bool, len(destinationVS.Mapping)),
				}
				for i, k := range destinationVS.Mapping {
					a := edgeMapping[k]
					b := vertexMapping[a]
					srcIdx := b
					dstIdx := int64(i)
					dst.Values1[dstIdx] = attr.Values1[srcIdx]
					dst.Values2[dstIdx] = attr.Values2[srcIdx]
					dst.Defined[dstIdx] = true
				}
			default:
				return fmt.Errorf("Not attribute type: %v", attr)
			}
			return nil
		},
	}
}

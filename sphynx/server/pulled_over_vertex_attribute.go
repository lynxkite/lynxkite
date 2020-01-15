// Implements the PulledOverVertexAttribute operation

package main

import "fmt"

func init() {
	operationRepository["PulledOverVertexAttribute"] = Operation{
		execute: func(ea *EntityAccessor) error {
			destinationVS := ea.getVertexSet("destinationVS")
			function := ea.getEdgeBundle("function")
			attributeEntity := ea.inputs["originalAttr"]

			destToOrig := make(map[int]int, len(function.Src))
			for i := range function.Src {
				destToOrig[function.Src[i]] = function.Dst[i]
			}
			switch origAttr := attributeEntity.(type) {
			case *DoubleAttribute:
				destAttr := DoubleAttribute{
					make([]float64, len(destinationVS.MappingToUnordered)),
					make([]bool, len(destinationVS.MappingToUnordered)),
				}
				for destId := range destinationVS.MappingToUnordered {
					origId := destToOrig[destId]
					destAttr.Values[destId] = origAttr.Values[origId]
					destAttr.Defined[destId] = true
				}
				ea.output("pulledAttr", &destAttr)
			case *StringAttribute:
				destAttr := StringAttribute{
					make([]string, len(destinationVS.MappingToUnordered)),
					make([]bool, len(destinationVS.MappingToUnordered)),
				}
				for destId := range destinationVS.MappingToUnordered {
					origId := destToOrig[destId]
					destAttr.Values[destId] = origAttr.Values[origId]
					destAttr.Defined[destId] = origAttr.Defined[origId]
				}
				ea.output("pulledAttr", &destAttr)
			case *DoubleTuple2Attribute:
				destAttr := DoubleTuple2Attribute{
					make([]float64, len(destinationVS.MappingToUnordered)),
					make([]float64, len(destinationVS.MappingToUnordered)),
					make([]bool, len(destinationVS.MappingToUnordered)),
				}
				for destId := range destinationVS.MappingToUnordered {
					origId := destToOrig[destId]
					destAttr.Values1[destId] = origAttr.Values1[origId]
					destAttr.Values2[destId] = origAttr.Values2[origId]
					destAttr.Defined[destId] = true
				}
				ea.output("pulledAttr", &destAttr)
			default:
				return fmt.Errorf("Not attribute type: %v", origAttr)
			}
			return nil
		},
	}
}

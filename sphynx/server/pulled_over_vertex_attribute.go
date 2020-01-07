// Implements the PulledOverVertexAttribute operation

package main

import "fmt"

func init() {
	operationRepository["PulledOverVertexAttribute"] = Operation{
		execute: func(ea *EntityAccessor) error {
			destinationVS := ea.getVertexSet("destinationVS")
			function := ea.getEdgeBundle("function")
			attributeEntity := ea.inputs["originalAttr"]

			destIdToOrigId := make(map[int]int, len(function.Src))
			for i := range function.Src {
				destIdToOrigId[function.Src[i]] = function.Dst[i]
			}
			switch origAttr := attributeEntity.(type) {
			case *DoubleAttribute:
				destAttr := DoubleAttribute{
					make([]float64, len(destinationVS.MappingToUnordered)),
					make([]bool, len(destinationVS.MappingToUnordered)),
				}
				for destId := range destinationVS.MappingToUnordered {
					origId := destIdToOrigId[destId]
					destAttr.Values[destId] = origAttr.Values[origId]
					destAttr.Defined[destId] = true
				}
				ea.output("pulledAttr", &destAttr)
			case *StringAttribute:
				destAttr := StringAttribute{
					make([]string, len(destinationVS.MappingToUnordered)),
					make([]bool, len(destinationVS.MappingToUnordered)),
				}
				fmt.Println("destinationVS")
				fmt.Println(destinationVS)
				fmt.Println("function")
				fmt.Println(function.Src)
				fmt.Println(function.Dst)
				fmt.Println("origAttr")
				fmt.Println(origAttr)
				for destId := range destinationVS.MappingToUnordered {
					origId := destIdToOrigId[destId]
					destAttr.Values[destId] = origAttr.Values[origId]
					destAttr.Defined[destId] = origAttr.Defined[origId]
					fmt.Println("destAttr")
					fmt.Println(destAttr)
				}
				ea.output("pulledAttr", &destAttr)
			case *DoubleTuple2Attribute:
				destAttr := DoubleTuple2Attribute{
					make([]float64, len(destinationVS.MappingToUnordered)),
					make([]float64, len(destinationVS.MappingToUnordered)),
					make([]bool, len(destinationVS.MappingToUnordered)),
				}
				for destId := range destinationVS.MappingToUnordered {
					origId := destIdToOrigId[destId]
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

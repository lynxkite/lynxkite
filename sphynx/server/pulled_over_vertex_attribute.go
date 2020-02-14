// Implements the PulledOverVertexAttribute operation

package main

import (
	"reflect"
)

func init() {
	operationRepository["PulledOverVertexAttribute"] = Operation{
		execute: func(ea *EntityAccessor) error {
			origAttr := ea.inputs["originalAttr"].(ParquetEntity)
			origValues := reflect.ValueOf(origAttr).Elem().FieldByName("Values")
			origDefined := reflect.ValueOf(origAttr).Elem().FieldByName("Defined")
			destinationVS := ea.getVertexSet("destinationVS")
			function := ea.getEdgeBundle("function")
			destToOrig := make(map[int]int, len(function.Src))
			for i := range function.Src {
				destToOrig[function.Src[i]] = function.Dst[i]
			}
			numVS := len(destinationVS.MappingToUnordered)
			attrType := reflect.Indirect(reflect.ValueOf(origAttr)).Type()
			destAttr := reflect.New(attrType)
			InitializeAttribute(destAttr, numVS)
			destValues := destAttr.Elem().FieldByName("Values")
			destDefined := destAttr.Elem().FieldByName("Defined")
			for destId := range destinationVS.MappingToUnordered {
				origId := destToOrig[destId]
				value := origValues.Index(origId)
				defined := origDefined.Index(origId)
				destValues.Index(destId).Set(value)
				destDefined.Index(destId).Set(defined)
			}
			destAttrValue := destAttr.Interface()
			ea.output("pulledAttr", destAttrValue.(Entity))
			return nil
		},
	}
}

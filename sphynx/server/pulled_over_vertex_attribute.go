// Implements the PulledOverVertexAttribute operation

package main

import (
	"reflect"
)

func init() {
	operationRepository["PulledOverVertexAttribute"] = Operation{
		execute: func(ea *EntityAccessor) error {
			destinationVS := ea.getVertexSet("destinationVS")
			function := ea.getEdgeBundle("function")
			destToOrig := make(map[int]int, len(function.Src))
			for i := range function.Src {
				destToOrig[function.Src[i]] = function.Dst[i]
			}
			numVS := len(destinationVS.MappingToUnordered)
			origAttr := ea.inputs["originalAttr"]
			origValues := reflect.ValueOf(origAttr).Elem().FieldByName("Values")
			attrType := reflect.Indirect(reflect.ValueOf(origAttr)).Type()
			destAttr := reflect.New(attrType)
			destValues := destAttr.Elem().FieldByName("Values")
			newValues := reflect.MakeSlice(destValues.Type(), numVS, numVS)
			destValues.Set(newValues)
			destDefined := destAttr.Elem().FieldByName("Defined")
			newDefined := reflect.MakeSlice(destDefined.Type(), numVS, numVS)
			destDefined.Set(newDefined)
			for destId := range destinationVS.MappingToUnordered {
				origId := destToOrig[destId]
				value := origValues.Index(origId)
				destValues.Index(destId).Set(value)
				destDefined.Index(destId).SetBool(true)
			}
			destAttrValue := destAttr.Interface()
			ea.output("pulledAttr", destAttrValue.(Entity))
			return nil
		},
	}
}

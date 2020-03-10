// Implements the PulledOverVertexAttribute operation
// See the Spark implementation for details

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
			destToOrig := make(map[VERTEX_ID]VERTEX_ID, len(function.Src))
			for i := range function.Src {
				destToOrig[function.Src[i]] = function.Dst[i]
			}
			numVS := len(destinationVS.MappingToUnordered)
			attrType := reflect.Indirect(reflect.ValueOf(origAttr)).Type()
			destAttr := reflect.New(attrType)
			InitializeAttribute(destAttr, numVS)
			destValues := destAttr.Elem().FieldByName("Values")
			destDefined := destAttr.Elem().FieldByName("Defined")
			for destId, origId := range destToOrig {
				value := origValues.Index(int(origId))
				defined := origDefined.Index(int(origId))
				if defined.Bool() {
					destValues.Index(int(destId)).Set(value)
					destDefined.Index(int(destId)).Set(defined)
				}
			}
			destAttrValue := destAttr.Interface()
			ea.output("pulledAttr", destAttrValue.(Entity))
			return nil
		},
	}
}

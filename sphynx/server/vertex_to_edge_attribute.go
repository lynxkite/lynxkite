// Pushes a vertex attribute to edges according to a mapping.
package main

import (
	"reflect"
)

func init() {
	operationRepository["VertexToEdgeAttribute"] = Operation{
		execute: func(ea *EntityAccessor) error {
			origAttr := ea.inputs["original"].(TabularEntity)
			origValues := reflect.ValueOf(origAttr).Elem().FieldByName("Values")
			origDefined := reflect.ValueOf(origAttr).Elem().FieldByName("Defined")
			mapping := ea.getSparkIDVectorAttribute("mapping")
			target := ea.getEdgeBundle("target")
			numEdges := len(target.EdgeMapping)
			attrType := reflect.Indirect(reflect.ValueOf(origAttr)).Type()
			edgeAttr := reflect.New(attrType)
			InitializeAttribute(edgeAttr, numEdges)
			edgeAttrValues := edgeAttr.Elem().FieldByName("Values")
			edgeAttrDefined := edgeAttr.Elem().FieldByName("Defined")
			idMapping := make(map[int64]int, numEdges)
			for sphynxID, sparkID := range target.EdgeMapping {
				idMapping[sparkID] = sphynxID
			}
			for vertexID, edgeIDs := range mapping.Values {
				value := origValues.Index(int(vertexID))
				defined := origDefined.Index(int(vertexID))
				if defined.Bool() {
					for _, edgeID := range edgeIDs {
						sphynxID := idMapping[edgeID]
						edgeAttrValues.Index(sphynxID).Set(value)
						edgeAttrDefined.Index(sphynxID).Set(defined)
					}
				}
			}
			edgeAttrValue := edgeAttr.Interface()
			ea.output("mappedAttribute", edgeAttrValue.(Entity))
			return nil
		},
	}
}

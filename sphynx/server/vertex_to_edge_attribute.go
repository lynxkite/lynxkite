//

package main

import "fmt"

func init() {
	operationRepository["VertexToEdgeAttribute"] = Operation{
		execute: func(ea *EntityAccessor) error {
			// targetSrc := ea.getVertexSet("ignoredSrc")
			// targetDst := ea.getVertexSet("ignoredDst")
			// origAttr := ea.inputs["originalAttr"].(TabularEntity)
			mapping := ea.getSparkIDVectorAttribute("mapping")
			// target := ea.getEdgeBundle("target")
			fmt.Println("aaaaaaaaaaaaaaaaa")
			fmt.Println(mapping)
			// ea.output("mappedAttribute", mappedAttribute)
			return nil
		},
	}
}

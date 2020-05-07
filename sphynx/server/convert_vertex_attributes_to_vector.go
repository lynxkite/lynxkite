// Bundles the chosen Double attributes into one Vector attribute.

package main

import (
	"fmt"
)

func init() {
	operationRepository["ConvertVertexAttributesToVector"] = Operation{
		execute: func(ea *EntityAccessor) error {
			numElements := int(ea.GetFloatParam("numElements"))
			vs := ea.getVertexSet("vs")
			size := len(vs.MappingToUnordered)
			defined := make([]bool, size, size)
			values := make([]DoubleVectorAttributeValue, size, size)
			for i := 0; i < size; i++ {
				defined[i] = true
				values[i] = make(DoubleVectorAttributeValue, numElements, numElements)
			}
			for i := 0; i < numElements; i++ {
				doubleAttr := ea.getDoubleAttribute(fmt.Sprintf("element-%v", i))
				for j := 0; j < size; j++ {
					defined[j] = doubleAttr.Defined[j] && defined[j]
					if defined[j] {
						values[j][i] = doubleAttr.Values[j]
					}
				}
			}
			vectorAttr := &DoubleVectorAttribute{
				Values:  values,
				Defined: defined,
			}
			ea.output("vectorAttr", vectorAttr)
			return nil
		},
	}
}

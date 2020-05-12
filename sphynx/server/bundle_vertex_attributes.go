// Bundles the chosen Double attributes into one Vector attribute.

package main

import (
	"fmt"
)

func init() {
	operationRepository["BundleVertexAttributesIntoVector"] = Operation{
		execute: func(ea *EntityAccessor) error {
			vs := ea.getVertexSet("vs")
			size := len(vs.MappingToUnordered)
			defined := make([]bool, size)
			values := make([]DoubleVectorAttributeValue, size)

			numDoubleElements := int(ea.GetFloatParam("numDoubleElements"))
			for i := 0; i < size; i++ {
				defined[i] = true
				values[i] = make(DoubleVectorAttributeValue, numDoubleElements)
			}
			for i := 0; i < numDoubleElements; i++ {
				attr := ea.getDoubleAttribute(fmt.Sprintf("doubleElement-%v", i))
				for j := 0; j < size; j++ {
					defined[j] = attr.Defined[j] && defined[j]
					if defined[j] {
						values[j][i] = attr.Values[j]
					}
				}
			}

			numVectorElements := int(ea.GetFloatParam("numVectorElements"))
			for i := 0; i < numVectorElements; i++ {
				attr := ea.getDoubleVectorAttribute(fmt.Sprintf("vectorElement-%v", i))
				for j := 0; j < size; j++ {
					if defined[j] {
						if attr.Defined[j] {
							values[j] = append(values[j], attr.Values[j]...)
						} else {
							defined[j] = false
							values[j] = nil
						}
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

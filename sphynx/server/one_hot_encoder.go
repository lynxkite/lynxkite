package main

import (
	"strings"
)

func init() {
	operationRepository["OneHotEncoder"] = Operation{
		execute: func(ea *EntityAccessor) error {
			catAttr := ea.getStringAttribute("catAttr")
			defined := catAttr.Defined
			categories := strings.Split(ea.GetStringParam("categories"), ",")
			ids := make(map[string]int)
			for i, cat := range categories {
				ids[cat] = i
			}
			size := len(defined)
			values := make([]DoubleVectorAttributeValue, size)
			for i, value := range catAttr.Values {
				if defined[i] {
					id, exists := ids[value]
					if exists {
						oneHot := make([]float64, len(categories))
						oneHot[id] = 1
						values[i] = oneHot
					}
				}
			}
			oneHotVector := &DoubleVectorAttribute{
				Values:  values,
				Defined: defined,
			}
			ea.output("oneHotVector", oneHotVector)
			return nil
		},
	}
}

package main

func init() {
	operationRepository["ApplyOneHotEncoder"] = Operation{
		execute: func(ea *EntityAccessor) error {
			catAttr := ea.getStringAttribute("catAttr")
			defined := catAttr.Defined
			ids := make(map[string]int)
			counter := 0
			for _, value := range catAttr.Values {
				_, seen := ids[value]
				if !seen {
					ids[value] = counter
					counter += 1
				}
			}
			size := len(defined)
			values := make([]DoubleVectorAttributeValue, size)
			for i := 0; i < size; i++ {
				values[i] = make(DoubleVectorAttributeValue, counter)
			}
			for i, value := range catAttr.Values {
				oneHot := make([]float64, counter)
				if defined[i] {
					oneHot[ids[value]] = 1
				}
				values[i] = oneHot
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

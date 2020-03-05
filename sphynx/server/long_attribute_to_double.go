// Implements the LongAttributeToDouble operation
// See the Spark implementation for details

package main

func init() {
	operationRepository["LongAttributeToDouble"] = Operation{
		execute: func(ea *EntityAccessor) error {
			inputAttr := ea.getLongAttribute("attr")
			outputAttr := &DoubleAttribute{
				Values:  make([]float64, len(inputAttr.Values)),
				Defined: make([]bool, len(inputAttr.Values)),
			}
			for i := 0; i < len(inputAttr.Values); i++ {
				if inputAttr.Defined[i] {
					outputAttr.Values[i] = float64(inputAttr.Values[i])
					outputAttr.Defined[i] = true
				}
			}
			ea.output("attr", outputAttr)
			return nil
		},
	}
}

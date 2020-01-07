package main

import (
	"fmt"
)

func init() {
	operationRepository["Dapcstp"] = Operation{
		execute: func(ea *EntityAccessor) error {
			gain := ea.getDoubleAttribute("gain")
			apcost := ea.getDoubleAttribute("apcost")
			ap := ea.getDoubleAttribute("ap")
			cost := ea.getDoubleAttribute("cost")
			fmt.Printf("gain: %v\napcost: %v\nap: %v\ncost: %v\n", gain, apcost, ap, cost)
			o := &DoubleAttribute{
				Defined: make([]bool, len(cost.Values)),
				Values:  make([]float64, len(cost.Values)),
			}
			for i := range o.Values {
				o.Values[i] = float64(i % 2)
				o.Defined[i] = true
			}
			ea.output("path", o)
			return nil
		},
	}
}

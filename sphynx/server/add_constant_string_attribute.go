package main

func init() {
	operationRepository["AddConstantStringAttribute"] = Operation{
		execute: func(ea *EntityAccessor) error {
			vs := ea.getVertexSet("vs")
			l := len(vs.Mapping)
			sa := &StringAttribute{
				Values:  make([]string, l, l),
				Defined: make([]bool, l, l),
			}
			ea.output("attr", sa)
			return nil
		},
	}
}

package main

type Operation struct {
	execute func(*Server, OperationInstance)
}

var operations = map[string]Operation{"GetBetter": getBetter}

var getBetter = Operation{
	execute: func(s *Server, opInst OperationInstance) {
		s.Lock()
		defer s.Unlock()
		s.scalars[opInst.Outputs["result"]] = "better"
	},
}

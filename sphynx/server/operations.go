// Implementations of Sphynx operations.

package main

type Operation struct {
	execute func(*Server, OperationInstance)
}

var operations = map[string]Operation{"ExampleGraph": exampleGraph}

var exampleGraph = Operation{
	execute: func(s *Server, opInst OperationInstance) {
		s.Lock()
		defer s.Unlock()
		s.scalars[opInst.Outputs["greeting"]] = "Hello world! 😀 "
	},
}

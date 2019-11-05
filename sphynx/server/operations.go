package main

func (s *Server) getBetter(opInst OperationInstance) {
	s.scalarsMu.Lock()
	defer s.scalarsMu.Unlock()
	s.scalars[opInst.Outputs["result"]] = "better"
}

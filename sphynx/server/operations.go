package main

func (s *Server) getBetter(opInst OperationInstance) {
	s.Lock()
	defer s.Unlock()
	s.scalars[opInst.Outputs["result"]] = "better"
}

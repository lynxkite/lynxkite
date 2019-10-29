package main

func (s *server) getBetter(opInst operationInstance) {
	s.scalarsMu.Lock()
	defer s.scalarsMu.Unlock()
	s.scalars[opInst.Outputs["result"]] = "better"
}

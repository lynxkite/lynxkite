package main

import "sync"

type ScalarValue interface{}
type Server struct {
	scalars map[GUID]ScalarValue
	*sync.Mutex
}
type GUID string
type OperationDescription struct {
	Class string
	Data  interface{}
}
type OperationInstance struct {
	GUID      GUID
	Inputs    map[string]GUID
	Outputs   map[string]GUID
	Operation OperationDescription
}

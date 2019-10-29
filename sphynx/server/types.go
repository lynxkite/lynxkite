package main

import "sync"

type scalarValue interface{}
type server struct {
	scalars   map[guid]scalarValue
	scalarsMu sync.Mutex
}
type guid string
type operationDescription struct {
	Class string
	Data  interface{}
}
type operationInstance struct {
	GUID      guid
	Inputs    map[string]guid
	Outputs   map[string]guid
	Operation operationDescription
}

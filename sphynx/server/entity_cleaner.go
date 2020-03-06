package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"unsafe"
)

func (e *Scalar) estimatedMemUsage() int {
	return len(*e)
}

func (e *VertexSet) estimatedMapMemUsage() int {
	return len(e.MappingToOrdered) * 40
}

func (e *VertexSet) estimatedMemUsage() int {
	i := len(e.MappingToUnordered) * 8
	i += e.estimatedMapMemUsage()
	return i
}

func (e *EdgeBundle) estimatedMemUsage() int {
	sizeOfVertexID := int(unsafe.Sizeof(VertexID(0)))
	i := len(e.EdgeMapping) * 8
	i += len(e.Src) * sizeOfVertexID
	i += len(e.Dst) * sizeOfVertexID
	return i
}

func (e *DoubleAttribute) estimatedMemUsage() int {
	i := len(e.Defined)
	i += len(e.Values) * 8
	return i
}

func (e *StringAttribute) estimatedMemUsage() int {
	i := len(e.Defined)
	i += len(e.Values) * (8 + 10)
	return i
}

func (e *LongAttribute) estimatedMemUsage() int {
	i := len(e.Defined)
	i += len(e.Values) * 8
	return i
}

func (e *DoubleTuple2Attribute) estimatedMemUsage() int {
	i := len(e.Defined)
	i += len(e.Values) * 16
	return i
}

func (e *DoubleVectorAttribute) estimatedMemUsage() int {
	i := len(e.Defined)
	i += len(e.Values) * 40
	return i
}

func debugPrint(server *Server) {
	lengths := make(map[string]int)
	numEntities := make(map[string]int)

	for _, e := range server.entities {
		lengths[e.entity.typeName()] += e.entity.estimatedMemUsage()
		numEntities[e.entity.typeName()]++
		switch vs := e.entity.(type) {
		case *VertexSet:
			l := vs.estimatedMapMemUsage()
			if l > 0 {
				numEntities["VSMap"]++
				lengths["VSMap"] += l
			}
		default:
			// Do nothing
		}
	}
	for a, b := range lengths {
		fmt.Printf("%v %v %v\n", a, b, numEntities[a])
	}
}

func clean(server *Server) {
	dropped := false
	for guid, e := range server.entities {
		switch vs := e.entity.(type) {
		case *VertexSet:
			if false && vs.MappingToOrdered != nil {
				dropped = true
				newVS := &VertexSet{
					Mutex:              sync.Mutex{},
					MappingToUnordered: vs.MappingToUnordered,
					MappingToOrdered:   nil,
				}
				server.entities[guid] = EntityStruct{
					entity:    newVS,
					timestamp: e.timestamp,
				}
			}
		default:
			// Do nothing
		}
	}
	if dropped {
		debug.FreeOSMemory()
	}
}

func getUsedMem() (int, error) {
	pid := os.Getpid()
	fileName := fmt.Sprintf("/proc/%v/status", pid)
	buf, err := ioutil.ReadFile(fileName)
	if err != nil {
		return 0, fmt.Errorf("Can't open %v", fileName)
	} else {
		file := string(buf)
		lines := strings.Split(file, "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "VmRSS:") {
				var rss int
				r := strings.NewReader(line)
				_, err := fmt.Fscanf(r, "VmRSS: %d", &rss)
				if err != nil {
					return 0, fmt.Errorf("Bad VmRSS line format: %v", line)
				}
				return rss * 1024, nil
			}
		}
	}
	return 0, fmt.Errorf("Can't find VmRSS field in %v")
}

func cleanEntities(server *Server) {
	server.cleanerMutex.Lock()
	defer server.cleanerMutex.Unlock()
	start := time.Now().UnixNano()
	debugPrint(server)
	clean(server)
	fmt.Printf("Time spent: %v ms\n", (time.Now().UnixNano()-start)/1000000)
}

func EntityCleaner(server *Server) {

	ticker := time.NewTicker(60 * time.Second)

	for _ = range ticker.C {
		rss, err := getUsedMem()
		if err != nil {
			fmt.Printf("Can't access used mem: %v\n", err)
			continue
		}
		fmt.Printf("UsedMem: %v\n", rss)
		cleanEntities(server)
	}
}

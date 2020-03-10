package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

func memAllocked() int {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int(m.Alloc)
}

func (e *Scalar) estimatedMemUsage() int {
	return len(*e)
}

func (e *VertexSet) estimatedMapMemUsage() int {
	return len(e.MappingToOrdered) * 40
}

func (e *VertexSet) estimatedMemUsage() int {
	i := len(e.MappingToUnordered) * 8
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

func evictGuid(server *Server, guid GUID) int {
	e := server.entities[guid]
	server.entities[guid] = CacheEntry{
		entity:       nil,
		timestamp:    ourTimestamp(),
		numEvictions: e.numEvictions + 1,
	}
	return e.entity.estimatedMemUsage()
}

func evictMappingToOrdered(server *Server) {
	itemsEvicted := 0
	for guid, e := range server.entities {
		if e.entity == nil {
			continue
		}
		switch vs := e.entity.(type) {
		case *VertexSet:
			if vs.MappingToOrdered != nil {
				newVS := &VertexSet{
					Mutex:              sync.Mutex{},
					MappingToUnordered: vs.MappingToUnordered,
					MappingToOrdered:   nil,
				}
				server.entities[guid] = CacheEntry{
					entity:    newVS,
					timestamp: e.timestamp, // Preserve the original ts
				}
				itemsEvicted++
			}
		default:
			// Do nothing
		}
	}
	fmt.Printf("evictMappingToOrdered: evicted %d maps\n", itemsEvicted)
}

type EntityEvictionItem struct {
	guid       GUID
	timestamp  int64
	memUsage   int
	numEvicted int
}

func evictUntilEnoughEvicted(server *Server, howMuchMemoryToRecycle int) int {
	start := ourTimestamp()
	items := make([]EntityEvictionItem, 0, len(server.entities))
	for guid, e := range server.entities {
		if e.entity == nil {
			continue
		}
		onDisk, err := hasOnDisk(server.dataDir, guid)
		if !onDisk || err != nil {
			if err != nil {
				fmt.Printf("Ordered disk check: error while checking for %v: %v\n", guid, err)
			}
			continue
		}
		items = append(items, EntityEvictionItem{
			guid:       guid,
			timestamp:  e.timestamp,
			memUsage:   e.entity.estimatedMemUsage(),
			numEvicted: e.numEvictions,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].timestamp < items[j].timestamp
	})
	//oldTimers := (4 * len(items)) / 5
	//sort.Slice(items[0:oldTimers], func(i, j int) bool {
	//	return items[i].memUsage > items[j].memUsage
	//})
	//tail := items[oldTimers:]
	//sort.Slice(tail, func(i, j int) bool {
	//	return tail[i].memUsage > tail[j].memUsage
	//})

	memEvicted := 0
	itemsEvicted := 0

	for i := 0; i < len(items) && memEvicted < howMuchMemoryToRecycle; i++ {
		guid := items[i].guid
		memEvicted += evictGuid(server, guid)
		itemsEvicted++
	}
	if itemsEvicted > 0 {
		runtime.GC()
	}
	fmt.Printf("Evicted %d entities (out of %d), estimated size: %d time: %d\n",
		itemsEvicted, len(items), memEvicted, ourTimestamp()-start)
	return itemsEvicted
}

func getRss() (int, error) {
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

func printMemStats(msg string, ms runtime.MemStats) {
	rss, err := getRss()
	if err != nil {
		fmt.Printf("Can't access used mem: %v\n", err)
		rss = 0
	}
	fmt.Printf("%s Alloc: %.1f  rss: %.1f heapIdle: %.1f\n",
		msg,
		float64(ms.Alloc)/(1024*1024*1204),
		float64(rss)/(1024*1024*1024),
		float64(ms.HeapIdle)/(1024*1024*1024))
}

func getNumericEnv(key string, dflt int) int {
	s, exists := os.LookupEnv(key)
	if exists {
		v, _ := strconv.ParseInt(s, 10, 64)
		return int(v)
	} else {
		return dflt
	}
}

func EntityCleaner(server *Server) {
	checkPeriodMs := getNumericEnv("SPHYNX_CACHE_EVICTION_PERIOD_MS", 1000*30)
	evictThreshold := getNumericEnv("SPHYNX_EVICTION_THRESHOLD_MB", 8*1024) * 1024 * 1024
	evictTarget := getNumericEnv("SPHYNX_EVICTION_THRESHOLD_MB", 6*1024) * 1024 * 1024
	ticker := time.NewTicker(time.Duration(checkPeriodMs) * time.Millisecond)
	for _ = range ticker.C {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		printMemStats("Checking: ", m)
		server.cleanerMutex.Lock()
		evictMappingToOrdered(server)
		if true || int(m.Alloc) > evictThreshold {
			printMemStats("Before eviction:", m)
			evictUntilEnoughEvicted(server, int(m.Alloc)-evictTarget)
			runtime.ReadMemStats(&m)
			printMemStats("After eviction:", m)
		}
		server.cleanerMutex.Unlock()
	}
}

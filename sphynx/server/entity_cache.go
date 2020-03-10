// This file contains code controlling the entity cache.
// All access to the entity cache should go through functions defined here,
// namely: getEntityFromCache, putEntityInCache, and getAnEntityWeAreSupposedToHave
//
// The main task for this code is to control the entity eviction process: we
// select and discard entities when the memory pressure is big.
//
// Entity eviction cannot run in parallel with any Sphynx Operations. The Server
// struct maintains a RWLock that can only be held by (any number of) operations
// or the only entity eviction process. So, the handlers for any incoming rpc calls
// should start by:
//
//   s.cleanerMutex.RLock()
//   defer s.cleanerMutex.RUnlock()
//
// Since entity eviction stops the world, it cannot take very long. In particular: it
// cannot save any entities to disk, because that can block everything for as long as
// 30 seconds. The solution to this is that we only consider entities to be eligible
// to eviction if they have already been saved to the unordered disk. The set of
// such entities is growing steadily, because putEntityInCache will fire a goroutine
// to write out the entity in the background. Once that job is finished (however long
// it takes), the entity can be discarded.
//
// I experimented with discarding the least recently used entries, but it did not work well,
// because I ended up discarding most entries anyway, because the really significant metric
// it the size of the entity. There is no point throwing away a 20 bytes scalar ever.
// Discarded entities are not removed from the cache, because they still provide valuable
// debug information, that can also be used in the selection process. (E.g., are we constantly
// throwing out this entity?)
//

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

type CacheEntry struct {
	entity    Entity
	timestamp int64 // The last time this entity was accessed with the getEntity.., putEntity.. api calls
	evicted   int64 // The time when this entity was (last) evicted
}

const (
	EntityIsInCache           = iota
	EntityWasEvictedFromCache = iota
	EntityIsNotInCache        = iota
)

func (server *Server) getEntityFromCache(guid GUID) (Entity, int) {
	ts := ourTimestamp()
	server.entityMutex.Lock()
	defer server.entityMutex.Unlock()
	e, exists := server.entities[guid]
	if exists {
		if e.entity != nil {
			server.entities[guid] = CacheEntry{
				entity:    e.entity,
				timestamp: ts,
			}
			return e.entity, EntityIsInCache
		} else {
			return nil, EntityWasEvictedFromCache
		}
	} else {
		return nil, EntityIsNotInCache
	}
}

func (server *Server) putEntityInCache(guid GUID, entity Entity) error {
	ts := ourTimestamp()
	server.entityMutex.Lock()
	defer server.entityMutex.Unlock()
	e, exists := server.entities[guid]
	if exists {
		if e.entity != nil {
			// Maybe panic?
			return fmt.Errorf("Caching %v but it was already cached", guid)
		}
		fmt.Printf("Guid %v is set again, last access: %v ms ago, evicted: %v ms ago\n",
			guid, timestampDiff(ts, e.timestamp), timestampDiff(ts, e.evicted))
	}
	server.entities[guid] = CacheEntry{
		entity:    entity,
		timestamp: ts,
	}
	go saveToOrderedDisk(entity, server.dataDir, guid)
	return nil
}

// The caller of this function knows that the entity is in the cache. (e.g., it
// collects inputs to an operation). We reload that entity here transparently.
// Can remove this quite painlessly as soon as the DataManager learns how to
// handle such errors.
func (server *Server) getAnEntityWeAreSupposedToHave(guid GUID) (Entity, error) {
	entity, status := server.getEntityFromCache(guid)
	switch status {
	case EntityIsNotInCache:
		return nil, fmt.Errorf("Guid %v not in the cache", guid)
	case EntityWasEvictedFromCache:
		err := server.readFromUnorderedDiskAndPutInCache(guid)
		if err != nil {
			return nil, err
		}
		entity, status = server.getEntityFromCache(guid)
		if status != EntityIsInCache {
			return nil, fmt.Errorf("Guid %v not found in cache, even though it was reloaded after eviction")
		}
	default: //EntityIsInCache: do nothing
	}
	return entity, nil
}

func ourTimestamp() int64 {
	// This must be precise
	return time.Now().UnixNano()
}
func timestampDiff(ts1 int64, ts2 int64) int64 {
	return (ts1 - ts2) / 1000000
}

// Some of these are estimations
// But most are exact
func (e *Scalar) estimatedMemUsage() int {
	return len(*e)
}

func (e *VertexSet) estimatedMemUsage() int {
	i := len(e.MappingToUnordered) * 8
	return i
}

func (e *DoubleTuple2Attribute) estimatedMemUsage() int {
	i := len(e.Defined)
	i += len(e.Values) * (8 + 8 + 8 + 16)
	return i
}

func (e *DoubleVectorAttribute) estimatedMemUsage() int {
	// This is only an estimation
	i := len(e.Defined)
	i += len(e.Values) * (2*8 + 3*8)
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
	// This is an estimation (lower bound)
	// We charge 24 bytes for strings: each has an 8 byte pointer, an 8 byte length
	// and the data itself, which is at least 8 bytes.
	i += len(e.Values) * 24
	return i
}

func (e *LongAttribute) estimatedMemUsage() int {
	i := len(e.Defined)
	i += len(e.Values) * 8
	return i
}

// Drops an entity from the cache
func evictGuid(server *Server, guid GUID) int {
	e := server.entities[guid]
	server.entities[guid] = CacheEntry{
		entity:    nil,
		timestamp: e.timestamp,
		evicted:   ourTimestamp(),
	}
	return e.entity.estimatedMemUsage()
}

// A low-hanging fruit: discard the MappingToUnordered maps
// from VertexSets that have it, but keep the vertex sets themselves.
func evictMappingToOrdered(server *Server) {
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
					timestamp: e.timestamp,
					evicted:   e.evicted,
				}
			}
		default:
			// Do nothing
		}
	}
}

type EntityEvictionItem struct {
	guid      GUID
	timestamp int64
	memUsage  int
}

// The main eviction function: it chooses the eligible (not already evicted, already on disk)
// entities, sorts them into a list, and keeps evicting entities from the list until
// enough memory seem to have been freed. runtime.GC() is an expensive operation (can
// take almost 1 second). We cannot gc and then check how much we freed after each
// eviction. Instead, we rely on our size estimates and call runtime.GC() in the end.
//
// The order of the entities is some ad-hoc heuristics. We choose the oldest 80% of
// the entities and sort them according to their estimated size.
// The remaining 20% is also sorted according to size.
// We go through first the first group and then, if necessary, the second, and discard
// the entitites.

func evictUntilEnoughEvicted(server *Server, howMuchMemoryToRecycle int) int {
	start := ourTimestamp()
	evictionCandidates := make([]EntityEvictionItem, 0, len(server.entities))
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
		evictionCandidates = append(evictionCandidates, EntityEvictionItem{
			guid:      guid,
			timestamp: e.timestamp,
			memUsage:  e.entity.estimatedMemUsage(),
		})
	}
	// Our timestamp is of nanosecond precision: this helps here to put outputs
	// before inputs.
	sort.Slice(evictionCandidates, func(i, j int) bool {
		return evictionCandidates[i].timestamp < evictionCandidates[j].timestamp
	})
	oldTimersLimit := (4 * len(evictionCandidates)) / 5
	sort.Slice(evictionCandidates[0:oldTimersLimit], func(i, j int) bool {
		return evictionCandidates[i].memUsage > evictionCandidates[j].memUsage
	})
	newOnes := evictionCandidates[oldTimersLimit:]
	sort.Slice(newOnes, func(i, j int) bool {
		return newOnes[i].memUsage > newOnes[j].memUsage
	})

	memEvicted := 0
	itemsEvicted := 0

	for i := 0; i < len(evictionCandidates) && memEvicted < howMuchMemoryToRecycle; i++ {
		guid := evictionCandidates[i].guid
		memEvicted += evictGuid(server, guid)
		itemsEvicted++
	}
	if itemsEvicted > 0 {
		runtime.GC()
	}
	fmt.Printf("Evicted %d entities (out of %d), estimated size: %d time: %d\n",
		itemsEvicted, len(evictionCandidates), memEvicted, timestampDiff(ourTimestamp(), start))
	return itemsEvicted
}

// This is the main eviction loop.
//
// The go runtime cannot be forced to keep a limit on the memory allocated from the system.
// Furthermore, it is also very reluctant to return any memory allocated from the OS.
// Two important metrics are of relevance here:
//  memStats.Alloc and memStats.HeapIdle
// The first is how much heap memory is currently used (mostly by entities), and
// the second is how much free memory the runtime has in reserve. HeapIdle is almost
// never returned to the system.
// We have to look at Alloc to check if the entity eviction should take place.
func EntityEvictor(server *Server) {
	checkPeriodMs := getNumericEnv("SPHYNX_CACHE_EVICTION_PERIOD_MS", 1000*30)
	evictThreshold := getNumericEnv("SPHYNX_EVICTION_THRESHOLD_MB", 14*1024) * 1024 * 1024
	evictTarget := getNumericEnv("SPHYNX_EVICTION_THRESHOLD_MB", 14*1024) * 1024 * 1024
	ticker := time.NewTicker(time.Duration(checkPeriodMs) * time.Millisecond)
	for _ = range ticker.C {
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		printMemStats("Checking: ", memStats)
		server.cleanerMutex.Lock()
		evictMappingToOrdered(server)
		if int(memStats.Alloc) > evictThreshold {
			printMemStats("Before eviction:", memStats)
			evictUntilEnoughEvicted(server, int(memStats.Alloc)-evictTarget)
			runtime.ReadMemStats(&memStats)
			printMemStats("After eviction:", memStats)
		}
		server.cleanerMutex.Unlock()
	}
}

func printMemStats(msg string, ms runtime.MemStats) {
	rss, err := getRss()
	if err != nil {
		fmt.Printf("Can't access used mem: %v\n", err)
		rss = 0
	}
	fmt.Printf("%s Alloc: %.1fg  rss: %.1fg heapIdle: %.1fg\n",
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

// Rss is the memory occupied by our process as seen by Linux
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

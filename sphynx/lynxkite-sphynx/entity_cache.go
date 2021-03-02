// This file contains code controlling the entity cache.

package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

type EntityCache struct {
	sync.Mutex
	cache         map[GUID]cacheEntry
	totalMemUsage int
}

func NewEntityCache() EntityCache {
	return EntityCache{
		Mutex:         sync.Mutex{},
		cache:         make(map[GUID]cacheEntry),
		totalMemUsage: 0,
	}
}

type cacheEntry struct {
	entity    Entity
	timestamp int64 // The last time this entity was accessed
	memUsage  int
}

var cachedEntitiesMaxMem = getNumericEnv("SPHYNX_CACHED_ENTITIES_MAX_MEM_MB", 1*1024) * 1024 * 1024
var sphynxThreads = getNumericEnv("SPHYNX_THREADS", runtime.NumCPU())

func (entityCache *EntityCache) Get(guid GUID) (Entity, bool) {
	ts := ourTimestamp()
	entityCache.Lock()
	defer entityCache.Unlock()
	entry, exists := entityCache.cache[guid]
	if exists {
		entry.timestamp = ts
		entityCache.cache[guid] = entry
		return entry.entity, true
	}
	return nil, false
}

// Clear the entity cache.
func (entityCache *EntityCache) Clear() {
	entityCache.Lock()
	defer entityCache.Unlock()
	entityCache.cache = make(map[GUID]cacheEntry)
	entityCache.totalMemUsage = 0
}

// Set puts the entity in the cache.
// It also drops old items if the total memory usage of cached items
// exceeds cachedEntitiesMaxMem
func (entityCache *EntityCache) Set(guid GUID, entity Entity) {
	memUsage := entity.estimatedMemUsage()
	entityCache.Lock()
	defer entityCache.Unlock()
	_, exists := entityCache.cache[guid]
	if !exists {
		entityCache.cache[guid] = cacheEntry{
			entity:    entity,
			timestamp: ourTimestamp(),
			memUsage:  memUsage,
		}
		entityCache.totalMemUsage += memUsage
		entityCache.maybeGarbageCollect()
	}
	// It is legitimate that the entity is already in the cache. E.g., the DataManager re-runs
	// an operation to re-create a missing output, but the rest of the outputs were not evicted.
	// But we do not want to update the timestamp for those.
}

func NotInCacheError(kind string, guid GUID) error {
	// If we drop something from the cache it will be reloaded before the next use.
	// The exception is when we drop it right after loading it. This generally means
	// the cache is too small.
	return fmt.Errorf("Could not fit %v %v into memory. Increase SPHYNX_CACHED_ENTITIES_MAX_MEM_MB?", kind, guid)
}

type entityEvictionItem struct {
	guid      GUID
	timestamp int64
	memUsage  int
}

func (entityCache *EntityCache) maybeGarbageCollect() {
	howMuchMemoryToRecycle := entityCache.totalMemUsage - cachedEntitiesMaxMem
	if howMuchMemoryToRecycle <= 0 {
		return
	}
	start := ourTimestamp()
	evictionCandidates := make([]entityEvictionItem, 0, len(entityCache.cache))
	for guid, e := range entityCache.cache {
		evictionCandidates = append(evictionCandidates, entityEvictionItem{
			guid:      guid,
			timestamp: e.timestamp,
			memUsage:  e.entity.estimatedMemUsage(),
		})
	}
	// Our timestamp is of nanosecond precision: this helps here to put inputs
	// before outputs.
	sort.Slice(evictionCandidates, func(i, j int) bool {
		return evictionCandidates[i].timestamp < evictionCandidates[j].timestamp
	})

	memEvicted := 0
	itemsEvicted := 0

	for i := 0; i < len(evictionCandidates) && memEvicted < howMuchMemoryToRecycle; i++ {
		guid := evictionCandidates[i].guid
		log.Printf("Evicting: %v\n", evictionCandidates[i])
		delete(entityCache.cache, guid)
		memEvicted += evictionCandidates[i].memUsage
		itemsEvicted++
	}
	log.Printf("Evicted %d entities (out of %d), estimated size: %d time: %d\n",
		itemsEvicted, len(evictionCandidates), memEvicted, (ourTimestamp()-start)/1000000)
	entityCache.totalMemUsage -= memEvicted
}

func ourTimestamp() int64 {
	// This must be precise
	return time.Now().UnixNano()
}

func ptrSize() int {
	var somePtr = [1]*uint8{}
	return int(unsafe.Sizeof(somePtr[0]))
}

func boolSize() int {
	someBool := false
	return int(unsafe.Sizeof(someBool))
}

// The number of occupied bytes (sizeof) for our basic types
var sliceCost = 3 * ptrSize()
var int64Cost = int(unsafe.Sizeof(int64(0)))
var float64Cost = int(unsafe.Sizeof(float64(0)))
var sphinxIdCost = int(unsafe.Sizeof(SphynxId(0)))
var boolCost = boolSize()

// Some of these are estimations
// But most are exact.

func (e *Scalar) estimatedMemUsage() int {
	return len(*e)
}

func (e *VertexSet) estimatedMemUsage() int {
	i := len(e.MappingToUnordered) * int64Cost
	return i
}

func (e *DoubleVectorAttribute) estimatedMemUsage() int {
	if len(e.Defined) == 0 {
		return 0
	}
	// We're assuming here that all the slice elements have the same size
	// In any case, we're using the first value
	oneElementSize := sliceCost + len(e.Values[0])*float64Cost
	i := len(e.Defined) * boolCost
	i += len(e.Values) * oneElementSize
	return i
}

func (e *LongVectorAttribute) estimatedMemUsage() int {
	if len(e.Defined) == 0 {
		return 0
	}
	// We're assuming here that all the slice elements have the same size
	// In any case, we're using the first value
	oneElementSize := sliceCost + len(e.Values[0])*int64Cost
	i := len(e.Defined) * boolCost
	i += len(e.Values) * oneElementSize
	return i
}

func (e *EdgeBundle) estimatedMemUsage() int {
	i := len(e.EdgeMapping) * int64Cost
	i += len(e.Src) * sphinxIdCost
	i += len(e.Dst) * sphinxIdCost
	return i
}

func (e *DoubleAttribute) estimatedMemUsage() int {
	i := len(e.Defined) * boolCost
	i += len(e.Values) * float64Cost
	return i
}

func (e *StringAttribute) estimatedMemUsage() int {
	i := len(e.Defined) * boolCost
	// This is an estimation (lower bound?)
	// We charge 2 pointers plus the string length,
	// which we assume is at least 8 bytes.
	i += len(e.Values) * (2*ptrSize() + 8)
	return i
}

func (e *LongAttribute) estimatedMemUsage() int {
	i := len(e.Defined) * boolCost
	i += len(e.Values) * int64Cost
	return i
}

func getNumericEnv(key string, dflt int) int {
	s, exists := os.LookupEnv(key)
	if exists {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			pmsg := fmt.Sprintf("Cannot parse environment variable (%v) as int: %v", key, s)
			panic(pmsg)
		}
		return int(v)
	} else {
		return dflt
	}
}

// This file contains code controlling the entity cache.

package main

import (
	"fmt"
	"os"
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

func (entityCache *EntityCache) Get(guid GUID) (Entity, bool) {
	ts := ourTimestamp()
	entityCache.Lock()
	defer entityCache.Unlock()
	entry, exists := entityCache.cache[guid]
	if exists {
		fresh := entry
		fresh.timestamp = ts
		entityCache.cache[guid] = fresh
		return entry.entity, true
	}
	return nil, false
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
		if entityCache.totalMemUsage > cachedEntitiesMaxMem {
			memEvicted := entityCache.evictUntilEnoughEvicted(entityCache.totalMemUsage - cachedEntitiesMaxMem)
			entityCache.totalMemUsage -= memEvicted
		}
	}
	// It is legitimate that the entity is already in the cache. E.g., the DataManager re-runs
	// an operation to re-create a missing output, but the rest of the outputs were not evicted.
	// But we do not want to update the timestamp for those.
}

type entityEvictionItem struct {
	guid      GUID
	timestamp int64
	memUsage  int
}

func (entityCache *EntityCache) evictUntilEnoughEvicted(howMuchMemoryToRecycle int) int {
	start := ourTimestamp()
	evictionCandidates := make([]entityEvictionItem, 0, len(entityCache.cache))
	for guid, e := range entityCache.cache {
		evictionCandidates = append(evictionCandidates, entityEvictionItem{
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

	memEvicted := 0
	itemsEvicted := 0

	for i := 0; i < len(evictionCandidates) && memEvicted < howMuchMemoryToRecycle; i++ {
		guid := evictionCandidates[i].guid
		fmt.Printf("Evicting: %v\n", evictionCandidates[i])
		delete(entityCache.cache, guid)
		memEvicted += evictionCandidates[i].memUsage
		itemsEvicted++
	}
	fmt.Printf("Evicted %d entities (out of %d), estimated size: %d time: %d\n",
		itemsEvicted, len(evictionCandidates), memEvicted, timestampDiff(ourTimestamp(), start))
	return memEvicted
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
	sizeOfVertexID := int(unsafe.Sizeof(SphynxId(0)))
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

func getNumericEnv(key string, dflt int) int {
	s, exists := os.LookupEnv(key)
	if exists {
		v, _ := strconv.ParseInt(s, 10, 64)
		return int(v)
	} else {
		return dflt
	}
}

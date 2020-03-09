// Types used by Sphynx.
package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type CacheEntry struct {
	entity       Entity
	timestamp    int64
	numEvictions int
}
type Server struct {
	cleanerMutex     sync.RWMutex
	entityMutex      sync.Mutex
	entities         map[GUID]CacheEntry
	dataDir          string
	unorderedDataDir string
}
type GUID string
type OperationDescription struct {
	Class string
	Data  map[string]interface{}
}
type OperationInstance struct {
	GUID      GUID
	Inputs    map[string]GUID
	Outputs   map[string]GUID
	Operation OperationDescription
}

const (
	EntityIsInCache           = iota
	EntityWasEvictedFromCache = iota
	EntityIsNotInCache        = iota
)

func (server *Server) getAnEntityWeKnowWeHave(guid GUID) (Entity, error) {
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
			return nil, fmt.Errorf("Guid %v not found in cache, but we reloaded it after eviction")
		}
	default: //EntityIsInCache: do nothing
	}
	return entity, nil
}

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

func ourTimestamp() int64 {
	return time.Now().UnixNano() / 1000000
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
		fmt.Printf("Guid %v is set again %v ms after it was evicted\n", guid, ts-e.timestamp)
	}
	server.entities[guid] = CacheEntry{
		entity:    entity,
		timestamp: ts,
	}
	return nil
}

type VertexID uint32

type EdgeBundle struct {
	Src         []VertexID
	Dst         []VertexID
	EdgeMapping []int64
}

func NewEdgeBundle(size int, maxSize int) *EdgeBundle {
	return &EdgeBundle{
		Src:         make([]VertexID, size, maxSize),
		Dst:         make([]VertexID, size, maxSize),
		EdgeMapping: make([]int64, size, maxSize),
	}
}

func (es *EdgeBundle) Make(size int, maxSize int) {
	es.Src = make([]VertexID, size, maxSize)
	es.Dst = make([]VertexID, size, maxSize)
	es.EdgeMapping = make([]int64, size, maxSize)
}

type VertexSet struct {
	sync.Mutex
	MappingToUnordered []int64
	MappingToOrdered   map[int64]VertexID
}

func (vs *VertexSet) GetMappingToOrdered() map[int64]VertexID {
	vs.Lock()
	defer vs.Unlock()
	if vs.MappingToOrdered == nil {
		vs.MappingToOrdered = make(map[int64]VertexID)
		for i, j := range vs.MappingToUnordered {
			vs.MappingToOrdered[j] = VertexID(i)
		}
	}
	return vs.MappingToOrdered
}

// A scalar is stored as its JSON encoding. If you need the real value, unmarshal it for yourself.
type Scalar []byte

func ScalarFrom(value interface{}) (Scalar, error) {
	jsonEncoding, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("Error while marshaling scalar: %v", err)
	}
	return Scalar(jsonEncoding), nil
}
func (scalar *Scalar) LoadTo(dst interface{}) error {
	if err := json.Unmarshal([]byte(*scalar), dst); err != nil {
		return fmt.Errorf("Error while unmarshaling scalar: %v", err)
	}
	return nil
}

type DoubleAttribute struct {
	Values  []float64
	Defined []bool
}

type LongAttribute struct {
	Values  []int64
	Defined []bool
}

type StringAttribute struct {
	Values  []string
	Defined []bool
}

type DoubleTuple2AttributeValue struct {
	X float64 `parquet:"name=x, type=DOUBLE"`
	Y float64 `parquet:"name=y, type=DOUBLE"`
}
type DoubleTuple2Attribute struct {
	Values  []DoubleTuple2AttributeValue
	Defined []bool
}

type DoubleVectorAttributeValue []float64
type DoubleVectorAttribute struct {
	Values  []DoubleVectorAttributeValue
	Defined []bool
}

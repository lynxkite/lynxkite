// Additional code to make it easier to work with NetworKit.
package main

import (
	"fmt"
	"log"
	"runtime/debug"
	"strconv"
	"strings"
	"unsafe"

	"github.com/lynxkite/lynxkite/sphynx/networkit"
)

func toNetworKit(vs *VertexSet, es *EdgeBundle, weight *DoubleAttribute, directed bool) networkit.Graph {
	builder := networkit.NewGraphBuilder(uint64(len(vs.MappingToUnordered)), weight != nil, directed)
	defer networkit.DeleteGraphBuilder(builder)
	for i := range es.Src {
		w := 1.0
		if weight != nil && weight.Defined[i] {
			w = weight.Values[i]
		}
		builder.AddHalfEdge(uint64(es.Src[i]), uint64(es.Dst[i]), w)
	}
	return builder.ToGraph(true)
}

func (self *NetworKitHelper) ToSphynx(g networkit.Graph) (vs *VertexSet, es *EdgeBundle) {
	vs = &VertexSet{}
	vs.MappingToUnordered = make([]int64, g.NumberOfNodes())
	for i := range vs.MappingToUnordered {
		vs.MappingToUnordered[i] = int64(i)
	}
	es = &EdgeBundle{}
	es.Src = make([]SphynxId, g.NumberOfEdges())
	es.Dst = make([]SphynxId, g.NumberOfEdges())
	// We want to copy directly into EdgeBundle from networkit.Graph.
	// But the networkit package doesn't know the SphynxId type.
	// Rather than merge the two packages or copy each element (again),
	// we use this unsafe cast.
	uint32Src := *(*[]uint32)(unsafe.Pointer(&es.Src))
	uint32Dst := *(*[]uint32)(unsafe.Pointer(&es.Dst))
	networkit.GraphToEdgeList(g, uint32Src, uint32Dst)
	es.EdgeMapping = make([]int64, len(es.Src))
	for i := range es.EdgeMapping {
		es.EdgeMapping[i] = int64(i)
	}
	return
}

type NetworKitOptions struct {
	Options map[string]interface{}
}

func (self *NetworKitOptions) Get(k string) interface{} {
	o := self.Options[k]
	if o == nil {
		panic(fmt.Sprintf("%#v not found in %#v", k, self.Options))
	}
	return o
}

func (self *NetworKitOptions) Double(k string) float64 {
	return self.Get(k).(float64)
}
func (self *NetworKitOptions) Count(k string) uint64 {
	return uint64(self.Double(k))
}
func (self *NetworKitOptions) Numbers(k string) []uint64 {
	raw := self.Get(k).(string)
	split := strings.Split(raw, ",")
	nums := make([]uint64, len(split))
	var err error
	for i, v := range split {
		nums[i], err = strconv.ParseUint(strings.Trim(v, " "), 10, 64)
		if err != nil {
			panic(fmt.Sprintf("Could not parse %#v: %v", raw, err))
		}
	}
	return nums
}

// The caller has to call DeleteUint64Vector.
func (self *NetworKitOptions) DegreeVector(k string) networkit.Uint64Vector {
	nums := self.Numbers(k)
	cv := networkit.NewUint64Vector()
	size := int(self.Count("size"))
	// Instead of requiring the user to type "1,2,3" hundreds of times,
	// we repeat the sequence to reach the target count ("size").
	for i := 0; i < size; i += 1 {
		cv.Add(nums[i%len(nums)])
	}
	return cv
}

type NetworKitHelper struct {
	ea        *EntityAccessor
	Op        string
	Options   *NetworKitOptions
	graph     networkit.Graph
	partition networkit.Partition
	cover     networkit.Cover
}

func (self *NetworKitHelper) Cleanup() (err error) {
	// Convert NetworKit exceptions to errors. The caller has to pass this on.
	if e := recover(); e != nil {
		err = fmt.Errorf("%v", e)
		log.Printf("%v\n%v", e, string(debug.Stack()))
	}
	if self.graph != nil {
		networkit.DeleteGraph(self.graph)
	}
	if self.partition != nil {
		networkit.DeletePartition(self.partition)
	}
	if self.cover != nil {
		networkit.DeleteCover(self.cover)
	}
	return
}

func NewNetworKitHelper(ea *EntityAccessor) (h *NetworKitHelper) {
	h = &NetworKitHelper{}
	h.ea = ea
	h.Op = ea.GetStringParam("op")
	o := &NetworKitOptions{ea.GetMapParam("options")}
	h.Options = o
	seed := uint64(1)
	if s, exists := o.Options["seed"]; exists {
		seed = uint64(s.(float64))
	}
	networkit.SetSeed(seed, true)
	networkit.SetThreadsFromEnv()
	return h
}

func (self *NetworKitHelper) GetGraph() networkit.Graph {
	if self.graph != nil {
		return self.graph
	}
	vs := self.ea.getVertexSet("vs")
	es := self.ea.getEdgeBundle("es")
	weight := self.ea.getDoubleAttributeOpt("weight")
	// The caller can set "directed" to false to create an undirected graph.
	self.graph = toNetworKit(vs, es, weight, self.Options.Options["directed"] != false)
	return self.graph
}

func (self *NetworKitHelper) GetPartition() networkit.Partition {
	if self.partition != nil {
		return self.partition
	}
	vs := self.ea.getVertexSet("vs")
	seg := self.ea.getVertexSet("segments")
	belongsTo := self.ea.getEdgeBundle("belongsTo")
	p := networkit.NewPartition(uint64(len(vs.MappingToUnordered)))
	p.SetUpperBound(uint64(len(seg.MappingToUnordered)))
	log.Printf("counts: %v %v %v", len(vs.MappingToUnordered), len(seg.MappingToUnordered))
	for i := range belongsTo.EdgeMapping {
		log.Printf("%v %v %v", i, belongsTo.Dst[i], belongsTo.Src[i])
		p.AddToSubset(uint64(belongsTo.Dst[i]), uint64(belongsTo.Src[i]))
	}
	self.partition = p
	return p
}

func (self *NetworKitHelper) GetCover() networkit.Cover {
	if self.cover != nil {
		return self.cover
	}
	vs := self.ea.getVertexSet("vs")
	seg := self.ea.getVertexSet("segments")
	belongsTo := self.ea.getEdgeBundle("belongsTo")
	p := networkit.NewCover(uint64(len(vs.MappingToUnordered)))
	p.SetUpperBound(uint64(len(seg.MappingToUnordered)))
	log.Printf("counts: %v %v %v", len(vs.MappingToUnordered), len(seg.MappingToUnordered))
	for i := range belongsTo.EdgeMapping {
		log.Printf("%v %v %v", i, belongsTo.Dst[i], belongsTo.Src[i])
		p.AddToSubset(uint64(belongsTo.Dst[i]), uint64(belongsTo.Src[i]))
	}
	self.cover = p
	return p
}

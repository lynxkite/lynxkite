// Types used by Sphynx.
package main

import (
	"encoding/json"
	"fmt"
	pb "github.com/lynxkite/lynxkite/sphynx/proto"
	"sort"
)

type Server struct {
	pb.UnimplementedSphynxServer
	entityCache      EntityCache
	dataDir          string
	unorderedDataDir string
}
type GUID string
type SphynxId uint32
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

type EdgeBundle struct {
	Src         []SphynxId
	Dst         []SphynxId
	EdgeMapping []int64
}

func NewEdgeBundle(size int, maxSize int) *EdgeBundle {
	return &EdgeBundle{
		Src:         make([]SphynxId, size, maxSize),
		Dst:         make([]SphynxId, size, maxSize),
		EdgeMapping: make([]int64, size, maxSize),
	}
}

func (self *EdgeBundle) Len() int {
	return len(self.EdgeMapping)
}
func (self *EdgeBundle) Swap(i, j int) {
	self.Src[i], self.Src[j] = self.Src[j], self.Src[i]
	self.Dst[i], self.Dst[j] = self.Dst[j], self.Dst[i]
	self.EdgeMapping[i], self.EdgeMapping[j] = self.EdgeMapping[j], self.EdgeMapping[i]
}
func (self *EdgeBundle) Less(i, j int) bool {
	return self.EdgeMapping[i] < self.EdgeMapping[j]
}
func (self *EdgeBundle) Sort() {
	sort.Sort(self)
}

type VertexSet struct {
	// This slice contains the Spark IDs in ascending order.
	MappingToUnordered []int64
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

type DoubleVectorAttributeValue []float64
type DoubleVectorAttribute struct {
	Values  []DoubleVectorAttributeValue
	Defined []bool
}

type LongVectorAttributeValue []int64
type LongVectorAttribute struct {
	Values  []LongVectorAttributeValue
	Defined []bool
}

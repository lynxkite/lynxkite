// Implements the AddRankingAttribute operation
// See the Spark implementation for details

package main

import (
	"fmt"
	"sort"
)

type DoubleAttributeSorterStruct struct {
	idx     int64
	val     float64
	defined bool
}
type DoubleAttributeSorterSlice []DoubleAttributeSorterStruct

func (a DoubleAttributeSorterSlice) Len() int {
	return len(a)
}
func (a DoubleAttributeSorterSlice) Less(i, j int) bool {
	return a[i].defined && (!a[j].defined || a[i].val < a[j].val)
}
func (a DoubleAttributeSorterSlice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

type StringAttributeSorterStruct struct {
	idx     int64
	val     string
	defined bool
}
type StringAttributeSorterSlice []StringAttributeSorterStruct

func (a StringAttributeSorterSlice) Len() int {
	return len(a)
}
func (a StringAttributeSorterSlice) Less(i, j int) bool {
	return a[i].defined && (!a[j].defined || a[i].val < a[j].val)
}
func (a StringAttributeSorterSlice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

type LongAttributeSorterStruct struct {
	idx     int64
	val     int64
	defined bool
}
type LongAttributeSorterSlice []LongAttributeSorterStruct

func (a LongAttributeSorterSlice) Len() int {
	return len(a)
}
func (a LongAttributeSorterSlice) Less(i, j int) bool {
	return a[i].defined && (!a[j].defined || a[i].val < a[j].val)
}
func (a LongAttributeSorterSlice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func doAddRankingAttribute(sortKey TabularEntity, length int, ascending bool) (*LongAttribute, error) {
	ranking := &LongAttribute{
		Values:  make([]int64, length),
		Defined: make([]bool, length),
	}
	switch sortKey := sortKey.(type) {
	case *DoubleAttribute:
		w := make([]DoubleAttributeSorterStruct, length)
		for i := 0; i < length; i++ {
			w[i].idx = int64(i)
			w[i].val = sortKey.Values[i]
			if sortKey.Defined[i] {
				w[i].defined = sortKey.Defined[i]
			}
		}
		sort.Sort(DoubleAttributeSorterSlice(w))
		if ascending {
			for i := 0; i < len(w) && w[i].defined; i++ {
				idx := w[i].idx
				ranking.Values[idx] = int64(i)
				ranking.Defined[idx] = true
			}
		} else {
			i := len(w) - 1
			for ; i >= 0 && !w[i].defined; i-- {
			}
			for j := 0; i >= 0; i-- {
				idx := w[i].idx
				ranking.Values[idx] = int64(j)
				ranking.Defined[idx] = true
				j++
			}
		}
	case *StringAttribute:
		w := make([]StringAttributeSorterStruct, length)
		for i := 0; i < length; i++ {
			w[i].idx = int64(i)
			w[i].val = sortKey.Values[i]
			if sortKey.Defined[i] {
				w[i].defined = sortKey.Defined[i]
			}
		}
		sort.Sort(StringAttributeSorterSlice(w))
		if ascending {
			for i := 0; i < len(w) && w[i].defined; i++ {
				idx := w[i].idx
				ranking.Values[idx] = int64(i)
				ranking.Defined[idx] = true
			}
		} else {
			i := len(w) - 1
			for ; i >= 0 && !w[i].defined; i-- {
			}
			for j := 0; i >= 0; i-- {
				idx := w[i].idx
				ranking.Values[idx] = int64(j)
				ranking.Defined[idx] = true
				j++
			}
		}
	case *LongAttribute:
		w := make([]LongAttributeSorterStruct, length)
		for i := 0; i < length; i++ {
			w[i].idx = int64(i)
			w[i].val = sortKey.Values[i]
			if sortKey.Defined[i] {
				w[i].defined = sortKey.Defined[i]
			}
		}
		sort.Sort(LongAttributeSorterSlice(w))
		if ascending {
			for i := 0; i < len(w) && w[i].defined; i++ {
				idx := w[i].idx
				ranking.Values[idx] = int64(i)
				ranking.Defined[idx] = true
			}
		} else {
			i := len(w) - 1
			for ; i >= 0 && !w[i].defined; i-- {
			}
			for j := 0; i >= 0; i-- {
				idx := w[i].idx
				ranking.Values[idx] = int64(j)
				ranking.Defined[idx] = true
				j++
			}
		}
	default:
		return nil, fmt.Errorf("Sort not supported for type: %T", sortKey)
	}
	return ranking, nil
}

func init() {
	operationRepository["AddRankingAttribute"] = Operation{
		execute: func(ea *EntityAccessor) error {
			sortKey := ea.inputs["sortKey"].(TabularEntity)
			vertices := ea.getVertexSet("vertices")
			ascending := ea.GetBoolParam("ascending")
			ordinal, err := doAddRankingAttribute(sortKey, len(vertices.MappingToUnordered), ascending)
			if err != nil {
				return err
			}
			ea.output("ordinal", ordinal)
			return nil
		},
	}
}

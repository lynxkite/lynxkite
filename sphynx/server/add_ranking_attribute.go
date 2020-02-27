// Implements the AddRankingAttribute operation

package main

import (
	"fmt"
	"sort"
)

func doAddRankingAttribute(sortKey ParquetEntity, length int, ascending bool) (*LongAttribute, error) {
	ranking := &LongAttribute{
		Values:  make([]int64, length),
		Defined: make([]bool, length),
	}
	switch sortKey := sortKey.(type) {
	case *DoubleAttribute:
		type s struct {
			idx     int64
			val     float64
			defined bool
		}
		w := make([]s, length)
		for i := 0; i < length; i++ {
			w[i].idx = int64(i)
			w[i].val = sortKey.Values[i]
			w[i].defined = sortKey.Defined[i]
		}
		if ascending {
			sort.Slice(w[:], func(i, j int) bool {
				return w[i].defined && (!w[j].defined || w[i].val < w[j].val)
			})
		} else {
			sort.Slice(w[:], func(i, j int) bool {
				return w[i].defined && (!w[j].defined || w[i].val > w[j].val)
			})
		}
		for i := 0; i < length && w[i].defined; i++ {
			idx := w[i].idx
			ranking.Values[idx] = int64(i)
			ranking.Defined[idx] = true
		}
	case *LongAttribute:
		type s struct {
			idx     int64
			val     int64
			defined bool
		}
		w := make([]s, length)
		for i := 0; i < length; i++ {
			w[i].idx = int64(i)
			w[i].val = sortKey.Values[i]
			w[i].defined = sortKey.Defined[i]
		}
		if ascending {
			sort.Slice(w[:], func(i, j int) bool {
				return w[i].defined && (!w[j].defined || w[i].val < w[j].val)
			})
		} else {
			sort.Slice(w[:], func(i, j int) bool {
				return w[i].defined && (!w[j].defined || w[i].val > w[j].val)
			})
		}
		for i := 0; i < length && w[i].defined; i++ {
			idx := w[i].idx
			ranking.Values[idx] = int64(i)
			ranking.Defined[idx] = true
		}
	case *StringAttribute:
		type s struct {
			idx     int64
			val     string
			defined bool
		}
		w := make([]s, length)
		for i := 0; i < length; i++ {
			w[i].idx = int64(i)
			w[i].val = sortKey.Values[i]
			w[i].defined = sortKey.Defined[i]
		}
		if ascending {
			sort.Slice(w[:], func(i, j int) bool {
				return w[i].defined && (!w[j].defined || w[i].val < w[j].val)
			})
		} else {
			sort.Slice(w[:], func(i, j int) bool {
				return w[i].defined && (!w[j].defined || w[i].val > w[j].val)
			})
		}
		for i := 0; i < length && w[i].defined; i++ {
			idx := w[i].idx
			ranking.Values[idx] = int64(i)
			ranking.Defined[idx] = true
		}
	default:
		return nil, fmt.Errorf("Sort not supported for type: %T", sortKey)
	}
	return ranking, nil
}

func init() {
	operationRepository["AddRankingAttribute"] = Operation{
		execute: func(ea *EntityAccessor) error {
			sortKey := ea.inputs["sortKey"].(ParquetEntity)
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

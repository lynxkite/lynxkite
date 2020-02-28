package main

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestAddRankingAttributeSpeed(t *testing.T) {
	n := 6000000
	sortKey := &DoubleAttribute{
		Values:  make([]float64, n),
		Defined: make([]bool, n),
	}
	rand.Seed(42)
	for i := 0; i < n; i++ {
		sortKey.Values[i] = rand.Float64()
		if rand.Float64() > 0.001 {
			sortKey.Defined[i] = true
		}
	}
	start := time.Now().UnixNano()
	_, _ = doAddRankingAttribute(sortKey, n, true)
	end := time.Now().UnixNano()
	elapsed := (end - start) / 1000000
	fmt.Println("AddRankingAttribute time: ", elapsed)
}

func TestAddRankingAttributeFloat64(t *testing.T) {
	sortKey := &DoubleAttribute{
		Values:  []float64{4.0, 3.0, 5.0, 1.0},
		Defined: []bool{true, false, true, true},
	}
	ordinal, _ := doAddRankingAttribute(sortKey, len(sortKey.Values), true)
	expected := &LongAttribute{
		Values:  []int64{1, 0, 2, 0},
		Defined: []bool{true, false, true, true},
	}
	if !reflect.DeepEqual(ordinal, expected) {
		t.Errorf("Bad ordering for %v got: %v expected: %v",
			sortKey, ordinal, expected)
	}
	ordinal, _ = doAddRankingAttribute(sortKey, len(sortKey.Values), false)
	expected = &LongAttribute{
		Values:  []int64{1, 0, 0, 2},
		Defined: []bool{true, false, true, true},
	}
	if !reflect.DeepEqual(ordinal, expected) {
		t.Errorf("Bad ordering for %v, got: %v expected: %v", sortKey, ordinal, expected)
	}
}

func TestAddRankingAttributeInt64(t *testing.T) {
	sortKey := &LongAttribute{
		Values:  []int64{4, 3, 5, 1},
		Defined: []bool{true, false, true, true},
	}
	ordinal, _ := doAddRankingAttribute(sortKey, len(sortKey.Values), true)
	expected := &LongAttribute{
		Values:  []int64{1, 0, 2, 0},
		Defined: []bool{true, false, true, true},
	}
	if !reflect.DeepEqual(ordinal, expected) {
		t.Errorf("Bad ordering for %v got: %v expected: %v",
			sortKey, ordinal, expected)
	}
	ordinal, _ = doAddRankingAttribute(sortKey, len(sortKey.Values), false)
	expected = &LongAttribute{
		Values:  []int64{1, 0, 0, 2},
		Defined: []bool{true, false, true, true},
	}
	if !reflect.DeepEqual(ordinal, expected) {
		t.Errorf("Bad ordering for %v, got: %v expected: %v", sortKey, ordinal, expected)
	}
}

func TestAddRankingAttributeString(t *testing.T) {
	sortKey := &StringAttribute{
		Values:  []string{"4", "3", "5", "1"},
		Defined: []bool{true, false, true, true},
	}
	ordinal, _ := doAddRankingAttribute(sortKey, len(sortKey.Values), true)
	expected := &LongAttribute{
		Values:  []int64{1, 0, 2, 0},
		Defined: []bool{true, false, true, true},
	}
	if !reflect.DeepEqual(ordinal, expected) {
		t.Errorf("Bad ordering for %v got: %v expected: %v",
			sortKey, ordinal, expected)
	}
	ordinal, _ = doAddRankingAttribute(sortKey, len(sortKey.Values), false)
	expected = &LongAttribute{
		Values:  []int64{1, 0, 0, 2},
		Defined: []bool{true, false, true, true},
	}
	if !reflect.DeepEqual(ordinal, expected) {
		t.Errorf("Bad ordering for %v, got: %v expected: %v", sortKey, ordinal, expected)
	}
}

package main

import (
	"reflect"
	"testing"
)

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

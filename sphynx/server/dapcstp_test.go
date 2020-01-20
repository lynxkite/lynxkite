package main

import (
	"testing"
)

func TestDacstp(t *testing.T) {
	// We test by computing a shorest path in a small graph. Make sure that
	// the net gain is 42 plus the length of the shortest path, this way
	// we have a positive gain.
	edges := &EdgeBundle{
		Src:         []int{0, 0, 1, 2, 4, 3},
		Dst:         []int{1, 2, 2, 4, 3, 5},
		EdgeMapping: nil, // Not used in dapcstp
	}
	cost := &DoubleAttribute{
		Values:  []float64{4, 2, 5, 3, 4, 11},
		Defined: []bool{true, true, true, true, true, true},
	}

	root := &DoubleAttribute{
		Values:  []float64{0, 0, 0, 0, 0, 0},
		Defined: []bool{true, false, false, false, false, false},
	}

	gain := &DoubleAttribute{
		Values:  []float64{0, 0, 0, 0, 0, (2 + 3 + 4 + 11) + 42},
		Defined: []bool{true, true, true, true, true, true},
	}

	solution, err := doDapcstp(gain, root, cost, edges)
	if err != nil {
		t.Errorf("Error during dapcstp computation err: %v", err)
	}
	expected := Scalar{Value: 42.0}
	if solution.Profit != expected {
		t.Errorf("Error: expected profit was: 42, got: %v\nSolution: %v\n",
			solution.Profit, solution)
	}
}

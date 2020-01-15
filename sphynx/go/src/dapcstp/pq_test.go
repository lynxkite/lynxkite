package dapcstp

import "testing"

func TestPriorityQueue(t *testing.T) {
	q := PriorityQueue{}
	q.Push(&Item{value: 1, priority: 3})
	toUpdate := &Item{value: 3, priority: 2}
	q.Push(toUpdate)
	q.Push(&Item{value: 2, priority: 4})
	q.Update(toUpdate, 3, 5)
	for i := 1; q.Len() > 0; i++ {
		if q.Pop().value != i {
			t.Errorf("PQ bad")
		}
	}
}

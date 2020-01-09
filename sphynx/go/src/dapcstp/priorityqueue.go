// Very closely resembles the content of
// https://golang.org/pkg/container/heap/#example__priorityQueue
// Why am i copy-pasting parts of the documentation you ask?
// Well, this is the idiomatic way of doing it. Thanks for reading my rant.
package dapcstp

import (
	"container/heap"
)

// An Item is something we manage in a priority queue.
type Item struct {
	value    int   // The value of the item; node or arc index in our usage.
	priority Value // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

type priorityQueue []*Item

// A PriorityQueue implements heap.Interface and holds Items. Returns lowest priorirty first.
type PriorityQueue struct {
	q priorityQueue
}

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Pop() *Item {
	return heap.Pop(&pq.q).(*Item)
}

func (pq *PriorityQueue) Push(x *Item) {
	heap.Push(&pq.q, x)
}

func (pq *PriorityQueue) Len() int {
	return pq.q.Len()
}

// Update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) Update(item *Item, value int, priority Value) {
	item.value = value
	item.priority = priority
	heap.Fix(&pq.q, item.index)
}

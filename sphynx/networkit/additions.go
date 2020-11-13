// Additional code to make it easier to work with NetworKit.
package networkit

type SphynxId uint32
type EdgeBundle struct {
	Src         []SphynxId
	Dst         []SphynxId
	EdgeMapping []int64
}

func ToSlice(v DoubleVector) []float64 {
	s := make([]float64, v.Size())
	for i := range s {
		s[i] = v.Get(i)
	}
	return s
}

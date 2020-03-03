package main

func doStripDuplicateEdgesFromBundle(es *EdgeBundle) *EdgeBundle {
	type EdgeKey struct {
		src int
		dst int
	}

	m := make(map[EdgeKey]int64, len(es.Src))
	for i := 0; i < len(es.Src); i++ {
		k := EdgeKey{
			src: es.Src[i],
			dst: es.Dst[i],
		}
		_, exists := m[k]
		if !exists {
			m[k] = es.EdgeMapping[i]
		}
	}
	uniqueBundle := &EdgeBundle{
		Src:         make([]int, len(m)),
		Dst:         make([]int, len(m)),
		EdgeMapping: make([]int64, len(m)),
	}
	i := 0
	for key, id := range m {
		uniqueBundle.Src[i] = key.src
		uniqueBundle.Dst[i] = key.dst
		uniqueBundle.EdgeMapping[i] = id
		i++
	}
	return uniqueBundle
}

func init() {
	operationRepository["StripDuplicateEdgesFromBundle"] = Operation{
		execute: func(ea *EntityAccessor) error {
			es := ea.getEdgeBundle("es")
			unique := doStripDuplicateEdgesFromBundle(es)
			ea.output("unique", unique)
			return nil
		},
	}
}

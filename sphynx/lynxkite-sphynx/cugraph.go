// All CuGraph operations.
package main

import "os"

// When KITE_ENABLE_CUDA=yes is set, these NetworKit operations
// will instead be passed to CuGraph for execution.
var preferCUDAOverNetworKit = map[string]bool{
	"EstimateBetweenness": true,
}

func cudaEnabled() bool {
	return os.Getenv("KITE_ENABLE_CUDA") == "yes"
}

func init() {
	if !cudaEnabled() {
		return
	}
	diskOperationRepository["PageRank"] = pythonOperation("cugraph_pagerank")
	diskOperationRepository["NetworKitComputeDoubleAttribute"] =
		pythonOperation("cugraph_instead_of_networkit")
}

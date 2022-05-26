// All CuGraph operations.
package main

import "os"

// When KITE_ENABLE_CUDA=yes is set, these NetworKit operations
// will instead be passed to CuGraph for execution.
var preferCUDAOverNetworKit = map[string]bool{
	"EstimateBetweenness": true,
	"KatzCentrality":      true,
	"PLM":                 true,
	"CoreDecomposition":   true,
}

// TODO: Connected components.
// TODO: Force atlas.
// TODO: Hungarian.
// TODO: SSSP.

func cudaEnabled() bool {
	return os.Getenv("KITE_ENABLE_CUDA") == "yes"
}

// Set ifCudaDoesNotTakePrecedence as canCompute for NetworKit operations.
func ifCudaDoesNotTakePrecedence(operationDescription OperationDescription) bool {
	// When CUDA is enabled, we allow the CUDA implementation to take precedence.
	op := operationDescription.Data["op"].(string)
	if cudaEnabled() && preferCUDAOverNetworKit[op] {
		return false
	}
	return true
}

func init() {
	if !cudaEnabled() {
		return
	}
	diskOperationRepository["PageRank"] = pythonOperation("cugraph_pagerank")
	diskOperationRepository["NetworKitComputeDoubleAttribute"] =
		pythonOperation("cugraph_instead_of_networkit")
	diskOperationRepository["NetworKitComputeDoubleEdgeAttribute"] =
		pythonOperation("cugraph_instead_of_networkit")
	diskOperationRepository["NetworKitCommunityDetection"] =
		pythonOperation("cugraph_instead_of_networkit")
}

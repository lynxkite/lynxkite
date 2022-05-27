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
	"ForceAtlas2":         true,
}

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
	diskOperationRepository["ConnectedComponents"] = pythonOperation("cugraph_connected_components")
	diskOperationRepository["PageRank"] = pythonOperation("cugraph_pagerank")
	insteadOfNK := pythonOperation("cugraph_instead_of_networkit")
	diskOperationRepository["NetworKitComputeDoubleAttribute"] = insteadOfNK
	diskOperationRepository["NetworKitComputeDoubleEdgeAttribute"] = insteadOfNK
	diskOperationRepository["NetworKitCommunityDetection"] = insteadOfNK
	diskOperationRepository["NetworKitComputeVectorAttribute"] = insteadOfNK
}

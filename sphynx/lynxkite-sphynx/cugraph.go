// All CuGraph operations.
package main

import "os"

func init() {
	enabled := os.Getenv("KITE_ENABLE_CUDA")
	if enabled != "yes" {
		return
	}
	diskOperationRepository["PageRank"] = pythonOperation("cugraph_pagerank")
}

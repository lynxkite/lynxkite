// Node2Vec is a node embedding operation implemented in Python.

package main

import (
	"fmt"
	"os"
	"os/exec"
)

func init() {
	operationRepository["Node2Vec"] = Operation{
		execute: func(ea *EntityAccessor) error {
			vs := ea.getVertexSet("vs")
			es, err := ea.WriteToDisk("es")
			if err != nil {
				return nil
			}
			cmd := exec.Command(
				"python", "python/node2vec.py",
				fmt.Sprintf("%v", len(vs.MappingToUnordered)),
				fmt.Sprintf("%v", ea.GetFloatParam("iterations")),
				es)
			cmd.Stderr = os.Stderr
			output, err := cmd.Output()
			if err != nil {
				return fmt.Errorf("node2vec failed: %v", err)
			}
			return ea.OutputJson(output)
		},
	}
}

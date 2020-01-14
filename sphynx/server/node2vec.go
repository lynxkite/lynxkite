// Implements the ExampleGraph operation

package main

import (
	"fmt"
	"os"
	"os/exec"
)

func init() {
	operationRepository["PageRank"] = Operation{
		execute: func(ea *EntityAccessor) error {
			es, err := ea.WriteToDisk("es")
			if err != nil {
				return nil
			}
			cmd := exec.Command("python", "node2vec.py", es)
			cmd.Stderr = os.Stderr
			output, err := cmd.Output()
			if err != nil {
				return fmt.Errorf("node2vec failed: %v", err)
			}
			return ea.OutputJson(output)
		},
	}
}

// Implements the ExampleGraph operation

package main

import (
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
			cmd := exec.Command("python", "node2vec.py", es, ea.NameOnDisk("pagerank"))
			cmd.Stderr = os.Stderr
			cmd.Stdout = os.Stdout
			if err = cmd.Run(); err != nil {
				return err
			}
			return ea.OutputOnDisk("pagerank")
		},
	}
}

// Node2Vec is a node embedding operation implemented in Python.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
)

func init() {
	diskOperationRepository["PageRank"] = DiskOperation{
		execute: func(dataDir string, op *OperationInstance) error {
			meta, err := json.Marshal(op)
			if err != nil {
				return fmt.Errorf("node2vec failed: %v", err)
			}
			fmt.Println("running node2vec", string(meta))
			cmd := exec.Command("python", "-m", "python.node2vec", dataDir, string(meta))
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				fmt.Println("node2vec failed")
				return fmt.Errorf("node2vec failed: %v", err)
			}
			fmt.Println("done node2vec")
			return nil
		},
	}
}

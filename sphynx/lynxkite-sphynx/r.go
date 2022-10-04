// All operations implemented in Python are registered here.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
)

func rOperation(script string) DiskOperation {
	return DiskOperation{
		execute: func(dataDir string, op *OperationInstance) error {
			meta, err := json.Marshal(op)
			if err != nil {
				return fmt.Errorf("%v failed: %v", script, err)
			}
			cmd := exec.Command("Rscript", "r/"+script, dataDir, string(meta))
			var output bytes.Buffer
			cmd.Stdout = io.MultiWriter(os.Stdout, &output)
			cmd.Stderr = io.MultiWriter(os.Stderr, &output)
			if err := cmd.Run(); err != nil {
				if output.Len() > 0 {
					return fmt.Errorf("\n%v", output.String())
				} else {
					return fmt.Errorf("%v failed: %v", script, err)
				}
			}
			return nil
		},
	}
}

func init() {
	diskOperationRepository["DeriveR"] = rOperation("derive.r")
	unorderedOperationRepository["DeriveTableR"] = rOperation("derive_table.r")
	diskOperationRepository["CreateGraphInR"] = rOperation("create_graph_in_python.r")
}

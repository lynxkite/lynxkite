// All operations implemented in Python are registered here.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
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
			writer := io.MultiWriter(os.Stdout, &output)
			cmd.Stdout = writer
			cmd.Stderr = writer
			if err := cmd.Run(); err != nil {
				if output.Len() > 0 {
					re := regexp.MustCompile(`(?s).*"RUNNING USER CODE"\n`)
					msg := re.ReplaceAllLiteralString(output.String(), "")
					return fmt.Errorf("\n%v", msg)
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
	diskOperationRepository["DeriveHTMLR"] = rOperation("derive_html.r")
	unorderedOperationRepository["DeriveTableR"] = rOperation("derive_table.r")
	unorderedOperationRepository["DeriveHTMLTableR"] = rOperation("derive_html.r")
	diskOperationRepository["CreateGraphInR"] = rOperation("create_graph_in_r.r")
}

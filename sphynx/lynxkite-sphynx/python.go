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

func pythonOperation(module string) DiskOperation {
	return DiskOperation{
		execute: func(dataDir string, op *OperationInstance) error {
			meta, err := json.Marshal(op)
			if err != nil {
				return fmt.Errorf("%v failed: %v", module, err)
			}
			cmd := exec.Command("python", "-m", "python."+module, dataDir, string(meta))
			var output bytes.Buffer
			cmd.Stdout = io.MultiWriter(os.Stdout, &output)
			cmd.Stderr = io.MultiWriter(os.Stderr, &output)
			if err := cmd.Run(); err != nil {
				if output.Len() > 0 {
					return fmt.Errorf("\n%v", output.String())
				} else {
					return fmt.Errorf("%v failed: %v", module, err)
				}
			}
			return nil
		},
	}
}

func init() {
	diskOperationRepository["Node2Vec"] = pythonOperation("node2vec")
	diskOperationRepository["TSNE"] = pythonOperation("tsne")
	diskOperationRepository["PyTorchGeometricDataset"] = pythonOperation("datasets")
	diskOperationRepository["TrainGCNClassifier"] = pythonOperation("train_GCN_classifier")
	diskOperationRepository["TrainGCNRegressor"] = pythonOperation("train_GCN_regressor")
	diskOperationRepository["PredictWithGCN"] = pythonOperation("predict_with_GCN")
	diskOperationRepository["DerivePython"] = pythonOperation("derive")
	diskOperationRepository["CreateGraphInPython"] = pythonOperation("create_graph_in_python")
}

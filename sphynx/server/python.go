// All operations implemented in Python are registered here.

package main

import (
	"encoding/json"
	"fmt"
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
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("%v failed: %v", module, err)
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
}

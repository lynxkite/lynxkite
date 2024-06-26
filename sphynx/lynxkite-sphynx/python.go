// All operations implemented in Python are registered here.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
			writer := io.MultiWriter(os.Stdout, &output)
			cmd.Stdout = writer
			cmd.Stderr = writer
			log.Printf("Running %s", cmd)
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
	diskOperationRepository["PCA"] = pythonOperation("pca")
	diskOperationRepository["PyTorchGeometricDataset"] = pythonOperation("datasets")
	diskOperationRepository["TrainGCNClassifier"] = pythonOperation("train_GCN_classifier")
	diskOperationRepository["TrainGCNRegressor"] = pythonOperation("train_GCN_regressor")
	diskOperationRepository["PredictWithGCN"] = pythonOperation("predict_with_GCN")
	diskOperationRepository["DerivePython"] = pythonOperation("derive")
	unorderedOperationRepository["DeriveTablePython"] = pythonOperation("derive_table")
	diskOperationRepository["DeriveTableFromGraphPython"] = pythonOperation("derive")
	diskOperationRepository["DeriveHTMLPython"] = pythonOperation("derive_html")
	unorderedOperationRepository["DeriveHTMLTablePython"] = pythonOperation("derive_html")
	diskOperationRepository["CreateGraphInPython"] = pythonOperation("create_graph_in_python")
	diskOperationRepository["TextEmbeddingPython"] = pythonOperation("text_embedding")
}

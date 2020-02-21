package main

import (
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
	"testing"
)

func SaveInputsForGCNTest(t *testing.T, dir string) {
	// Create graph with two disjoint triangles. The target variable is the id of
	// the connected component.
	data := map[string]Entity{}
	data["es"] = &EdgeBundle{
		Src:         []int{0, 1, 2, 3, 4, 5, 1, 2, 0, 4, 5, 3},
		Dst:         []int{1, 2, 0, 4, 5, 3, 0, 1, 2, 3, 4, 5},
		EdgeMapping: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
	}
	data["vs"] = &VertexSet{
		MappingToUnordered: []int64{0, 1, 2, 3, 4, 5},
		MappingToOrdered:   nil,
	}
	data["label"] = &DoubleAttribute{
		Values:  []float64{0, 0, 0, 0, 1, 1},
		Defined: []bool{false, true, true, false, true, true},
	}
	data["features"] = &DoubleVectorAttribute{
		Values:  []DoubleVectorAttributeValue{{0}, {0}, {0}, {0}, {0}, {0}},
		Defined: []bool{true, true, true, true, true, true},
	}
	for g, entity := range data {
		guid := GUID(g)
		err := saveToOrderedDisk(entity, dir, guid)
		if err != nil {
			t.Errorf("Error while saving %v: %v", guid, err)
		}
	}
	inputs := make(map[string]GUID)
	for name := range data {
		inputs[name] = GUID(name)
	}
}

func PredictForGCNTest(t *testing.T, dir string) {
	predOpInst := &OperationInstance{
		GUID:      "PredictWithGCN",
		Inputs:    map[string]GUID{"es": "es", "features": "features", "label": "label", "model": "model"},
		Outputs:   map[string]GUID{"prediction": GUID("prediction")},
		Operation: OperationDescription{},
	}
	err := diskOperationRepository["PredictWithGCN"].execute(dir, predOpInst)
	if err != nil {
		t.Errorf("PredictWithGCN failed: %v", err)
	}
	expectedPred := &DoubleAttribute{
		Values:  []float64{0, 0, 0, 1, 1, 1},
		Defined: []bool{true, true, true, true, true, true},
	}
	loadedPred, err := loadFromOrderedDisk(dir, GUID("prediction"))
	if err != nil {
		t.Errorf("Error while loading prediction: %v", err)
	}
	values := loadedPred.(*DoubleAttribute).Values
	for i, v := range values {
		values[i] = math.Round(v)
	}
	if !reflect.DeepEqual(expectedPred, loadedPred) {
		t.Errorf("Wrong prediction: expected %v, got %v", expectedPred, loadedPred)
	}
}

func TestGCNClassifier(t *testing.T) {
	dir, err := ioutil.TempDir("", "gcn_classifier")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	SaveInputsForGCNTest(t, dir)
	parameters := map[string]interface{}{
		"batch_size":      1,
		"forget":          true,
		"hidden_size":     4,
		"iterations":      1000,
		"learning_rate":   0.003,
		"num_conv_layers": 2,
		"seed":            1,
	}
	for _, conv_op := range []string{"GCNConv", "GatedGraphConv"} {
		parameters["conv_op"] = conv_op
		opInst := &OperationInstance{
			GUID:      "TrainGCNClassifier",
			Inputs:    map[string]GUID{"es": "es", "features": "features", "label": "label"},
			Outputs:   map[string]GUID{"model": GUID("model"), "trainAcc": GUID("trainAcc")},
			Operation: OperationDescription{Data: parameters},
		}
		err = diskOperationRepository["TrainGCNClassifier"].execute(dir, opInst)
		if err != nil {
			t.Errorf("TrainGCNClassifier failed: %v", err)
		}
		var trainAcc float64
		s, err := loadFromOrderedDisk(dir, GUID("trainAcc"))
		if err != nil {
			t.Errorf("Failed to load training accuracy: %v", err)
		}
		s.(*Scalar).LoadTo(&trainAcc)
		if trainAcc != 1 {
			t.Errorf("TrainGCNClassifier has too low training accuracy %v", trainAcc)
		}
		PredictForGCNTest(t, dir)
	}
}

func TestGCNREgressor(t *testing.T) {
	dir, err := ioutil.TempDir("", "gcn_regressor")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	SaveInputsForGCNTest(t, dir)
	parameters := map[string]interface{}{
		"batch_size":      1,
		"forget":          true,
		"hidden_size":     4,
		"iterations":      1000,
		"learning_rate":   0.003,
		"num_conv_layers": 2,
		"seed":            1,
	}
	for _, conv_op := range []string{"GCNConv", "GatedGraphConv"} {
		parameters["conv_op"] = conv_op
		opInst := &OperationInstance{
			GUID:      "TrainGCNRegressor",
			Inputs:    map[string]GUID{"es": "es", "features": "features", "label": "label"},
			Outputs:   map[string]GUID{"model": GUID("model"), "trainMSE": GUID("trainMSE")},
			Operation: OperationDescription{Data: parameters},
		}
		err = diskOperationRepository["TrainGCNRegressor"].execute(dir, opInst)
		if err != nil {
			t.Errorf("TrainGCNRegressor failed: %v", err)
		}
		var trainMSE float64
		s, err := loadFromOrderedDisk(dir, GUID("trainMSE"))
		if err != nil {
			t.Errorf("Failed to load training MSE: %v", err)
		}
		s.(*Scalar).LoadTo(&trainMSE)
		if trainMSE > 0.1 {
			t.Errorf("TrainGCNRegressor has too high training MSE %v", trainMSE)
		}
		PredictForGCNTest(t, dir)
	}
}

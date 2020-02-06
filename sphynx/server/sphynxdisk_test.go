package main

import (
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"
)

func TestEntityIO(t *testing.T) {
	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	data := map[string]Entity{}
	data["VertexSet"] = &VertexSet{MappingToUnordered: []int64{0, 1, 2, 3}}
	data["StringAttribute"] = &StringAttribute{
		Values:  []string{"Adam", "Eve", "Bob", "Isolated Joe"},
		Defined: []bool{true, true, true, true}}
	data["DoubleAttribute"] = &DoubleAttribute{
		Values:  []float64{20.3, 18.2, 50.3, 2.0},
		Defined: []bool{true, true, true, true},
	}
	data["DoubleTuple2Attribute"] = &DoubleTuple2Attribute{
		Values: []DoubleTuple2AttributeValue{
			{40.71448, -74.00598},
			{47.5269674, 19.0323968},
			{1.352083, 103.819836},
			{-33.8674869, 151.2069902}},
		Defined: []bool{true, true, true, true}}
	data["EdgeBundle"] = &EdgeBundle{
		Src:         []int{0, 1, 2, 2},
		Dst:         []int{1, 0, 0, 1},
		EdgeMapping: []int64{0, 1, 2, 3},
	}
	data["DoubleVectorAttribute"] = &DoubleVectorAttribute{
		Values: []DoubleVectorAttributeValue{
			{1.1, 2.2, 3.3},
			{2.2, 3.3, 4.4},
			{3.3, 4.4, 5.5},
			{4.4, 5.5, 6.6}},
		Defined: []bool{true, true, true, true}}

	for g, entity := range data {
		// We use readable "guids" but it doesn't really matter
		guid := GUID(g)
		err = saveToOrderedDisk(entity, dir, guid)
		if err != nil {
			t.Errorf("Error while saving %v: %v", guid, err)
		}
		loaded, err := loadFromOrderedDisk(dir, guid)
		if err != nil {
			t.Errorf("Error while loading %v: %v", guid, err)
		}
		if !reflect.DeepEqual(entity, loaded) {
			t.Errorf("Entities (saved) %v and %v (reloaded) differ", entity, loaded)
		}
	}
}

func TestScalarIO(t *testing.T) {
	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	scalarValues := make(map[string]interface{})
	loadedScalars := make(map[string]*Scalar)

	scalarValues["ScalarString"] = "Hello world! 😀 "
	scalarValues["ScalarInt"] = int(42)
	scalarValues["ScalarSlice"] = []int{1, 2, 3, 4}

	for g, value := range scalarValues {
		entity, err := ScalarFrom(&value)
		if err != nil {
			t.Error(err)
		}
		guid := GUID(g)
		err = saveToOrderedDisk(&entity, dir, guid)
		if err != nil {
			t.Errorf("Error while saving %v: %v", guid, err)
		}
		loadedEntity, err := loadFromOrderedDisk(dir, guid)
		if err != nil {
			t.Errorf("Error while loading %v: %v", guid, err)
		}
		loadedScalar, ok := loadedEntity.(*Scalar)
		if !ok {
			t.Errorf("Loaded entity %v is not a scalar", guid)
		}
		loadedScalars[g] = loadedScalar
	}

	value := scalarValues["ScalarString"]
	loadedScalar := loadedScalars["ScalarString"]
	var loadedString string
	if err = loadedScalar.LoadTo(&loadedString); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(value, loadedString) {
		t.Errorf("Scalars (saved) %v and %v (reloaded) differ", value, loadedString)
	}
	value = scalarValues["ScalarInt"]
	loadedScalar = loadedScalars["ScalarInt"]
	var loadedInt int
	if err = loadedScalar.LoadTo(&loadedInt); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(value, loadedInt) {
		t.Errorf("Scalars (saved) %v and %v (reloaded) differ", value, loadedInt)
	}
	value = scalarValues["ScalarSlice"]
	loadedScalar = loadedScalars["ScalarSlice"]
	var loadedSlice []int
	if err = loadedScalar.LoadTo(&loadedSlice); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(value, loadedSlice) {
		t.Errorf("Scalars (saved) %v and %v (reloaded) differ", value, loadedSlice)
	}
}

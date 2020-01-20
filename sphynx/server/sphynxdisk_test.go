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
	data["Scalar_String"] = &Scalar{Value: "Hello world! 😀 "}
	data["Scalar_number"] = &Scalar{Value: 42}

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

// Implementations of Sphynx operations.

package main

import "log"

type EntityAccess interface {
	addVertexSet(string, *VertexSet)
	addEdgeBundle(string, *EdgeBundle)
	addScalar(string, *Scalar)
	addDoubleAttribute(string, *DoubleAttribute)
	addStringAttribute(string, *StringAttribute)
	addDoubleTuple2Attribute(string, *DoubleTuple2Attribute)
	operationInstance() *OperationInstance
	getEdgeBundle(string)
}

type EntityAccessor struct {
	server   *Server
	entities *EntityMap
	opInst   *OperationInstance
}

func (ea *EntityAccessor) operationInstance() *OperationInstance {
	return ea.opInst
}
func (ea *EntityAccessor) addVertexSet(name string, vs *VertexSet) {
	guid := ea.opInst.Outputs[name]
	ea.entities.Lock()
	ea.entities.vertexSets[guid] = vs
	ea.entities.Unlock()
	ea.server.saveEntityAndThenReloadAsATest(guid, vs)
}
func (ea *EntityAccessor) addEdgeBundle(name string, eb *EdgeBundle) {
	ea.entities.Lock()
	ea.entities.edgeBundles[ea.opInst.Outputs[name]] = eb
	ea.entities.Unlock()
}
func (ea *EntityAccessor) addScalar(name string, s *Scalar) {
	ea.entities.Lock()
	ea.entities.scalars[ea.opInst.Outputs[name]] = s
	ea.entities.Unlock()
}
func (ea *EntityAccessor) addStringAttribute(name string, sa *StringAttribute) {
	ea.entities.Lock()
	ea.entities.stringAttributes[ea.opInst.Outputs[name]] = sa
	ea.entities.Unlock()
}
func (ea *EntityAccessor) addDoubleAttribute(name string, da *DoubleAttribute) {
	ea.entities.Lock()
	ea.entities.doubleAttributes[ea.opInst.Outputs[name]] = da
	ea.entities.Unlock()
}
func (ea *EntityAccessor) addDoubleTuple2Attribute(name string, dt *DoubleTuple2Attribute) {
	ea.entities.Lock()
	ea.entities.doubleTuple2Attributes[ea.opInst.Outputs[name]] = dt
	ea.entities.Unlock()
}
func (ea *EntityAccessor) getEdgeBundle(name string) *EdgeBundle {
	ea.entities.Lock()
	defer ea.entities.Unlock()
	return ea.entities.edgeBundles[ea.opInst.Outputs[name]]

}

type Operation struct {
	execute func(ea EntityAccessor)
}

var operations = map[string]Operation{
	"ExampleGraph": exampleGraph,
}

var exampleGraph = Operation{
	execute: func(ea EntityAccessor) {
		log.Printf("exampleGraph execute called")
		vertexSet := VertexSet{Mapping: []int64{0, 1, 2, 3}}
		vsGUID := ea.operationInstance().Outputs["vertices"]
		ea.addVertexSet("vertices", &vertexSet)
		eb := &EdgeBundle{
			Src:         []int64{0, 1, 2, 2},
			Dst:         []int64{1, 0, 0, 1},
			VertexSet:   vsGUID,
			EdgeMapping: []int64{0, 1, 2, 3},
		}
		ea.addEdgeBundle("edges", eb)
		name := &StringAttribute{
			Values:        []string{"Adam", "Eve", "Bob", "Isolated Joe"},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		ea.addStringAttribute("name", name)
		age := &DoubleAttribute{
			Values:        []float64{20.3, 18.2, 50.3, 2.0},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		ea.addDoubleAttribute("age", age)
		gender := &StringAttribute{
			Values:        []string{"Male", "Female", "Male", "Male"},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		ea.addStringAttribute("gender", gender)
		income := &DoubleAttribute{
			Values:        []float64{1000, 0, 0, 2000},
			Defined:       []bool{true, false, false, true},
			VertexSetGuid: vsGUID,
		}
		ea.addDoubleAttribute("income", income)
		location := &DoubleTuple2Attribute{
			Values1:       []float64{40.71448, 47.5269674, 1.352083, -33.8674869},
			Values2:       []float64{-74.00598, 19.0323968, 103.819836, 151.2069902},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		ea.addDoubleTuple2Attribute("location", location)
		comment := &StringAttribute{
			Values: []string{"Adam loves Eve", "Eve loves Adam",
				"Bob envies Adam", "Bob loves Eve"},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		ea.addStringAttribute("comment", comment)
		weight := &DoubleAttribute{
			Values:        []float64{1, 2, 3, 4},
			Defined:       []bool{true, true, true, true},
			VertexSetGuid: vsGUID,
		}
		ea.addDoubleAttribute("weight", weight)
		greeting := Scalar{Value: "Hello world! 😀 "}
		ea.addScalar("greeting", &greeting)
	},
}

// Implements the CountVertices operation. (See the Spark implementation for details.)

package main

func init() {
	operationRepository["CountVertices"] = Operation{
		execute: func(ea *EntityAccessor) error {
			vertices := ea.getVertexSet("vertices")
			return ea.outputScalar("count", len(vertices.MappingToUnordered))
		},
	}
}

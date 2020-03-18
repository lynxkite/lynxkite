// Implements the CountVertices operation. (See the Spark implementation for details.)

package main

func init() {
	operationRepository["CountVertices"] = Operation{
		execute: func(ea *EntityAccessor) error {
			vertices := ea.getVertexSet("vertices")
			count, err := ScalarFrom(len(vertices.MappingToUnordered))
			if err != nil {
				return err
			}
			ea.output("count", &count)
			return nil
		},
	}
}

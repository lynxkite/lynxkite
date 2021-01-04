package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.controllers.Operation
import com.lynxanalytics.biggraph.controllers.ProjectTransformation
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import play.api.libs.json

class SegmentationAttributeOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Context
  import Operation.Implicits._

  val category = Categories.SegmentationAttributeOperations

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Compute segment stability")(new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = params ++= List(
      Param("name", "Save as", defaultValue = "stability"))
    def enabled = project.assertSegmentation && parent.hasEdgeBundle
    def apply() = {
      val attr = graph_operations.NetworKitComputeSegmentAttribute.run(
        "StablePartitionNodes", parent.edgeBundle, seg.belongsTo)
      project.vertexAttributes(params("name")) = attr
    }
  })
}

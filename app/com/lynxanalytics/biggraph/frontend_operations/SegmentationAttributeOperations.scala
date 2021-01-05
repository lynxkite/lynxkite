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

  abstract class NetworKitOp(ctx: Context) extends ProjectTransformation(ctx) with SegOp {
    def defaultName: String
    def nkClass: String
    def directed = true
    def addSegmentationParameters = params ++= List(
      Param("name", "Save as", defaultValue = defaultName))
    def enabled = project.assertSegmentation && parent.hasEdgeBundle
    def apply() = {
      val attr = graph_operations.NetworKitComputeSegmentAttribute.run(
        nkClass, parent.edgeBundle, seg.belongsTo, Map("directed" -> directed))
      project.vertexAttributes(params("name")) = attr
    }
  }

  register("Compute segment stability")(new NetworKitOp(_) {
    override def defaultName = "stability"
    override def nkClass = "StablePartitionNodes"
  })
  register("Compute hub dominance")(new NetworKitOp(_) {
    override def defaultName = "hub_dominance"
    override def nkClass = "CoverHubDominance"
    // TODO: We could use PartitionHubDominance when the segments are not overlapping.
  })
  register("Compute segment density")(new NetworKitOp(_) {
    override def defaultName = "density"
    override def nkClass = "IntrapartitionDensity"
  })
  register("Compute segment conductance")(new NetworKitOp(_) {
    override def defaultName = "conductance"
    override def nkClass = "IsolatedInterpartitionConductance"
  })
  register("Compute segment expansion")(new NetworKitOp(_) {
    override def defaultName = "expansion"
    override def nkClass = "IsolatedInterpartitionExpansion"
  })
  register("Compute segment fragmentation")(new NetworKitOp(_) {
    override def defaultName = "fragmentation"
    override def nkClass = "PartitionFragmentation"
    override def directed = false
  })
}

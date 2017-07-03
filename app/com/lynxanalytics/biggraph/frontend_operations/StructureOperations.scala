package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.controllers.Operation
import com.lynxanalytics.biggraph.controllers.ProjectTransformation
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_api.Attribute
import play.api.libs.json

class StructureOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Category
  import Operation.Context
  import Operation.Implicits._

  val StructureOperationsTemp = Category("Structure", "blue")
  val defaultIcon = "house"

  def register(id: String)(factory: Context => ProjectTransformation): Unit = {
    registerOp(id, defaultIcon, StructureOperationsTemp, List(projectOutput), List(projectOutput), factory)
  }

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Add reversed edges")(new ProjectTransformation(_) {
    params += Param("distattr", "Distinguishing edge attribute")
    def enabled = project.hasEdgeBundle
    def apply() = {
      val addIsNewAttr = params("distattr").nonEmpty

      val rev = {
        val op = graph_operations.AddReversedEdges(addIsNewAttr)
        op(op.es, project.edgeBundle).result
      }

      project.pullBackEdges(
        project.edgeBundle,
        project.edgeAttributes.toIndexedSeq,
        rev.esPlus,
        rev.newToOriginal)
      if (addIsNewAttr) {
        project.edgeAttributes(params("distattr")) = rev.isNew
      }
    }
  })

  register("Merge parallel edges")(new ProjectTransformation(_) {
    params ++= aggregateParams(project.edgeAttributes)
    def enabled = project.hasEdgeBundle

    def apply() = {
      applyMergeParallelEdges(project, params, byKey = false)
    }
  })

  register("Merge parallel edges by attribute")(new ProjectTransformation(_) {
    params += Choice("key", "Merge by", options = project.edgeAttrList)
    params ++= aggregateParams(project.edgeAttributes)
    def enabled = FEStatus.assert(project.edgeAttrList.nonEmpty,
      "There must be at least one edge attribute")

    def apply() = {
      applyMergeParallelEdges(project, params, byKey = true)
    }
  })

  register("Merge vertices by attribute")(new ProjectTransformation(_) {
    params += Choice("key", "Match by", options = project.vertexAttrList)
    params ++= aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes")
    def merge[T](attr: Attribute[T]): graph_operations.MergeVertices.Output = {
      val op = graph_operations.MergeVertices[T]()
      op(op.attr, attr).result
    }
    def apply() = {
      val key = params("key")
      val m = merge(project.vertexAttributes(key))
      val oldVAttrs = project.vertexAttributes.toMap
      val oldEdges = project.edgeBundle
      val oldEAttrs = project.edgeAttributes.toMap
      val oldSegmentations = project.viewer.segmentationMap
      val oldBelongsTo = if (project.isSegmentation) project.asSegmentation.belongsTo else null
      project.setVertexSet(m.segments, idAttr = "id")
      for ((name, segViewer) <- oldSegmentations) {
        val seg = project.segmentation(name)
        seg.segmentationState = segViewer.segmentationState
        val op = graph_operations.InducedEdgeBundle(induceDst = false)
        seg.belongsTo = op(
          op.srcMapping, m.belongsTo)(
            op.edges, seg.belongsTo).result.induced
      }
      if (project.isSegmentation) {
        val seg = project.asSegmentation
        val op = graph_operations.InducedEdgeBundle(induceSrc = false)
        seg.belongsTo = op(
          op.dstMapping, m.belongsTo)(
            op.edges, oldBelongsTo).result.induced
      }
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          m.belongsTo,
          AttributeWithLocalAggregator(oldVAttrs(attr), choice))
        project.newVertexAttribute(s"${attr}_${choice}", result)
      }
      // Automatically keep the key attribute.
      project.vertexAttributes(key) = aggregateViaConnection(
        m.belongsTo,
        AttributeWithAggregator(oldVAttrs(key), "first"))
      if (oldEdges != null) {
        val edgeInduction = {
          val op = graph_operations.InducedEdgeBundle()
          op(op.srcMapping, m.belongsTo)(op.dstMapping, m.belongsTo)(op.edges, oldEdges).result
        }
        project.edgeBundle = edgeInduction.induced
        for ((name, eAttr) <- oldEAttrs) {
          project.edgeAttributes(name) = eAttr.pullVia(edgeInduction.embedding)
        }
      }
    }
  })

  register("Replace edges with triadic closure")(new ProjectTransformation(_) {
    def enabled = project.hasVertexSet && project.hasEdgeBundle
    def apply() = {
      val op = graph_operations.ConcatenateBundlesMulti()
      val result = op(op.edgesAB, project.edgeBundle)(
        op.edgesBC, project.edgeBundle).result

      // saving attributes and original edges
      val origEdgeAttrs = project.edgeAttributes.toIndexedSeq

      // new edges after closure
      project.edgeBundle = result.edgesAC

      // pulling old edge attributes
      for ((name, attr) <- origEdgeAttrs) {
        project.newEdgeAttribute("ab_" + name, attr.pullVia(result.projectionFirst))
        project.newEdgeAttribute("bc_" + name, attr.pullVia(result.projectionSecond))
      }
    }
  })

  register("Replace with edge graph")(new ProjectTransformation(_) {
    def enabled = project.hasEdgeBundle
    def apply() = {
      val op = graph_operations.EdgeGraph()
      val g = op(op.es, project.edgeBundle).result
      project.setVertexSet(g.newVS, idAttr = "id")
      project.edgeBundle = g.newES
    }
  })

  register("Reverse edge direction")(new ProjectTransformation(_) {
    def enabled = project.hasEdgeBundle
    def apply() = {
      val op = graph_operations.ReverseEdges()
      val res = op(op.esAB, project.edgeBundle).result
      project.pullBackEdges(
        project.edgeBundle,
        project.edgeAttributes.toIndexedSeq,
        res.esBA,
        res.injection)
    }
  })

  register("Split vertices")(new ProjectTransformation(_) {
    params ++= List(
      Choice("rep", "Repetition attribute", options = project.vertexAttrList[Double]),
      Param("idattr", "ID attribute name", defaultValue = "new_id"),
      Param("idx", "Index attribute name", defaultValue = "index"))

    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No Double vertex attributes")
    def doSplit(doubleAttr: Attribute[Double]): graph_operations.SplitVertices.Output = {
      val op = graph_operations.SplitVertices()
      op(op.attr, doubleAttr.asLong).result
    }
    def apply() = {
      val rep = params("rep")
      val split = doSplit(project.vertexAttributes(rep).runtimeSafeCast[Double])

      project.pullBack(split.belongsTo)
      project.vertexAttributes(params("idx")) = split.indexAttr
      project.newVertexAttribute(params("idattr"), project.vertexSet.idAttribute)
    }
  })

  register("Split edges")(new ProjectTransformation(_) {
    params ++= List(
      Choice("rep", "Repetition attribute", options = project.edgeAttrList[Double]),
      Param("idx", "Index attribute name", defaultValue = "index"))

    def enabled =
      FEStatus.assert(project.edgeAttrList[Double].nonEmpty, "No Double edge attributes")
    def doSplit(doubleAttr: Attribute[Double]): graph_operations.SplitEdges.Output = {
      val op = graph_operations.SplitEdges()
      op(op.es, project.edgeBundle)(op.attr, doubleAttr.asLong).result
    }
    def apply() = {
      val rep = params("rep")
      val split = doSplit(project.edgeAttributes(rep).runtimeSafeCast[Double])

      project.pullBackEdges(
        project.edgeBundle, project.edgeAttributes.toIndexedSeq, split.newEdges, split.belongsTo)
      project.edgeAttributes(params("idx")) = split.indexAttr
    }
  })
}

package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.controllers.Operation
import com.lynxanalytics.biggraph.controllers.ProjectTransformation
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import play.api.libs.json

class UseSegmentationOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Context
  import Operation.Implicits._

  val category = Categories.UseSegmentationOperations

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Copy edges to base project")(new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {}
    override def visibleScalars =
      if (project.isSegmentation && project.edgeBundle != null) {
        val scalar = segmentationSizesProductSum(seg, parent)
        implicit val entityProgressManager = env.entityProgressManager
        List(ProjectViewer.feScalar(scalar, "num_copied_edges", "", Map()))
      } else {
        List()
      }
    def enabled = project.assertSegmentation &&
      project.hasEdgeBundle &&
      FEStatus.assert(parent.edgeBundle == null, "There are already edges on base project")
    def apply() = {
      val seg = project.asSegmentation
      val reverseBelongsTo = seg.belongsTo.reverse
      val induction = {
        val op = graph_operations.InducedEdgeBundle()
        op(op.srcMapping, reverseBelongsTo)(
          op.dstMapping, reverseBelongsTo)(
            op.edges, seg.edgeBundle).result
      }
      parent.edgeBundle = induction.induced
      for ((name, attr) <- seg.edgeAttributes) {
        parent.edgeAttributes(name) = attr.pullVia(induction.embedding)
      }
    }
  })

  register("Copy edges to segmentation")(new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {}
    def enabled = project.assertSegmentation &&
      FEStatus.assert(parent.edgeBundle != null, "No edges on base project")
    def apply() = {
      val induction = {
        val op = graph_operations.InducedEdgeBundle()
        op(op.srcMapping, seg.belongsTo)(op.dstMapping, seg.belongsTo)(op.edges, parent.edgeBundle).result
      }
      project.edgeBundle = induction.induced
      for ((name, attr) <- parent.edgeAttributes) {
        project.edgeAttributes(name) = attr.pullVia(induction.embedding)
      }
    }
  })

  register("Create edges from co-occurrence")(new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {}
    override def visibleScalars =
      if (project.isSegmentation) {
        val scalar = segmentationSizesSquareSum(seg, parent)
        implicit val entityProgressManager = env.entityProgressManager
        List(ProjectViewer.feScalar(scalar, "num_created_edges", "", Map()))
      } else {
        List()
      }

    def enabled =
      project.assertSegmentation &&
        FEStatus.assert(parent.edgeBundle == null, "Parent graph has edges already.")
    def apply() = {
      val op = graph_operations.EdgesFromSegmentation()
      val result = op(op.belongsTo, seg.belongsTo).result
      parent.edgeBundle = result.es
      for ((name, attr) <- project.vertexAttributes) {
        parent.edgeAttributes(s"${seg.segmentationName}_$name") = attr.pullVia(result.origin)
      }
    }
  })

  register("Sample edges from co-occurrence")(new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = params ++= List(
      NonNegDouble("probability", "Vertex pair selection probability", defaultValue = "0.001"),
      RandomSeed("seed", "Random seed"))
    override def visibleScalars =
      if (project.isSegmentation) {
        val scalar = segmentationSizesSquareSum(seg, parent)
        implicit val entityProgressManager = env.entityProgressManager
        List(ProjectViewer.feScalar(scalar, "num_total_edges", "", Map()))
      } else {
        List()
      }

    def enabled =
      project.assertSegmentation &&
        FEStatus.assert(parent.edgeBundle == null, "Parent graph has edges already.")
    def apply() = {
      val op = graph_operations.SampleEdgesFromSegmentation(
        params("probability").toDouble,
        params("seed").toLong)
      val result = op(op.belongsTo, seg.belongsTo).result
      parent.edgeBundle = result.es
      parent.edgeAttributes("multiplicity") = result.multiplicity
    }
  })

  register("Create edges from set overlaps")(new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters =
      params += NonNegInt("minOverlap", "Minimal overlap for connecting two segments", default = 3)
    def enabled = project.assertSegmentation
    def apply() = {
      val op = graph_operations.SetOverlap(params("minOverlap").toInt)
      val res = op(op.belongsTo, seg.belongsTo).result
      project.edgeBundle = res.overlaps
      // Long is better supported on the frontend than Int.
      project.edgeAttributes("Overlap size") = res.overlapSize.asLong
    }
  })

  register("Grow segmentation")(new ProjectTransformation(_) with SegOp {
    def enabled = project.assertSegmentation && project.hasVertexSet &&
      FEStatus.assert(parent.edgeBundle != null, "Parent has no edges.")

    def addSegmentationParameters =
      params += Choice("direction", "Direction", options = Direction.neighborOptions)

    def apply() = {
      val segmentation = project.asSegmentation
      val direction = Direction(params("direction"), parent.edgeBundle, reversed = true)

      val op = graph_operations.GrowSegmentation()
      segmentation.belongsTo = op(
        op.vsG, parent.vertexSet)(
          op.esG, direction.edgeBundle)(
            op.esGS, segmentation.belongsTo).result.esGS
    }
  })

  register(
    "Link project and segmentation by fingerprint")(new ProjectTransformation(_) with SegOp {
      def addSegmentationParameters = params ++= List(
        NonNegInt("mo", "Minimum overlap", default = 1),
        Ratio("ms", "Minimum similarity", defaultValue = "0.0"),
        Param(
          "extra",
          "Fingerprinting algorithm additional parameters",
          defaultValue = ""))
      def enabled =
        project.assertSegmentation &&
          project.hasEdgeBundle && FEStatus.assert(parent.edgeBundle != null, s"No edges on $parent")
      def apply() = {
        val mo = params("mo").toInt
        val ms = params("ms").toDouble

        // We are setting the stage here for the generic fingerprinting operation. For a vertex A
        // on the left (base project) side and a vertex B on the right (segmentation) side we
        // want to "create" a common neighbor for fingerprinting purposes iff a neighbor of A (A') is
        // connected to a neigbor of B (B'). In practice, to make the setup symmetric, we will
        // actually create two common neighbors, namely we will connect both A and B to A' and B'.
        //
        // There is one more twist, that we want to consider A being connected to B directly also
        // as an evidence for A and B being a good match. To achieve this, we basically artificially
        // make every vertex a member of its own neighborhood by adding loop edges.
        val leftWithLoops = parallelEdgeBundleUnion(parent.edgeBundle, parent.vertexSet.loops)
        val rightWithLoops = parallelEdgeBundleUnion(project.edgeBundle, project.vertexSet.loops)
        val fromLeftToRight = leftWithLoops.concat(seg.belongsTo)
        val fromRightToLeft = rightWithLoops.concat(seg.belongsTo.reverse)
        val leftEdges = generalEdgeBundleUnion(leftWithLoops, fromLeftToRight)
        val rightEdges = generalEdgeBundleUnion(rightWithLoops, fromRightToLeft)

        val candidates = {
          val op = graph_operations.FingerprintingCandidatesFromCommonNeighbors()
          op(op.leftEdges, leftEdges)(op.rightEdges, rightEdges).result.candidates
        }

        val fingerprinting = {
          // TODO: This is a temporary hack to facilitate experimentation with the underlying backend
          // operation w/o too much disruption to users. Should be removed once we are clear on what
          // we want to provide for fingerprinting.
          val baseParams = s""""minimumOverlap": $mo, "minimumSimilarity": $ms"""
          val extraParams = params("extra")
          val paramsJson = if (extraParams == "") baseParams else (baseParams + ", " + extraParams)
          val op = graph_operations.Fingerprinting.fromJson(json.Json.parse(s"{$paramsJson}"))
          op(
            op.leftEdges, leftEdges)(
              op.leftEdgeWeights, leftEdges.const(1.0))(
                op.rightEdges, rightEdges)(
                  op.rightEdgeWeights, rightEdges.const(1.0))(
                    op.candidates, candidates)
            .result
        }

        project.scalars("fingerprinting matches found") = fingerprinting.matching.countScalar
        seg.belongsTo = fingerprinting.matching
        parent.newVertexAttribute(
          "fingerprinting_similarity_score", fingerprinting.leftSimilarities)
        project.newVertexAttribute(
          "fingerprinting_similarity_score", fingerprinting.rightSimilarities)
      }
    })

  register("Merge parallel segmentation links")(new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {}
    def enabled = project.assertSegmentation
    def apply() = {
      val linksAsAttr = {
        val op = graph_operations.EdgeBundleAsAttribute()
        op(op.edges, seg.belongsTo).result.attr
      }
      val mergedResult = mergeEdges(linksAsAttr)
      val newLinks = {
        val op = graph_operations.PulledOverEdges()
        op(op.originalEB, seg.belongsTo)(op.injection, mergedResult.representative)
          .result.pulledEB
      }
      seg.belongsTo = newLinks
    }
  })

  register("Pull segmentation one level up")(new ProjectTransformation(_) with SegOp {
    def addSegmentationParameters = {}

    def enabled =
      project.assertSegmentation && FEStatus.assert(parent.isSegmentation, "Parent graph is not a segmentation")

    def apply() = {
      val parentSegmentation = parent.asSegmentation
      val thisSegmentation = project.asSegmentation
      val segmentationName = thisSegmentation.segmentationName
      val targetSegmentation = parentSegmentation.parent.segmentation(segmentationName)
      targetSegmentation.state = thisSegmentation.state
      targetSegmentation.belongsTo =
        parentSegmentation.belongsTo.concat(thisSegmentation.belongsTo)
    }
  })
}

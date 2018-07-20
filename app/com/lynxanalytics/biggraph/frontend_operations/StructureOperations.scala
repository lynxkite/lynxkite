package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.controllers.Operation
import com.lynxanalytics.biggraph.controllers.ProjectTransformation
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_api.Attribute
import com.lynxanalytics.biggraph.graph_api.Scalar

class StructureOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Context
  import Operation.Implicits._

  val category = Categories.StructureOperations

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

    override def cleanParametersImpl(params: Map[String, String]) = cleanAggregateParams(params)
  })

  register("Merge parallel edges by attribute")(new ProjectTransformation(_) {
    params += Choice("key", "Merge by", options = project.edgeAttrList)
    params ++= aggregateParams(project.edgeAttributes)
    def enabled = FEStatus.assert(
      project.edgeAttrList.nonEmpty,
      "There must be at least one edge attribute")

    def apply() = {
      applyMergeParallelEdges(project, params, byKey = true)
    }

    override def cleanParametersImpl(params: Map[String, String]) = cleanAggregateParams(params)
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
    override def cleanParametersImpl(params: Map[String, String]) = cleanAggregateParams(params)
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

  register("Single customer view step")(new ProjectTransformation(_) {
    params ++= List(
      Choice("weight_attr", "Weight attribute", options = project.edgeAttrList))
    def enabled = project.hasEdgeBundle &&
      FEStatus.assert(
        project.edgeAttrList[Double].nonEmpty,
        "needs at least 1 Double edge attribute which can serve as weight")
    override def summary = {
      s"Create single customer view"
    }

    def apply() = {
      import SingleCustomerViewUtils._

      val weightAttr = params("weight_attr")

      addReverseEdges(project)
      mergeParallelEdges(project, params, weightAttr)
      // We need to store the original edges and the weight attribute corresponding to it.
      val baseProject = copyProject(project)
      // For each vertex calculate the rank of the best non-negative edge adjacent to it. We use
      // this attribute for merging the vertices - this ensures that the edges we are contracting
      // form a matching.
      filterOutNegativeEdges(project, weightAttr)
      val rankAttribute = createEdgeRankAttribute(project, weightAttr)
      val bestEdgeRankVertexAttribute = createBestEdgeRankVertexAttribute(project, rankAttribute)
      val guidVertexAttribute = createGUIDVertexAttribute(project)
      val mergeByVertexAttribute = createMergedVertexAttribute(
        bestEdgeRankVertexAttribute, guidVertexAttribute)
      // Use the original EdgeBundle and weight EdgeAttribute for merging the vertices.
      project.edgeBundle = baseProject.edgeBundle
      project.edgeAttributes(weightAttr) = baseProject.edgeAttributes(weightAttr)
      project.newVertexAttribute("merge_by", mergeByVertexAttribute)
      mergeVertices(project, mergeByVertexAttribute)
    }

    override def cleanParametersImpl(params: Map[String, String]) = cleanAggregateParams(params)
  })

  object SingleCustomerViewUtils {
    def copyProject(project: ProjectEditor): ProjectEditor = {
      new RootProjectEditor(project.state.copy())
    }

    def addReverseEdges(project: ProjectEditor) {
      val rev = {
        val op = graph_operations.AddReversedEdges()
        op(op.es, project.edgeBundle).result
      }
      project.pullBackEdges(
        project.edgeBundle,
        project.edgeAttributes.toIndexedSeq,
        rev.esPlus,
        rev.newToOriginal)
    }

    def mergeParallelEdges(project: ProjectEditor, params: ParameterHolder, weightAttr: String): Unit = {
      val aggrWeightAttr = Param(s"aggregate_${weightAttr}", weightAttr, defaultValue = "sum")
      params += aggrWeightAttr
      applyMergeParallelEdges(project, params, byKey = false)
      // Rename ${weightAttr}_sum to ${weightAttr}.
      val weightAttrSum = s"${weightAttr}_sum"
      project.newEdgeAttribute(
        weightAttr,
        project.edgeAttributes(weightAttrSum))
      project.deleteEdgeAttribute(weightAttrSum)
    }

    def filterOutNegativeEdges(project: ProjectEditor, weightAttr: String) {
      val weightAttribute = project.edgeAttributes(weightAttr)
      val filter = Seq(FEVertexAttributeFilter(weightAttribute.gUID.toString, ">0"))
      val filteredEdges = FEFilters.embedFilteredVertices(
        project.edgeBundle.idSet, filter, heavy = true)
      project.pullBackEdges(filteredEdges)
    }

    def createEdgeRankAttribute(project: ProjectEditor, weightAttr: String): Attribute[Double] = {
      val weightAttribute = project.edgeAttributes(weightAttr)
      graph_operations.AddRankingAttribute.run(
        weightAttribute,
        ascending = false).asDouble
    }

    def createBestEdgeRankVertexAttribute(
      project: ProjectEditor, rankAttribute: Attribute[Double]): Attribute[Double] = {
      val direction = Direction("all edges", project.edgeBundle)
      val result = aggregateFromEdges(
        direction.edgeBundle,
        AttributeWithLocalAggregator(
          direction.pull(rankAttribute),
          "min"))
      result.runtimeSafeCast[Double]
    }

    def createGUIDVertexAttribute(project: ProjectEditor): Attribute[String] = {
      val expr = """Some("id_" + id)"""
      val vertexSet = project.vertexSet
      val namedAttributes = Seq(("id", vertexSet.idAttribute.asString))
      val namedScalars = Seq[(String, Scalar[_])]()
      graph_operations.DeriveScala.deriveAndInferReturnType(
        expr, namedAttributes, vertexSet, namedScalars).asString
    }

    def createMergedVertexAttribute(
      attr1: Attribute[_],
      attr2: Attribute[_]): Attribute[String] = {
      unifyAttribute(attr1.asString, attr2.asString).asString
    }

    def mergeVertices(
      project: ProjectEditor,
      mergeByAttribute: Attribute[String],
      paramsToVector: Seq[String] = Seq()) {
      val op = graph_operations.MergeVertices[String]()
      val m = op(op.attr, mergeByAttribute).result
      val oldVAttrs = project.vertexAttributes.toMap
      val oldEdges = project.edgeBundle
      val oldEAttrs = project.edgeAttributes.toMap
      project.setVertexSet(m.segments, idAttr = "id")
      val aggregateParams = paramsToVector.map(i => (i, "vector"))
      for ((attr, choice) <- aggregateParams) {
        val result = aggregateViaConnection(
          m.belongsTo,
          AttributeWithLocalAggregator(oldVAttrs(attr), choice))
        project.newVertexAttribute(s"${attr}_${choice}", result)
      }
      if (oldEdges != null) {
        val edgeInduction = {
          val op = graph_operations.InducedEdgeBundle()
          op(op.srcMapping, m.belongsTo)(op.dstMapping, m.belongsTo)(op.edges, oldEdges).result
        }
        val newEdges = edgeInduction.induced
        project.edgeBundle = edgeInduction.induced
        for ((name, eAttr) <- oldEAttrs) {
          project.edgeAttributes(name) = eAttr.pullVia(edgeInduction.embedding)
        }
      }
    }
  }
}

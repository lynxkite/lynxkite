// Frontend operations for taking a subgraph of an existing graph.
package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._

class SubgraphOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Category
  import Operation.Context
  import Operation.Implicits._

  val SubgraphOperations = Category("Subgraph", "blue")
  val defaultIcon = "hammer"

  def register(id: String)(factory: Context => ProjectTransformation): Unit = {
    registerOp(id, defaultIcon, SubgraphOperations, List(projectOutput), List(projectOutput), factory)
  }

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Create snowball sample")(new ProjectTransformation(_) {
    params ++= List(
      Ratio("ratio", "Fraction of vertices to use as starting points", defaultValue = "0.0001"),
      NonNegInt("radius", "Radius", default = 3),
      Param("attrName", "Attribute name", defaultValue = "distance_from_start_point"),
      RandomSeed("seed", "Seed"))
    def enabled = project.hasVertexSet && project.hasEdgeBundle
    def apply() = {
      val ratio = params("ratio")
      // Creating random attr for filtering the original center vertices of the "snowballs".
      val rnd = {
        val op = graph_operations.AddRandomAttribute(params("seed").toInt, "Standard Uniform")
        op(op.vs, project.vertexSet).result.attr
      }

      // Creating derived attribute based on rnd and ratio parameter.
      val startingDistance = rnd.deriveX[Double](s"x < ${ratio} ? 0.0 : undefined")

      // Constant unit length for all edges.
      val edgeLength = project.edgeBundle.const(1.0)

      // Running shortest path from vertices with attribute startingDistance.
      val distance = {
        val op = graph_operations.ShortestPath(params("radius").toInt)
        op(op.vs, project.vertexSet)(
          op.es, project.edgeBundle)(
            op.edgeDistance, edgeLength)(
              op.startingDistance, startingDistance).result.distance
      }
      project.newVertexAttribute(params("attrName"), distance)

      // Filtering on distance attribute.
      val guid = distance.entity.gUID.toString
      val vertexEmbedding = FEFilters.embedFilteredVertices(
        project.vertexSet, Seq(FEVertexAttributeFilter(guid, ">-1")), heavy = true)
      project.pullBack(vertexEmbedding)

    }
  })

  register("Sample graph by random walks")(new ProjectTransformation(_) {
    params ++= List(
      NonNegInt("startPoints", "Number of start points", default = 1),
      NonNegInt("walksFromOnePoint", "Number of walks from each start point", default = 10000),
      Ratio("walkAbortionProbability", "Walk abortion probability", defaultValue = "0.15"),
      Param("vertexAttrName", "Save vertex indices as", defaultValue = "first_reached"),
      Param("edgeAttrName", "Save edge indices as", defaultValue = "first_traversed"),
      RandomSeed("seed", "Seed"))
    def enabled = project.hasVertexSet && project.hasEdgeBundle

    def apply() = {
      val output = {
        val startPoints = params("startPoints").toInt
        val walksFromOnePoint = params("walksFromOnePoint").toInt
        val walkAbortionProbability = params("walkAbortionProbability").toDouble
        val seed = params("seed").toInt
        val op = graph_operations.RandomWalkSample(startPoints, walksFromOnePoint,
          walkAbortionProbability, seed)
        op(op.vs, project.vertexSet)(op.es, project.edgeBundle).result
      }
      project.newVertexAttribute(params("vertexAttrName"), output.vertexFirstVisited)
      project.newEdgeAttribute(params("edgeAttrName"), output.edgeFirstTraversed)
    }
  })

  register("Discard edges")(new ProjectTransformation(_) {
    def enabled = project.hasEdgeBundle
    def apply() = {
      project.edgeBundle = null
    }
  })

  register("Discard loop edges")(new ProjectTransformation(_) {
    def enabled = project.hasEdgeBundle
    def apply() = {
      val edgesAsAttr = {
        val op = graph_operations.EdgeBundleAsAttribute()
        op(op.edges, project.edgeBundle).result.attr
      }
      val guid = edgesAsAttr.entity.gUID.toString
      val embedding = FEFilters.embedFilteredVertices(
        project.edgeBundle.idSet,
        Seq(FEVertexAttributeFilter(guid, "!=")))
      project.pullBackEdges(embedding)
    }
  })

  register("Filter by attributes")(new ProjectTransformation(_) {
    params ++= project.vertexAttrList.map {
      attr => Param(s"filterva_${attr.id}", attr.id)
    }
    params ++= project.segmentations.map {
      seg =>
        Param(
          s"filterva_${seg.viewer.equivalentUIAttributeTitle}",
          seg.segmentationName)
    }
    params ++= project.edgeAttrList.map {
      attr => Param(s"filterea_${attr.id}", attr.id)
    }
    def enabled =
      FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes") ||
        FEStatus.assert(project.edgeAttrList.nonEmpty, "No edge attributes")
    val vaFilter = "filterva_(.*)".r
    val eaFilter = "filterea_(.*)".r

    override def summary = {
      val filterStrings = params.toMap.collect {
        case (vaFilter(name), filter) if filter.nonEmpty => s"$name $filter"
        case (eaFilter(name), filter) if filter.nonEmpty => s"$name $filter"
      }
      "Filter " + filterStrings.mkString(", ")
    }
    def apply() = {
      val vertexFilters = params.toMap.collect {
        case (vaFilter(name), filter) if filter.nonEmpty =>
          // The filter may be for a segmentation's equivalent attribute or for a vertex attribute.
          val segs = project.segmentations.map(_.viewer)
          val segGUIDOpt =
            segs.find(_.equivalentUIAttributeTitle == name).map(_.belongsToAttribute.gUID)
          val gUID = segGUIDOpt.getOrElse(project.vertexAttributes(name).gUID)
          FEVertexAttributeFilter(gUID.toString, filter)
      }.toSeq

      if (vertexFilters.nonEmpty) {
        val vertexEmbedding = FEFilters.embedFilteredVertices(
          project.vertexSet, vertexFilters, heavy = true)
        project.pullBack(vertexEmbedding)
      }
      val edgeFilters = params.toMap.collect {
        case (eaFilter(name), filter) if filter.nonEmpty =>
          val attr = project.edgeAttributes(name)
          FEVertexAttributeFilter(attr.gUID.toString, filter)
      }.toSeq
      assert(vertexFilters.nonEmpty || edgeFilters.nonEmpty, "No filters specified.")
      if (edgeFilters.nonEmpty) {
        val edgeEmbedding = FEFilters.embedFilteredVertices(
          project.edgeBundle.idSet, edgeFilters, heavy = true)
        project.pullBackEdges(edgeEmbedding)
      }
    }
  })
}

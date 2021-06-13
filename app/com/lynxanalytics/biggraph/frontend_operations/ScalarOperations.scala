package com.lynxanalytics.biggraph.frontend_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.controllers.Operation
import com.lynxanalytics.biggraph.controllers.ProjectTransformation
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util.Scripting._
import com.lynxanalytics.biggraph.controllers._
import com.lynxanalytics.biggraph.graph_api.EdgeBundleProperties
import com.lynxanalytics.biggraph.graph_api.Scalar

class ScalarOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Context
  import Operation.Implicits._

  val category = Categories.ScalarOperations

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Aggregate edge attribute globally")(new ProjectTransformation(_) {
    params ++= aggregateParams(project.edgeAttributes, needsGlobal = true)
    def enabled = project.hasEdgeBundle
    def apply() = {
      for ((attr, choice, name) <- parseAggregateParams(params)) {
        val result = aggregate(
          AttributeWithAggregator(project.edgeAttributes(attr), choice))
        project.scalars(name) = result
      }
    }
    override def cleanParametersImpl(params: Map[String, String]) = cleanAggregateParams(params)
  })

  register("Weighted aggregate edge attribute globally")(new ProjectTransformation(_) {
    params ++= List(
      Choice("weight", "Weight", options = project.edgeAttrList[Double])) ++
      aggregateParams(
        project.edgeAttributes,
        needsGlobal = true,
        weighted = true)
    def enabled =
      FEStatus.assert(project.edgeAttrList[Double].nonEmpty, "No numeric edge attributes")
    def apply() = {
      val weightName = params("weight")
      val weight = project.edgeAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice, name) <- parseAggregateParams(params, weight = weightName)) {
        val result = aggregate(
          AttributeWithWeightedAggregator(weight, project.edgeAttributes(attr), choice))
        project.scalars(name) = result
      }
    }
    override def cleanParametersImpl(params: Map[String, String]) = cleanAggregateParams(params)
  })

  register("Aggregate vertex attribute globally")(new ProjectTransformation(_) {
    params ++= aggregateParams(project.vertexAttributes, needsGlobal = true)
    def enabled = project.hasVertexSet
    def apply() = {
      for ((attr, choice, name) <- parseAggregateParams(params)) {
        val result = aggregate(AttributeWithAggregator(project.vertexAttributes(attr), choice))
        project.scalars(name) = result
      }
    }
    override def cleanParametersImpl(params: Map[String, String]) = cleanAggregateParams(params)
  })

  register("Weighted aggregate vertex attribute globally")(new ProjectTransformation(_) {
    params ++= List(
      Choice("weight", "Weight", options = project.vertexAttrList[Double])) ++
      aggregateParams(project.vertexAttributes, needsGlobal = true, weighted = true)
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes")
    def apply() = {
      val weightName = params("weight")
      val weight = project.vertexAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice, name) <- parseAggregateParams(params, weight = weightName)) {
        val result = aggregate(
          AttributeWithWeightedAggregator(weight, project.vertexAttributes(attr), choice))
        project.scalars(name) = result
      }
    }
    override def cleanParametersImpl(params: Map[String, String]) = cleanAggregateParams(params)
  })

  register("Compare segmentation edges")(new ProjectTransformation(_) {
    def isCompatibleSegmentation(segmentation: SegmentationEditor): Boolean = {
      return segmentation.edgeBundle != null &&
        segmentation.belongsTo.properties.compliesWith(EdgeBundleProperties.identity)
    }

    val possibleSegmentations = FEOption.list(
      project.segmentations
        .filter(isCompatibleSegmentation)
        .map { seg => seg.segmentationName }
        .toList)

    params ++= List(
      Choice("golden", "Golden segmentation", options = possibleSegmentations),
      Choice("test", "Test segmentation", options = possibleSegmentations))
    def enabled = FEStatus.assert(
      possibleSegmentations.size >= 2,
      "At least two segmentations are needed. Both should have edges " +
        "and both have to contain the same vertices as the base project. " +
        "(For example, use the copy graph into segmentation operation.)",
    )

    def apply() = {
      val golden = project.existingSegmentation(params("golden"))
      val test = project.existingSegmentation(params("test"))
      val op = graph_operations.CompareSegmentationEdges()
      val result = op(
        op.goldenBelongsTo,
        golden.belongsTo)(
        op.testBelongsTo,
        test.belongsTo)(
        op.goldenEdges,
        golden.edgeBundle)(
        op.testEdges,
        test.edgeBundle).result
      test.scalars("precision") = result.precision
      test.scalars("recall") = result.recall
      test.edgeAttributes("present_in_" + golden.segmentationName) = result.presentInGolden
      golden.edgeAttributes("present_in_" + test.segmentationName) = result.presentInTest
    }
  })

  register("Copy graph attribute from other graph", List("destination", "source"))(
    new ProjectOutputOperation(_) {
      override lazy val project = projectInput("destination")
      lazy val them = projectInput("source")
      params ++= List(
        Choice("graph_attribute", "Name of the graph attribute to copy", options = them.scalarList),
        Param("save_as", "Save as"))
      def enabled = FEStatus.assert(them.scalarNames.nonEmpty, "No graph attributes found.")
      def apply() = {
        val origName = params("graph_attribute")
        val newName = params("save_as")
        val scalarName = if (newName.isEmpty) origName else newName
        project.scalars(scalarName) = them.scalars(origName)
      }
    })

  register("Correlate two attributes")(new ProjectTransformation(_) {
    params ++= List(
      Choice("attrA", "First attribute", options = project.vertexAttrList[Double]),
      Choice("attrB", "Second attribute", options = project.vertexAttrList[Double]))
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes")
    def apply() = {
      val attrA = project.vertexAttributes(params("attrA")).runtimeSafeCast[Double]
      val attrB = project.vertexAttributes(params("attrB")).runtimeSafeCast[Double]
      val op = graph_operations.CorrelateAttributes()
      val res = op(op.attrA, attrA)(op.attrB, attrB).result
      project.scalars(s"correlation of ${params("attrA")} and ${params("attrB")}") =
        res.correlation
    }
  })

  register("Derive graph attribute")(new ProjectTransformation(_) {
    params ++= List(
      Param("output", "Save as"),
      Code("expr", "Value", defaultValue = "", language = "scala"))
    def enabled = FEStatus.enabled
    override def summary = {
      val name = params("output")
      val expr = params("expr")
      s"Derive graph attribute: $name = $expr"
    }
    def apply() = {
      val expr = params("expr")
      val namedScalars = ScalaUtilities.collectIdentifiers[Scalar[_]](project.scalars, expr)
      val result = graph_operations.DeriveScalaScalar.deriveAndInferReturnType(expr, namedScalars)
      project.newScalar(params("output"), result, expr + help)
    }
  })
}

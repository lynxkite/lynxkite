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
  import Operation.Category
  import Operation.Context
  import Operation.Implicits._

  val ScalarOperations = Category("Scalars", "blue")
  val defaultIcon = "glyphicon-globe"

  def register(id: String)(factory: Context => ProjectTransformation): Unit = {
    registerOp(id, defaultIcon, ScalarOperations, List(projectOutput),
      List(projectOutput), factory)
  }

  def register(id: String, inputs: List[String])(factory: Context => ProjectOutputOperation): Unit = {
    registerOp(id, defaultIcon, ScalarOperations, List(projectOutput),
      List(projectOutput), factory)
  }

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Aggregate edge attribute globally")(new ProjectTransformation(_) {
    params += Param("prefix", "Generated name prefix")
    params ++= aggregateParams(project.edgeAttributes, needsGlobal = true)
    def enabled =
      FEStatus.assert(project.edgeAttrList.nonEmpty, "No edge attributes")
    def apply() = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(
          AttributeWithAggregator(project.edgeAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}"
        project.scalars(name) = result
      }
    }
  })

  register("Weighted aggregate edge attribute globally")(new ProjectTransformation(_) {
    params ++= List(
      Param("prefix", "Generated name prefix"),
      Choice("weight", "Weight", options = project.edgeAttrList[Double])) ++
      aggregateParams(
        project.edgeAttributes,
        needsGlobal = true, weighted = true)
    def enabled =
      FEStatus.assert(project.edgeAttrList[Double].nonEmpty, "No numeric edge attributes")
    def apply() = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val weightName = params("weight")
      val weight = project.edgeAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(
          AttributeWithWeightedAggregator(weight, project.edgeAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}_by_${weightName}"
        project.scalars(name) = result
      }
    }
  })

  register("Aggregate vertex attribute globally")(new ProjectTransformation(_) {
    params += Param("prefix", "Generated name prefix")
    params ++= aggregateParams(project.vertexAttributes, needsGlobal = true)
    def enabled =
      FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes")
    def apply() = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(AttributeWithAggregator(project.vertexAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}"
        project.scalars(name) = result
      }
    }
  })

  register("Weighted aggregate vertex attribute globally")(new ProjectTransformation(_) {
    params ++= List(
      Param("prefix", "Generated name prefix"),
      Choice("weight", "Weight", options = project.vertexAttrList[Double])) ++
      aggregateParams(project.vertexAttributes, needsGlobal = true, weighted = true)
    def enabled =
      FEStatus.assert(project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes")
    def apply() = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val weightName = params("weight")
      val weight = project.vertexAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(
          AttributeWithWeightedAggregator(weight, project.vertexAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}_by_${weightName}"
        project.scalars(name) = result
      }
    }
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
        "(For example, use the copy graph into segmentation operation.)")

    def apply() = {
      val golden = project.existingSegmentation(params("golden"))
      val test = project.existingSegmentation(params("test"))
      val op = graph_operations.CompareSegmentationEdges()
      val result = op(
        op.goldenBelongsTo, golden.belongsTo)(
          op.testBelongsTo, test.belongsTo)(
            op.goldenEdges, golden.edgeBundle)(
              op.testEdges, test.edgeBundle).result
      test.scalars("precision") = result.precision
      test.scalars("recall") = result.recall
      test.edgeAttributes("present_in_" + golden.segmentationName) = result.presentInGolden
      golden.edgeAttributes("present_in_" + test.segmentationName) = result.presentInTest
    }
  })

  register("Copy scalar from other project", List("project", "scalar"))(
    new ProjectOutputOperation(_) {
      override lazy val project = projectInput("project")
      lazy val them = projectInput("scalar")
      params ++= List(
        Choice("scalar", "Name of the scalar to copy", options = them.scalarList),
        Param("save_as", "Save as"))
      def enabled = FEStatus.assert(them.scalarNames.nonEmpty, "No scalars found.")
      def apply() = {
        val origName = params("scalar")
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

  register("Derive scalar")(new ProjectTransformation(_) {
    params ++= List(
      Param("output", "Save as"),
      Choice("type", "Result type", options = FEOption.list("Double", "String")),
      Code("expr", "Value", defaultValue = "", language = "javascript"))
    def enabled = FEStatus.enabled
    override def summary = {
      val name = params("output")
      val expr = params("expr")
      s"Derive scalar: $name = $expr"
    }
    def apply() = {
      val expr = params("expr")
      val namedScalars = JSUtilities.collectIdentifiers[Scalar[_]](project.scalars, expr)
      val result = params("type") match {
        case "String" =>
          graph_operations.DeriveJSScalar.deriveFromScalars[String](expr, namedScalars)
        case "Double" =>
          graph_operations.DeriveJSScalar.deriveFromScalars[Double](expr, namedScalars)
      }
      project.newScalar(params("output"), result.sc, expr + help)
    }
  })
}

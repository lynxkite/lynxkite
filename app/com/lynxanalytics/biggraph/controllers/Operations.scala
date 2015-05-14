// "Frontend" operations are all defined here.
//
// The code in this file defines the operation parameters to be offered on the UI,
// and also takes care of parsing the parameters given by the user and creating
// the "backend" operations and updating the projects.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.JavaScript
import com.lynxanalytics.biggraph.graph_util.HadoopFile
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_util
import play.api.libs.json

object OperationParams {
  case class Param(
      id: String,
      title: String,
      defaultValue: String = "") extends OperationParameterMeta {
    val kind = "default"
    val options = List()
    val multipleChoice = false
    def validate(value: String): Unit = {}
  }
  case class Choice(
      id: String,
      title: String,
      options: List[UIValue],
      multipleChoice: Boolean = false) extends OperationParameterMeta {
    val kind = "choice"
    val defaultValue = ""
    def validate(value: String): Unit = {}
  }
  case class TagList(id: String, title: String, options: List[UIValue])
      extends OperationParameterMeta {
    val kind = "tag-list"
    val multipleChoice = true
    val defaultValue = ""
    def validate(value: String): Unit = {}
  }
  case class File(id: String, title: String) extends OperationParameterMeta {
    val kind = "file"
    val multipleChoice = false
    val defaultValue = ""
    val options = List()
    def validate(value: String): Unit = {}
  }
  case class Ratio(id: String, title: String, defaultValue: String = "")
      extends OperationParameterMeta {
    val kind = "default"
    val options = List()
    val multipleChoice = false
    def validate(value: String): Unit = {
      assert((value matches """\d+(\.\d+)?""") && (value.toDouble <= 1.0),
        s"$title ($value) has to be a ratio, a double between 0.0 and 1.0")
    }
  }
  case class NonNegInt(id: String, title: String, default: Int)
      extends OperationParameterMeta {
    val kind = "default"
    val defaultValue = default.toString
    val options = List()
    val multipleChoice = false
    def validate(value: String): Unit = {
      assert(value matches """\d+""", s"$title ($value) has to be a non negative integer")
    }
  }
  case class NonNegDouble(id: String, title: String, defaultValue: String = "")
      extends OperationParameterMeta {
    val kind = "default"
    val options = List()
    val multipleChoice = false
    def validate(value: String): Unit = {
      assert(value matches """\d+(\.\d+)?""", s"$title ($value) has to be a non negative double")
    }
  }

  // A random number to be used as default value for random seed parameters.
  case class RandomSeed(id: String, title: String) extends OperationParameterMeta {
    val defaultValue = util.Random.nextInt.toString
    val kind = "default"
    val options = List()
    val multipleChoice = false
    def validate(value: String): Unit = {
      assert(value matches """[+-]?\d+""", s"$title ($value) has to be an integer")
    }
  }
}
class Operations(env: BigGraphEnvironment) extends OperationRepository(env) {
  import Operation.Category
  import Operation.Context
  // Categories.
  abstract class VertexOperation(t: String, c: Context)
    extends Operation(t, c, Category("Vertex operations", "blue"))
  abstract class EdgeOperation(t: String, c: Context)
    extends Operation(t, c, Category("Edge operations", "orange"))
  abstract class AttributeOperation(t: String, c: Context)
    extends Operation(t, c, Category("Attribute operations", "yellow"))
  abstract class CreateSegmentationOperation(t: String, c: Context)
    extends Operation(t, c, Category("Create segmentation", "green"))
  abstract class UtilityOperation(t: String, c: Context)
    extends Operation(t, c, Category("Utility operations", "green", icon = "wrench", sortKey = "zz"))
  trait SegOp extends Operation {
    protected def seg = project.asSegmentation
    protected def parent = seg.parent
  }
  abstract class SegmentationUtilityOperation(t: String, c: Context)
    extends Operation(t, c, Category(
      "Segmentation utility operations",
      "green",
      visible = c.project.isSegmentation,
      icon = "wrench",
      sortKey = "zz")) with SegOp
  abstract class SegmentationOperation(t: String, c: Context)
    extends Operation(t, c, Category(
      "Segmentation operations",
      "yellow",
      visible = c.project.isSegmentation)) with SegOp
  abstract class SegmentationWorkflowOperation(t: String, c: Context)
    extends Operation(t, c, Category(
      "Workflows on segmentation",
      "magenta",
      visible = c.project.isSegmentation)) with SegOp

  import OperationParams._

  register("Discard vertices", new VertexOperation(_, _) {
    def parameters = List()
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      project.vertexSet = null
    }
  })

  register("Discard edges", new EdgeOperation(_, _) {
    def parameters = List()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      project.edgeBundle = null
    }
  })

  register("New vertex set", new VertexOperation(_, _) {
    def parameters = List(
      NonNegInt("size", "Vertex set size", default = 10))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val result = graph_operations.CreateVertexSet(params("size").toLong)().result
      project.setVertexSet(result.vs, idAttr = "id")
      project.vertexAttributes("ordinal") = result.ordinal
    }
  })

  register("Create random edge bundle", new EdgeOperation(_, _) {
    def parameters = List(
      NonNegDouble("degree", "Average degree", defaultValue = "10.0"),
      RandomSeed("seed", "Seed"))
    def enabled = hasVertexSet && hasNoEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.FastRandomEdgeBundle(
        params("seed").toInt, params("degree").toDouble)
      project.edgeBundle = op(op.vs, project.vertexSet).result.es
    }
  })

  register("Create scale-free random edge bundle", new EdgeOperation(_, _) {
    def parameters = List(
      NonNegInt("iterations", "Number of iterations", default = 10),
      NonNegDouble(
        "perIterationMultiplier",
        "Per iteration edge number multiplier",
        defaultValue = "1.3"),
      RandomSeed("seed", "Seed"))
    def enabled = hasVertexSet && hasNoEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.ScaleFreeEdgeBundle(
        params("iterations").toInt,
        params("seed").toLong,
        params("perIterationMultiplier").toDouble)
      project.edgeBundle = op(op.vs, project.vertexSet).result.es
    }
  })

  register("Connect vertices on attribute", new EdgeOperation(_, _) {
    def parameters = List(
      Choice("fromAttr", "Source attribute", options = vertexAttributes),
      Choice("toAttr", "Destination attribute", options = vertexAttributes))
    def enabled =
      (hasVertexSet && hasNoEdgeBundle
        && FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes."))
    private def applyAA[A](fromAttr: Attribute[A], toAttr: Attribute[A]) = {
      if (fromAttr == toAttr) {
        // Use the slightly faster operation.
        val op = graph_operations.EdgesFromAttributeMatches[A]()
        project.edgeBundle = op(op.attr, fromAttr).result.edges
      } else {
        val op = graph_operations.EdgesFromBipartiteAttributeMatches[A]()
        project.edgeBundle = op(op.fromAttr, fromAttr)(op.toAttr, toAttr).result.edges
      }
    }
    private def applyAB[A, B](fromAttr: Attribute[A], toAttr: Attribute[B]) = {
      applyAA(fromAttr, toAttr.asInstanceOf[Attribute[A]])
    }
    def apply(params: Map[String, String]) = {
      val fromAttrName = params("fromAttr")
      val toAttrName = params("toAttr")
      val fromAttr = project.vertexAttributes(fromAttrName)
      val toAttr = project.vertexAttributes(toAttrName)
      assert(fromAttr.typeTag.tpe =:= toAttr.typeTag.tpe,
        s"$fromAttrName and $toAttrName are not of the same type.")
      applyAB(fromAttr, toAttr)
    }
  })

  trait RowReader {
    def sourceParameters: List[OperationParameterMeta]
    def source(params: Map[String, String]): graph_operations.RowInput
  }

  trait CSVRowReader extends RowReader {
    def sourceParameters = List(
      File("files", "Files"),
      Param("header", "Header", defaultValue = "<read first line>"),
      Param("delimiter", "Delimiter", defaultValue = ","),
      Param("filter", "(optional) Filtering expression"))
    def source(params: Map[String, String]) = {
      val files = HadoopFile(params("files"))
      val header = if (params("header") == "<read first line>")
        graph_operations.ImportUtil.header(files) else params("header")
      graph_operations.CSV(
        files,
        params("delimiter"),
        header,
        JavaScript(params("filter")))
    }
  }

  trait SQLRowReader extends RowReader {
    def sourceParameters = List(
      Param("db", "Database"),
      Param("table", "Table or view"),
      Param("columns", "Columns"),
      Param("key", "Key column"))
    def source(params: Map[String, String]) = {
      val columns = params("columns").split(",", -1).map(_.trim).filter(_.nonEmpty)
      graph_operations.DBTable(
        params("db"),
        params("table"),
        (columns.toSet + params("key")).toSeq, // Always include "key".
        params("key"))
    }
  }

  abstract class ImportVerticesOperation(t: String, c: Context)
      extends VertexOperation(t, c) with RowReader {
    def parameters = sourceParameters ++ List(
      Param("id-attr", "ID attribute name", defaultValue = "id"))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val imp = graph_operations.ImportVertexList(source(params))().result
      project.vertexSet = imp.vertices
      project.vertexAttributes = imp.attrs.mapValues(_.entity)
      val idAttr = params("id-attr")
      assert(
        !project.vertexAttributes.contains(idAttr),
        s"The input also contains a field called '$idAttr'. Please pick a different name.")
      project.vertexAttributes(idAttr) = idAsAttribute(project.vertexSet)
    }
  }
  register("Import vertices from CSV files",
    new ImportVerticesOperation(_, _) with CSVRowReader)
  register("Import vertices from a database",
    new ImportVerticesOperation(_, _) with SQLRowReader)

  abstract class ImportEdgesForExistingVerticesOperation(t: String, c: Context)
      extends VertexOperation(t, c) with RowReader {
    def parameters = sourceParameters ++ List(
      Choice("attr", "Vertex ID attribute", options = vertexAttributes[String]),
      Param("src", "Source ID field"),
      Param("dst", "Destination ID field"))
    def enabled =
      hasNoEdgeBundle &&
        hasVertexSet &&
        FEStatus.assert(vertexAttributes[String].nonEmpty, "No vertex attributes to use as id.")
    def apply(params: Map[String, String]) = {
      val src = params("src")
      val dst = params("dst")
      val attr = project.vertexAttributes(params("attr")).runtimeSafeCast[String]
      val op = graph_operations.ImportEdgeListForExistingVertexSet(source(params), src, dst)
      val imp = op(op.srcVidAttr, attr)(op.dstVidAttr, attr).result
      project.edgeBundle = imp.edges
      project.edgeAttributes = imp.attrs.mapValues(_.entity)
    }
  }
  register("Import edges for existing vertices from CSV files",
    new ImportEdgesForExistingVerticesOperation(_, _) with CSVRowReader)
  register("Import edges for existing vertices from a database",
    new ImportEdgesForExistingVerticesOperation(_, _) with SQLRowReader)

  abstract class ImportVerticesAndEdgesOperation(t: String, c: Context)
      extends VertexOperation(t, c) with RowReader {
    def parameters = sourceParameters ++ List(
      Param("src", "Source ID field"),
      Param("dst", "Destination ID field"))
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val src = params("src")
      val dst = params("dst")
      val imp = graph_operations.ImportEdgeList(source(params), src, dst)().result
      project.setVertexSet(imp.vertices, idAttr = "id")
      project.vertexAttributes("stringID") = imp.stringID
      project.edgeBundle = imp.edges
      project.edgeAttributes = imp.attrs.mapValues(_.entity)
    }
  }
  register("Import vertices and edges from single CSV fileset",
    new ImportVerticesAndEdgesOperation(_, _) with CSVRowReader)
  register("Import vertices and edges from single database table",
    new ImportVerticesAndEdgesOperation(_, _) with SQLRowReader)

  register("Convert vertices into edges", new EdgeOperation(_, _) {
    def parameters = List(
      Choice("src", "Source", options = vertexAttributes[String]),
      Choice("dst", "Destination", options = vertexAttributes[String]))
    def enabled = hasNoEdgeBundle &&
      FEStatus.assert(vertexAttributes[String].size > 2, "Two string attributes are needed.")
    def apply(params: Map[String, String]) = {
      val srcAttr = project.vertexAttributes(params("src")).runtimeSafeCast[String]
      val dstAttr = project.vertexAttributes(params("dst")).runtimeSafeCast[String]
      val newGraph = {
        val op = graph_operations.VerticesToEdges()
        op(op.srcAttr, srcAttr)(op.dstAttr, dstAttr).result
      }
      val oldAttrs = project.vertexAttributes.toMap
      project.vertexSet = newGraph.vs
      project.edgeBundle = newGraph.es
      project.vertexAttributes("stringID") = newGraph.stringID
      for ((name, attr) <- oldAttrs) {
        project.edgeAttributes(name) =
          graph_operations.PulledOverVertexAttribute.pullAttributeVia(
            attr, newGraph.embedding)
      }
    }
  })

  abstract class ImportVertexAttributesOperation(t: String, c: Context)
      extends VertexOperation(t, c) with RowReader {
    def parameters = sourceParameters ++ List(
      Choice("id-attr", "Vertex ID attribute", options = vertexAttributes[String]),
      Param("id-field", "ID field"),
      Param("prefix", "Name prefix for the imported vertex attributes"))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      val idAttr = project.vertexAttributes(params("id-attr")).runtimeSafeCast[String]
      val op = graph_operations.ImportAttributesForExistingVertexSet(source(params), params("id-field"))
      val res = op(op.idAttr, idAttr).result
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((name, attr) <- res.attrs) {
        project.vertexAttributes(prefix + name) = attr
      }
    }
  }
  register("Import vertex attributes from CSV files",
    new ImportVertexAttributesOperation(_, _) with CSVRowReader)
  register("Import vertex attributes from a database",
    new ImportVertexAttributesOperation(_, _) with SQLRowReader)

  register("Maximal cliques", new CreateSegmentationOperation(_, _) {
    def parameters = List(
      Param("name", "Segmentation name", defaultValue = "maximal_cliques"),
      Choice("bothdir", "Edges required in both directions", options = UIValue.list(List("true", "false"))),
      NonNegInt("min", "Minimum clique size", default = 3))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.FindMaxCliques(params("min").toInt, params("bothdir").toBoolean)
      val result = op(op.es, project.edgeBundle).result
      val segmentation = project.segmentation(params("name"))
      segmentation.project.setVertexSet(result.segments, idAttr = "id")
      segmentation.project.notes = title
      segmentation.belongsTo = result.belongsTo
      segmentation.project.vertexAttributes("size") = computeSegmentSizes(segmentation)
    }
  })

  register("Check cliques", new SegmentationUtilityOperation(_, _) {
    def parameters = List(
      Param("selected", "Segment IDs to check", defaultValue = "<All>"),
      Choice("bothdir", "Edges required in both directions", options = UIValue.list(List("true", "false"))))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      val selected =
        if (params("selected") == "<All>") None
        else Some(params("selected").split(",", -1).map(_.toLong).toSet)
      val op = graph_operations.CheckClique(selected, params("bothdir").toBoolean)
      val result = op(op.es, parent.edgeBundle)(op.belongsTo, seg.belongsTo).result
      parent.scalars("invalid_cliques") = result.invalid
    }
  })

  register("Connected components", new CreateSegmentationOperation(_, _) {
    def parameters = List(
      Param("name", "Segmentation name", defaultValue = "connected_components"),
      Choice(
        "directions",
        "Edge direction",
        options = UIValue.list(List("ignore directions", "require both directions"))))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val symmetric = params("directions") match {
        case "ignore directions" => addReversed(project.edgeBundle)
        case "require both directions" => removeNonSymmetric(project.edgeBundle)
      }
      val op = graph_operations.ConnectedComponents()
      val result = op(op.es, symmetric).result
      val segmentation = project.segmentation(params("name"))
      segmentation.project.setVertexSet(result.segments, idAttr = "id")
      segmentation.project.notes = title
      segmentation.belongsTo = result.belongsTo
      segmentation.project.vertexAttributes("size") = computeSegmentSizes(segmentation)
    }
  })

  register("Find infocom communities", new CreateSegmentationOperation(_, _) {
    def parameters = List(
      Param(
        "cliques_name", "Name for maximal cliques segmentation", defaultValue = "maximal_cliques"),
      Param(
        "communities_name", "Name for communities segmentation", defaultValue = "communities"),
      Choice("bothdir", "Edges required in cliques in both directions", options = UIValue.list(List("true", "false"))),
      NonNegInt("min_cliques", "Minimum clique size", default = 3),
      Ratio("adjacency_threshold", "Adjacency threshold for clique overlaps", defaultValue = "0.6"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val cliquesResult = {
        val op = graph_operations.FindMaxCliques(
          params("min_cliques").toInt, params("bothdir").toBoolean)
        op(op.es, project.edgeBundle).result
      }

      val cliquesSegmentation = project.segmentation(params("cliques_name"))
      cliquesSegmentation.project.setVertexSet(cliquesResult.segments, idAttr = "id")
      cliquesSegmentation.project.notes = "Maximal cliques of %s".format(project.projectName)
      cliquesSegmentation.belongsTo = cliquesResult.belongsTo
      cliquesSegmentation.project.vertexAttributes("size") =
        computeSegmentSizes(cliquesSegmentation)

      val cedges = {
        val op = graph_operations.InfocomOverlapForCC(params("adjacency_threshold").toDouble)
        op(op.belongsTo, cliquesResult.belongsTo).result.overlaps
      }

      val ccResult = {
        val op = graph_operations.ConnectedComponents()
        op(op.es, cedges).result
      }

      val weightedVertexToClique = const(cliquesResult.belongsTo)
      val weightedCliqueToCommunity = const(ccResult.belongsTo)

      val vertexToCommunity = {
        val op = graph_operations.ConcatenateBundles()
        op(
          op.edgesAB, cliquesResult.belongsTo)(
            op.edgesBC, ccResult.belongsTo)(
              op.weightsAB, weightedVertexToClique)(
                op.weightsBC, weightedCliqueToCommunity).result.edgesAC
      }

      val communitiesSegmentation = project.segmentation(params("communities_name"))
      communitiesSegmentation.project.setVertexSet(ccResult.segments, idAttr = "id")
      communitiesSegmentation.project.notes =
        "Infocom Communities of %s".format(project.projectName)
      communitiesSegmentation.belongsTo = vertexToCommunity
      communitiesSegmentation.project.vertexAttributes("size") =
        computeSegmentSizes(communitiesSegmentation)
    }
  })

  register("Modular clustering", new CreateSegmentationOperation(_, _) {
    def parameters = List(
      Param("name", "Segmentation name", defaultValue = "modular_clusters"),
      Choice("weights", "Weight attribute", options =
        UIValue("!no weight", "no weight") +: edgeAttributes[Double]))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val edgeBundle = project.edgeBundle
      val weightsName = params("weights")
      val weights =
        if (weightsName == "!no weight") const(edgeBundle)
        else project.edgeAttributes(weightsName).runtimeSafeCast[Double]
      val result = {
        val op = graph_operations.FindModularClusteringByTweaks()
        op(op.edges, edgeBundle)(op.weights, weights).result
      }
      val segmentation = project.segmentation(params("name"))
      segmentation.project.setVertexSet(result.clusters, idAttr = "id")
      segmentation.project.notes = title
      segmentation.belongsTo = result.belongsTo
      segmentation.project.vertexAttributes("size") =
        computeSegmentSizes(segmentation)

      val symmetricDirection = Direction("all edges", project.edgeBundle)
      val symmetricEdges = symmetricDirection.edgeBundle
      val symmetricWeights = symmetricDirection.pull(weights)
      val modularity = {
        val op = graph_operations.Modularity()
        op(op.edges, symmetricEdges)(op.weights, symmetricWeights)(op.belongsTo, result.belongsTo)
          .result.modularity
      }
      segmentation.project.scalars("modularity") = modularity
    }
  })

  register("Segment by double attribute", new CreateSegmentationOperation(_, _) {
    def parameters = List(
      Param("name", "Segmentation name", defaultValue = "bucketing"),
      Choice("attr", "Attribute", options = vertexAttributes[Double]),
      NonNegDouble("interval-size", "Interval size"),
      Choice("overlap", "Overlap", options = UIValue.list(List("no", "yes"))))
    def enabled = FEStatus.assert(vertexAttributes[Double].nonEmpty, "No double vertex attributes.")
    override def summary(params: Map[String, String]) = {
      val attrName = params("attr")
      val overlap = params("overlap") == "yes"
      s"Segmentation by $attrName" + (if (overlap) " with overlap" else "")
    }

    def apply(params: Map[String, String]) = {
      val attrName = params("attr")
      val attr = project.vertexAttributes(attrName).runtimeSafeCast[Double]
      val overlap = params("overlap") == "yes"
      val intervalSize = params("interval-size").toDouble
      val bucketing = {
        val op = graph_operations.DoubleBucketing(intervalSize, overlap)
        op(op.attr, attr).result
      }
      val segmentation = project.segmentation(params("name"))
      segmentation.project.setVertexSet(bucketing.segments, idAttr = "id")
      segmentation.project.notes = summary(params)
      segmentation.belongsTo = bucketing.belongsTo
      segmentation.project.vertexAttributes("size") =
        computeSegmentSizes(segmentation)
      segmentation.project.vertexAttributes(s"bottom") = bucketing.bottom
      segmentation.project.vertexAttributes(s"top") = bucketing.top
    }
  })

  register("Segment by string attribute", new CreateSegmentationOperation(_, _) {
    def parameters = List(
      Param("name", "Segmentation name", defaultValue = "bucketing"),
      Choice("attr", "Attribute", options = vertexAttributes[String]))
    def enabled = FEStatus.assert(vertexAttributes[String].nonEmpty, "No string vertex attributes.")
    override def summary(params: Map[String, String]) = {
      val attrName = params("attr")
      s"Segmentation by $attrName"
    }

    def apply(params: Map[String, String]) = {
      val attrName = params("attr")
      val attr = project.vertexAttributes(attrName).runtimeSafeCast[String]
      val bucketing = {
        val op = graph_operations.StringBucketing()
        op(op.attr, attr).result
      }
      val segmentation = project.segmentation(params("name"))
      segmentation.project.setVertexSet(bucketing.segments, idAttr = "id")
      segmentation.project.notes = summary(params)
      segmentation.belongsTo = bucketing.belongsTo
      segmentation.project.vertexAttributes("size") =
        computeSegmentSizes(segmentation)
      segmentation.project.vertexAttributes(attrName) = bucketing.label
    }
  })

  register("Combine segmentations", new CreateSegmentationOperation(_, _) {
    def parameters = List(
      Param("name", "New segmentation name"),
      Choice("segmentations", "Segmentations", options = segmentations, multipleChoice = true))
    def enabled = FEStatus.assert(segmentations.nonEmpty, "No segmentations")
    override def summary(params: Map[String, String]) = {
      val segmentations = params("segmentations").split(",").mkString(", ")
      s"Combination of $segmentations"
    }

    def apply(params: Map[String, String]) = {
      val segmentations = params("segmentations").split(",").map(project.segmentation(_))
      assert(segmentations.size >= 2, "Please select at least 2 segmentations to combine.")
      val result = project.segmentation(params("name"))
      // Start by copying the first segmentation.
      val first = segmentations.head
      result.project.setVertexSet(first.project.vertexSet, idAttr = "id")
      result.project.notes = summary(params)
      result.belongsTo = first.belongsTo
      for ((name, attr) <- first.project.vertexAttributes) {
        result.project.vertexAttributes(s"${first.name}_$name") = attr
      }
      // Then combine the other segmentations one by one.
      for (seg <- segmentations.tail) {
        val combination = {
          val op = graph_operations.CombineSegmentations()
          op(op.belongsTo1, result.belongsTo)(op.belongsTo2, seg.belongsTo).result
        }
        val attrs = result.project.vertexAttributes.toMap
        result.project.setVertexSet(combination.segments, idAttr = "id")
        result.belongsTo = combination.belongsTo
        for ((name, attr) <- attrs) {
          // These names are already prefixed.
          result.project.vertexAttributes(name) =
            graph_operations.PulledOverVertexAttribute.pullAttributeVia(
              attr, combination.origin1)
        }
        for ((name, attr) <- seg.project.vertexAttributes) {
          // Add prefix for the new attributes.
          result.project.vertexAttributes(s"${seg.name}_$name") =
            graph_operations.PulledOverVertexAttribute.pullAttributeVia(
              attr, combination.origin2)
        }
      }
      // Calculate sizes at the end.
      result.project.vertexAttributes("size") =
        computeSegmentSizes(result)
    }
  })

  register("Internal vertex ID as attribute", new AttributeOperation(_, _) {
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "id"))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      project.vertexAttributes(params("name")) = idAsAttribute(project.vertexSet)
    }
  })

  def idAsAttribute(vs: VertexSet) = {
    graph_operations.IdAsAttribute.run(vs)
  }

  register("Add gaussian vertex attribute", new AttributeOperation(_, _) {
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "random"),
      RandomSeed("seed", "Seed"))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.AddGaussianVertexAttribute(params("seed").toInt)
      project.vertexAttributes(params("name")) = op(op.vertices, project.vertexSet).result.attr
    }
  })

  register("Add constant edge attribute", new AttributeOperation(_, _) {
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "weight"),
      Param("value", "Value", defaultValue = "1"),
      Choice("type", "Type", options = UIValue.list(List("Double", "String"))))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val res = {
        if (params("type") == "Double") {
          const(project.edgeBundle, params("value").toDouble)
        } else {
          graph_operations.AddConstantAttribute.run(project.edgeBundle.idSet, params("value"))
        }
      }
      project.edgeAttributes(params("name")) = res
    }
  })

  register("Add constant vertex attribute", new AttributeOperation(_, _) {
    def parameters = List(
      Param("name", "Attribute name"),
      Param("value", "Value", defaultValue = "1"),
      Choice("type", "Type", options = UIValue.list(List("Double", "String"))))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op: graph_operations.AddConstantAttribute[_] =
        graph_operations.AddConstantAttribute.doubleOrString(
          isDouble = (params("type") == "Double"), params("value"))
      project.vertexAttributes(params("name")) = op(op.vs, project.vertexSet).result.attr
    }
  })

  register("Fill with constant default value", new AttributeOperation(_, _) {
    def parameters = List(
      Choice("attr", "Vertex attribute", options = vertexAttributes[String] ++ vertexAttributes[Double]),
      Param("def", "Default value"))
    def enabled = FEStatus.assert(
      (vertexAttributes[String] ++ vertexAttributes[Double]).nonEmpty, "No vertex attributes.")
    def apply(params: Map[String, String]) = {
      val attr = project.vertexAttributes(params("attr"))
      val op: graph_operations.AddConstantAttribute[_] =
        graph_operations.AddConstantAttribute.doubleOrString(
          isDouble = attr.is[Double], params("def"))
      val default = op(op.vs, project.vertexSet).result
      project.vertexAttributes(params("attr")) = unifyAttribute(attr, default.attr.entity)
    }
  })

  register("Merge two attributes", new AttributeOperation(_, _) {
    def parameters = List(
      Param("name", "New attribute name", defaultValue = ""),
      Choice("attr1", "Primary attribute", options = vertexAttributes),
      Choice("attr2", "Secondary attribute", options = vertexAttributes))
    def enabled = FEStatus.assert(
      vertexAttributes.size >= 2, "Not enough vertex attributes.")
    def apply(params: Map[String, String]) = {
      val name = params("name")
      assert(name.nonEmpty, "You must specify a name for the new attribute.")
      val attr1 = project.vertexAttributes(params("attr1"))
      val attr2 = project.vertexAttributes(params("attr2"))
      assert(attr1.typeTag.tpe =:= attr2.typeTag.tpe,
        "The two attributes must have the same type.")
      project.vertexAttributes(name) = unifyAttribute(attr1, attr2)
    }
  })

  register("Reverse edge direction", new EdgeOperation(_, _) {
    def parameters = List()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.ReverseEdges()
      val res = op(op.esAB, project.edgeBundle).result
      project.pullBackEdges(
        project.edgeBundle,
        project.edgeAttributes.toIndexedSeq,
        res.esBA,
        res.injection)
    }
  })

  register("Add reversed edges", new EdgeOperation(_, _) {
    def parameters = List()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val dir = Direction("all edges", project.edgeBundle)
      project.pullBackEdges(
        project.edgeBundle,
        project.edgeAttributes.toIndexedSeq,
        dir.edgeBundle,
        dir.pullBundleOpt.get)
    }
  })

  register("Clustering coefficient", new AttributeOperation(_, _) {
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "clustering_coefficient"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.ClusteringCoefficient()
      project.vertexAttributes(params("name")) = op(op.es, project.edgeBundle).result.clustering
    }
  })

  register("Embeddedness", new AttributeOperation(_, _) {
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "embeddedness"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.Embeddedness()
      project.edgeAttributes(params("name")) = op(op.es, project.edgeBundle).result.embeddedness
    }
  })

  register("Dispersion", new AttributeOperation(_, _) {
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "dispersion"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val dispersion = {
        val op = graph_operations.Dispersion()
        op(op.es, project.edgeBundle).result.dispersion.entity
      }
      val embeddedness = {
        val op = graph_operations.Embeddedness()
        op(op.es, project.edgeBundle).result.embeddedness.entity
      }
      // http://arxiv.org/pdf/1310.6753v1.pdf
      var normalizedDispersion = {
        val op = graph_operations.DeriveJSDouble(
          JavaScript("Math.pow(disp, 0.61) / (emb + 5)"),
          Seq("disp", "emb"))
        op(op.attrs, graph_operations.VertexAttributeToJSValue.seq(
          dispersion, embeddedness)).result.attr.entity
      }
      // TODO: recursive dispersion
      project.edgeAttributes(params("name")) = dispersion
      project.edgeAttributes("normalized_" + params("name")) = normalizedDispersion
    }
  })

  register("Degree", new AttributeOperation(_, _) {
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "degree"),
      Choice("direction", "Count", options = Direction.options))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val es = Direction(params("direction"), project.edgeBundle, reversed = true).edgeBundle
      val op = graph_operations.OutDegree()
      project.vertexAttributes(params("name")) = op(op.es, es).result.outDegree
    }
  })

  register("PageRank", new AttributeOperation(_, _) {
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "page_rank"),
      Choice("weights", "Weight attribute",
        options = UIValue("!no weight", "no weight") +: edgeAttributes[Double]),
      NonNegInt("iterations", "Number of iterations", default = 5),
      Ratio("damping", "Damping factor", defaultValue = "0.85"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.PageRank(params("damping").toDouble, params("iterations").toInt)
      val weights =
        if (params("weights") == "!no weight") const(project.edgeBundle)
        else project.edgeAttributes(params("weights")).runtimeSafeCast[Double]
      project.vertexAttributes(params("name")) =
        op(op.es, project.edgeBundle)(op.weights, weights).result.pagerank
    }
  })

  register("Centrality", new AttributeOperation(_, _) {
    def parameters = List(
      Param("name", "Attribute name", defaultValue = "harmonic_centrality"),
      NonNegInt("maxDiameter", "Maximal diameter to check", default = 10))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.HyperBallCentrality(params("maxDiameter").toInt)
      project.vertexAttributes(params("name")) =
        op(op.es, project.edgeBundle).result.harmonicCentrality
    }
  })

  register("Add rank attribute", new AttributeOperation(_, _) {
    def parameters = List(
      Param("rankattr", "Rank attribute name", defaultValue = "ranking"),
      Choice("keyattr", "Key attribute name", options = vertexAttributes[Double]),
      Choice("order", "Order", options = UIValue.list(List("ascending", "descending"))))

    def enabled = FEStatus.assert(vertexAttributes[Double].nonEmpty, "No numeric (double) vertex attributes")
    def apply(params: Map[String, String]) = {
      val keyAttr = params("keyattr")
      val rankAttr = params("rankattr")
      val ascending = params("order") == "ascending"
      assert(keyAttr.nonEmpty, "Please set a key attribute name.")
      assert(rankAttr.nonEmpty, "Please set a name for the rank attribute")
      val op = graph_operations.AddRankingAttributeDouble(ascending)
      val sortKey = project.vertexAttributes(keyAttr).runtimeSafeCast[Double]
      project.vertexAttributes(rankAttr) = toDouble(op(op.sortKey, sortKey).result.ordinal)
    }
  })

  register("Example Graph", new VertexOperation(_, _) {
    def parameters = List()
    def enabled = hasNoVertexSet
    def apply(params: Map[String, String]) = {
      val g = graph_operations.ExampleGraph()().result
      project.vertexSet = g.vertices
      project.edgeBundle = g.edges
      project.vertexAttributes = g.vertexAttributes.mapValues(_.entity)
      project.vertexAttributes("id") = idAsAttribute(project.vertexSet)
      project.edgeAttributes = g.edgeAttributes.mapValues(_.entity)
    }
  })

  private val toStringHelpText = "Converts the selected %s attributes to string type."
  register("Vertex attribute to string", new AttributeOperation(_, _) {
    def parameters = List(
      Choice("attr", "Vertex attribute", options = vertexAttributes, multipleChoice = true))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes.")
    def apply(params: Map[String, String]) = {
      for (attr <- params("attr").split(",", -1)) {
        project.vertexAttributes(attr) = attributeToString(project.vertexAttributes(attr))
      }
    }
  })

  register("Edge attribute to string", new AttributeOperation(_, _) {
    def parameters = List(
      Choice("attr", "Edge attribute", options = edgeAttributes, multipleChoice = true))
    def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes.")
    def apply(params: Map[String, String]) = {
      for (attr <- params("attr").split(",", -1)) {
        project.edgeAttributes(attr) = attributeToString(project.edgeAttributes(attr))
      }
    }
  })

  private val toDoubleHelpText =
    """Converts the selected string typed %s attributes to double (double precision floating point
    number) type.
    """
  register("Vertex attribute to double", new AttributeOperation(_, _) {
    val eligible = vertexAttributes[String] ++ vertexAttributes[Long]
    def parameters = List(
      Choice("attr", "Vertex attribute", options = eligible, multipleChoice = true))
    def enabled = FEStatus.assert(eligible.nonEmpty, "No eligible vertex attributes.")
    def apply(params: Map[String, String]) = {
      for (name <- params("attr").split(",", -1)) {
        val attr = project.vertexAttributes(name)
        project.vertexAttributes(name) = toDouble(attr)
      }
    }
  })

  register("Edge attribute to double", new AttributeOperation(_, _) {
    val eligible = edgeAttributes[String] ++ edgeAttributes[Long]
    def parameters = List(
      Choice("attr", "Edge attribute", options = eligible, multipleChoice = true))
    def enabled = FEStatus.assert(eligible.nonEmpty, "No eligible edge attributes.")
    def apply(params: Map[String, String]) = {
      for (name <- params("attr").split(",", -1)) {
        val attr = project.edgeAttributes(name)
        project.edgeAttributes(name) = toDouble(attr)
      }
    }
  })

  register("Vertex attributes to position", new AttributeOperation(_, _) {
    def parameters = List(
      Param("output", "Save as", defaultValue = "position"),
      Choice("x", "X or latitude", options = vertexAttributes[Double]),
      Choice("y", "Y or longitude", options = vertexAttributes[Double]))
    def enabled = FEStatus.assert(vertexAttributes[Double].nonEmpty, "No numeric vertex attributes.")
    def apply(params: Map[String, String]) = {
      assert(params("output").nonEmpty, "Please set an attribute name.")
      val pos = {
        val op = graph_operations.JoinAttributes[Double, Double]()
        val x = project.vertexAttributes(params("x")).runtimeSafeCast[Double]
        val y = project.vertexAttributes(params("y")).runtimeSafeCast[Double]
        op(op.a, x)(op.b, y).result.attr
      }
      project.vertexAttributes(params("output")) = pos
    }
  })

  register("Edge graph", new VertexOperation(_, _) {
    def parameters = List()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.EdgeGraph()
      val g = op(op.es, project.edgeBundle).result
      project.setVertexSet(g.newVS, idAttr = "id")
      project.edgeBundle = g.newES
    }
  })

  register("Derived vertex attribute", new AttributeOperation(_, _) {
    def parameters = List(
      Param("output", "Save as"),
      Choice("type", "Result type", options = UIValue.list(List("double", "string"))),
      Param("expr", "Value", defaultValue = "1"))
    def enabled = hasVertexSet
    def apply(params: Map[String, String]) = {
      assert(params("output").nonEmpty, "Please set an output attribute name.")
      val expr = params("expr")
      val vertexSet = project.vertexSet
      val namedAttributes = project.vertexAttributes
        .filter { case (name, attr) => containsIdentifierJS(expr, name) }
        .toIndexedSeq
      val result = params("type") match {
        case "string" =>
          graph_operations.DeriveJS.deriveFromAttributes[String](expr, namedAttributes, vertexSet)
        case "double" =>
          graph_operations.DeriveJS.deriveFromAttributes[Double](expr, namedAttributes, vertexSet)
      }
      project.vertexAttributes(params("output")) = result.attr
    }
  })

  register("Derived edge attribute", new AttributeOperation(_, _) {
    def parameters = List(
      Param("output", "Save as"),
      Choice("type", "Result type", options = UIValue.list(List("double", "string"))),
      Param("expr", "Value", defaultValue = "1"))
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val expr = params("expr")
      val edgeBundle = project.edgeBundle
      val idSet = project.edgeBundle.idSet
      val namedEdgeAttributes = project.edgeAttributes
        .filter { case (name, attr) => containsIdentifierJS(expr, name) }
        .toIndexedSeq
      val namedSrcVertexAttributes = project.vertexAttributes
        .filter { case (name, attr) => containsIdentifierJS(expr, "src$" + name) }
        .toIndexedSeq
        .map {
          case (name, attr) =>
            "src$" + name -> graph_operations.VertexToEdgeAttribute.srcAttribute(attr, edgeBundle)
        }
      val namedDstVertexAttributes = project.vertexAttributes
        .filter { case (name, attr) => containsIdentifierJS(expr, "dst$" + name) }
        .toIndexedSeq
        .map {
          case (name, attr) =>
            "dst$" + name -> graph_operations.VertexToEdgeAttribute.dstAttribute(attr, edgeBundle)
        }

      val namedAttributes =
        namedEdgeAttributes ++ namedSrcVertexAttributes ++ namedDstVertexAttributes

      val result = params("type") match {
        case "string" =>
          graph_operations.DeriveJS.deriveFromAttributes[String](expr, namedAttributes, idSet)
        case "double" =>
          graph_operations.DeriveJS.deriveFromAttributes[Double](expr, namedAttributes, idSet)
      }
      project.edgeAttributes(params("output")) = result.attr
    }
  })

  register("Aggregate to segmentation", new SegmentationOperation(_, _) {
    def parameters = aggregateParams(parent.vertexAttributes)
    def enabled =
      FEStatus.assert(parent.vertexAttributes.nonEmpty,
        "No vertex attributes on parent")
    def apply(params: Map[String, String]) = {
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          seg.belongsTo,
          AttributeWithLocalAggregator(parent.vertexAttributes(attr), choice))
        project.vertexAttributes(s"${attr}_${choice}") = result
      }
    }
  })

  register("Weighted aggregate to segmentation", new SegmentationOperation(_, _) {
    def parameters = List(
      Choice("weight", "Weight", options = vertexAttributes[Double])) ++
      aggregateParams(parent.vertexAttributes, weighted = true)
    def enabled =
      FEStatus.assert(parent.vertexAttributeNames[Double].nonEmpty,
        "No numeric vertex attributes on parent")
    def apply(params: Map[String, String]) = {
      val weightName = params("weight")
      val weight = parent.vertexAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          seg.belongsTo,
          AttributeWithWeightedAggregator(weight, parent.vertexAttributes(attr), choice))
        project.vertexAttributes(s"${attr}_${choice}_by_${weightName}") = result
      }
    }
  })

  register("Aggregate from segmentation", new SegmentationOperation(_, _) {
    def parameters = List(
      Param("prefix", "Generated name prefix",
        defaultValue = project.asSegmentation.name)) ++
      aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          reverse(seg.belongsTo),
          AttributeWithLocalAggregator(project.vertexAttributes(attr), choice))
        seg.parent.vertexAttributes(s"${prefix}${attr}_${choice}") = result
      }
    }
  })

  register("Weighted aggregate from segmentation", new SegmentationOperation(_, _) {
    def parameters = List(
      Param("prefix", "Generated name prefix",
        defaultValue = project.asSegmentation.name),
      Choice("weight", "Weight", options = vertexAttributes[Double])) ++
      aggregateParams(project.vertexAttributes, weighted = true)
    def enabled =
      FEStatus.assert(vertexAttributes[Double].nonEmpty, "No numeric vertex attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val weightName = params("weight")
      val weight = project.vertexAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          reverse(seg.belongsTo),
          AttributeWithWeightedAggregator(weight, project.vertexAttributes(attr), choice))
        seg.parent.vertexAttributes(s"${prefix}${attr}_${choice}_by_${weightName}") = result
      }
    }
  })

  register("Create edges from set overlaps", new SegmentationOperation(_, _) {
    def parameters = List(
      NonNegInt("minOverlap", "Minimal overlap for connecting two segments", default = 3))
    def enabled = hasNoEdgeBundle
    def apply(params: Map[String, String]) = {
      val op = graph_operations.SetOverlap(params("minOverlap").toInt)
      val res = op(op.belongsTo, seg.belongsTo).result
      project.edgeBundle = res.overlaps
      project.edgeAttributes("Overlap size") =
        // Long is better supported on the frontend.
        graph_operations.IntAttributeToLong.run(res.overlapSize)
    }
  })

  register("Create edges from co-occurrence", new SegmentationOperation(_, _) {
    def parameters = List()
    def enabled = FEStatus.assert(parent.edgeBundle == null, "Parent graph has edges already.")
    def apply(params: Map[String, String]) = {
      val op = graph_operations.EdgesFromSegmentation()
      val result = op(op.belongsTo, seg.belongsTo).result
      parent.edgeBundle = result.es
      for ((name, attr) <- project.vertexAttributes) {
        parent.edgeAttributes(s"${seg.name}_$name") =
          graph_operations.PulledOverVertexAttribute.pullAttributeVia(
            attr, result.origin)
      }
    }
  })

  register("Aggregate on neighbors", new AttributeOperation(_, _) {
    def parameters = List(
      Param("prefix", "Generated name prefix", defaultValue = "neighborhood"),
      Choice("direction", "Aggregate on", options = Direction.options)) ++
      aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes") && hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val edges = Direction(params("direction"), project.edgeBundle).edgeBundle
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateViaConnection(
          edges,
          AttributeWithLocalAggregator(project.vertexAttributes(attr), choice))
        project.vertexAttributes(s"${prefix}${attr}_${choice}") = result
      }
    }
  })

  register("Weighted aggregate on neighbors", new AttributeOperation(_, _) {
    def parameters = List(
      Param("prefix", "Generated name prefix", defaultValue = "neighborhood"),
      Choice("weight", "Weight", options = vertexAttributes[Double]),
      Choice("direction", "Aggregate on", options = Direction.options)) ++
      aggregateParams(project.vertexAttributes, weighted = true)
    def enabled =
      FEStatus.assert(vertexAttributes[Double].nonEmpty, "No numeric vertex attributes") && hasEdgeBundle
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val edges = Direction(params("direction"), project.edgeBundle).edgeBundle
      val weightName = params("weight")
      val weight = project.vertexAttributes(weightName).runtimeSafeCast[Double]
      for ((name, choice) <- parseAggregateParams(params)) {
        val attr = project.vertexAttributes(name)
        val result = aggregateViaConnection(
          edges,
          AttributeWithWeightedAggregator(weight, attr, choice))
        project.vertexAttributes(s"${prefix}${name}_${choice}_by_${weightName}") = result
      }
    }
  })

  register("Merge vertices by attribute", new VertexOperation(_, _) {
    def parameters = List(
      Choice("key", "Match by", options = vertexAttributes)) ++
      aggregateParams(project.vertexAttributes)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def merge[T](attr: Attribute[T]): graph_operations.MergeVertices.Output = {
      val op = graph_operations.MergeVertices[T]()
      op(op.attr, attr).result
    }
    def apply(params: Map[String, String]) = {
      val m = merge(project.vertexAttributes(params("key")))
      val oldVAttrs = project.vertexAttributes.toMap
      val oldEdges = project.edgeBundle
      val oldEAttrs = project.edgeAttributes.toMap
      project.setVertexSet(m.segments, idAttr = "id")
      // Always use most_common for the key attribute.
      val hack = "aggregate-" + params("key") -> "most_common"
      for ((attr, choice) <- parseAggregateParams(params + hack)) {
        val result = aggregateViaConnection(
          m.belongsTo,
          AttributeWithLocalAggregator(oldVAttrs(attr), choice))
        if (attr == params("key")) { // Don't actually add "_most_common" for the key.
          project.vertexAttributes(attr) = result
        } else {
          project.vertexAttributes(s"${attr}_${choice}") = result
        }
      }
      if (oldEdges != null) {
        val edgeInduction = {
          val op = graph_operations.InducedEdgeBundle()
          op(op.srcMapping, m.belongsTo)(op.dstMapping, m.belongsTo)(op.edges, oldEdges).result
        }
        project.edgeBundle = edgeInduction.induced
        for ((name, eAttr) <- oldEAttrs) {
          project.edgeAttributes(name) =
            graph_operations.PulledOverVertexAttribute.pullAttributeVia(
              eAttr, edgeInduction.embedding)
        }
      }
    }
  })

  private def mergeEdgesWithKey(edgesAsAttr: Attribute[(ID, ID)], keyAttr: Attribute[String]) = {
    val edgesAndKey: Attribute[((ID, ID), String)] = joinAttr(edgesAsAttr, keyAttr)
    val op = graph_operations.MergeVertices[((ID, ID), String)]()
    op(op.attr, edgesAndKey).result
  }

  private def mergeEdges(edgesAsAttr: Attribute[(ID, ID)]) = {
    val op = graph_operations.MergeVertices[(ID, ID)]()
    op(op.attr, edgesAsAttr).result
  }
  // Common code for operations "merge parallel edges" and "merge parallel edges by key"

  private def applyMergeParallelEdgesByKey(project: Project, params: Map[String, String]) = {

    val edgesAsAttr = {
      val op = graph_operations.EdgeBundleAsAttribute()
      op(op.edges, project.edgeBundle).result.attr
    }

    val haveKeyAttr = params.contains("key")

    val mergedResult =
      if (haveKeyAttr) {
        val keyAttr = project.edgeAttributes(params("key")).runtimeSafeCast[String]
        mergeEdgesWithKey(edgesAsAttr, keyAttr)
      } else {
        mergeEdges(edgesAsAttr)
      }

    val newEdges = {
      val op = graph_operations.PulledOverEdges()
      op(op.originalEB, project.edgeBundle)(op.injection, mergedResult.representative)
        .result.pulledEB
    }
    val oldAttrs = project.edgeAttributes.toMap
    project.edgeBundle = newEdges

    for ((attr, choice) <- parseAggregateParams(params)) {
      project.edgeAttributes(s"${attr}_${choice}") =
        aggregateViaConnection(
          mergedResult.belongsTo,
          AttributeWithLocalAggregator(oldAttrs(attr), choice))
    }
  }

  register("Merge parallel edges", new EdgeOperation(_, _) {
    def parameters = aggregateParams(project.edgeAttributes)
    def enabled = hasEdgeBundle

    def apply(params: Map[String, String]) = {
      applyMergeParallelEdgesByKey(project, params)
    }
  })

  register("Merge parallel edges by string attribute", new EdgeOperation(_, _) {
    def parameters = List(
      Choice("key", "Merge by", options = edgeAttributes[String])) ++
      aggregateParams(project.edgeAttributes)
    def enabled = FEStatus.assert(edgeAttributes[String].nonEmpty,
      "There must be at least one string edge attribute")

    def apply(params: Map[String, String]) = {
      applyMergeParallelEdgesByKey(project, params)
    }
  })

  register("Discard loop edges", new EdgeOperation(_, _) {
    def parameters = List()
    def enabled = hasEdgeBundle
    def apply(params: Map[String, String]) = {
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

  register("Aggregate vertex attribute globally", new AttributeOperation(_, _) {
    def parameters = List(Param("prefix", "Generated name prefix")) ++
      aggregateParams(project.vertexAttributes, needsGlobal = true)
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(AttributeWithAggregator(project.vertexAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}"
        project.scalars(name) = result
      }
    }
  })

  register("Weighted aggregate vertex attribute globally", new AttributeOperation(_, _) {
    def parameters = List(
      Param("prefix", "Generated name prefix"),
      Choice("weight", "Weight", options = vertexAttributes[Double])) ++
      aggregateParams(project.vertexAttributes, needsGlobal = true, weighted = true)
    def enabled =
      FEStatus.assert(vertexAttributes[Double].nonEmpty, "No numeric vertex attributes")
    def apply(params: Map[String, String]) = {
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

  register("Aggregate edge attribute globally", new AttributeOperation(_, _) {
    def parameters = List(Param("prefix", "Generated name prefix")) ++
      aggregateParams(
        project.edgeAttributes,
        needsGlobal = true)
    def enabled =
      FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]) = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregate(
          AttributeWithAggregator(project.edgeAttributes(attr), choice))
        val name = s"${prefix}${attr}_${choice}"
        project.scalars(name) = result
      }
    }
  })

  register("Weighted aggregate edge attribute globally", new AttributeOperation(_, _) {
    def parameters = List(
      Param("prefix", "Generated name prefix"),
      Choice("weight", "Weight", options = edgeAttributes[Double])) ++
      aggregateParams(
        project.edgeAttributes,
        needsGlobal = true, weighted = true)
    def enabled =
      FEStatus.assert(edgeAttributes[Double].nonEmpty, "No numeric edge attributes")
    def apply(params: Map[String, String]) = {
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

  register("Aggregate edge attribute to vertices", new AttributeOperation(_, _) {
    def parameters = List(
      Param("prefix", "Generated name prefix", defaultValue = "edge"),
      Choice("direction", "Aggregate on", options = Direction.attrOptions)) ++
      aggregateParams(
        project.edgeAttributes)
    def enabled =
      FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    def apply(params: Map[String, String]) = {
      val direction = Direction(params("direction"), project.edgeBundle)
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateFromEdges(
          direction.edgeBundle,
          AttributeWithLocalAggregator(
            direction.pull(project.edgeAttributes(attr)),
            choice))
        project.vertexAttributes(s"${prefix}${attr}_${choice}") = result
      }
    }
  })

  register("Weighted aggregate edge attribute to vertices", new AttributeOperation(_, _) {
    def parameters = List(
      Param("prefix", "Generated name prefix", defaultValue = "edge"),
      Choice("weight", "Weight", options = edgeAttributes[Double]),
      Choice("direction", "Aggregate on", options = Direction.attrOptions)) ++
      aggregateParams(
        project.edgeAttributes,
        weighted = true)
    def enabled =
      FEStatus.assert(edgeAttributes[Double].nonEmpty, "No numeric edge attributes")
    def apply(params: Map[String, String]) = {
      val direction = Direction(params("direction"), project.edgeBundle)
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      val weightName = params("weight")
      val weight = project.edgeAttributes(weightName).runtimeSafeCast[Double]
      for ((attr, choice) <- parseAggregateParams(params)) {
        val result = aggregateFromEdges(
          direction.edgeBundle,
          AttributeWithWeightedAggregator(
            direction.pull(weight),
            direction.pull(project.edgeAttributes(attr)),
            choice))
        project.vertexAttributes(s"${prefix}${attr}_${choice}_by_${weightName}") = result
      }
    }
  })

  register("No operation", new UtilityOperation(_, _) {
    def parameters = List()
    def enabled = FEStatus.enabled
    def apply(params: Map[String, String]) = {}
  })

  register("Discard edge attribute", new UtilityOperation(_, _) {
    def parameters = List(
      Choice("name", "Name", options = edgeAttributes))
    def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    override def summary(params: Map[String, String]) = {
      val name = params("name")
      s"Discard edge attribute: $name"
    }
    def apply(params: Map[String, String]) = {
      project.edgeAttributes(params("name")) = null
    }
  })

  register("Discard vertex attribute", new UtilityOperation(_, _) {
    def parameters = List(
      Choice("name", "Name", options = vertexAttributes))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    override def summary(params: Map[String, String]) = {
      val name = params("name")
      s"Discard vertex attribute: $name"
    }
    def apply(params: Map[String, String]) = {
      project.vertexAttributes(params("name")) = null
    }
  })

  register("Discard segmentation", new UtilityOperation(_, _) {
    def parameters = List(
      Choice("name", "Name", options = segmentations))
    def enabled = FEStatus.assert(segmentations.nonEmpty, "No segmentations")
    override def summary(params: Map[String, String]) = {
      val name = params("name")
      s"Discard segmentation: $name"
    }
    def apply(params: Map[String, String]) = {
      project.segmentation(params("name")).remove
    }
  })

  register("Discard scalar", new UtilityOperation(_, _) {
    def parameters = List(
      Choice("name", "Name", options = scalars))
    def enabled = FEStatus.assert(scalars.nonEmpty, "No scalars")
    override def summary(params: Map[String, String]) = {
      val name = params("name")
      s"Discard scalar: $name"
    }
    def apply(params: Map[String, String]) = {
      project.scalars(params("name")) = null
    }
  })

  register("Rename edge attribute", new UtilityOperation(_, _) {
    def parameters = List(
      Choice("from", "Old name", options = edgeAttributes),
      Param("to", "New name"))
    def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    override def summary(params: Map[String, String]) = {
      val from = params("from")
      val to = params("to")
      s"Rename edge attribute $from to $to"
    }
    def apply(params: Map[String, String]) = {
      assert(!project.edgeAttributes.contains(params("to")),
        s"""An edge-attribute named '${params("to")}' already exists,
            please discard it or choose another name""")
      project.edgeAttributes(params("to")) = project.edgeAttributes(params("from"))
      project.edgeAttributes(params("from")) = null
    }
  })

  register("Rename vertex attribute", new UtilityOperation(_, _) {
    def parameters = List(
      Choice("from", "Old name", options = vertexAttributes),
      Param("to", "New name"))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    override def summary(params: Map[String, String]) = {
      val from = params("from")
      val to = params("to")
      s"Rename vertex attribute $from to $to"
    }
    def apply(params: Map[String, String]) = {
      assert(!project.vertexAttributes.contains(params("to")),
        s"""A vertex-attribute named '${params("to")}' already exists,
            please discard it or choose another name""")
      assert(params("to").nonEmpty, "Please set the new attribute name.")
      project.vertexAttributes(params("to")) = project.vertexAttributes(params("from"))
      project.vertexAttributes(params("from")) = null
    }
  })

  register("Rename segmentation", new UtilityOperation(_, _) {
    def parameters = List(
      Choice("from", "Old name", options = segmentations),
      Param("to", "New name"))
    def enabled = FEStatus.assert(segmentations.nonEmpty, "No segmentations")
    override def summary(params: Map[String, String]) = {
      val from = params("from")
      val to = params("to")
      s"Rename segmentation $from to $to"
    }
    def apply(params: Map[String, String]) = {
      assert(!project.segmentations.contains(params("to")),
        s"""A segmentation named '${params("to")}' already exists,
            please discard it or choose another name""")
      project.segmentation(params("from")).rename(params("to"))
    }
  })

  register("Rename scalar", new UtilityOperation(_, _) {
    def parameters = List(
      Choice("from", "Old name", options = scalars),
      Param("to", "New name"))
    def enabled = FEStatus.assert(scalars.nonEmpty, "No scalars")
    override def summary(params: Map[String, String]) = {
      val from = params("from")
      val to = params("to")
      s"Rename scalar $from to $to"
    }
    def apply(params: Map[String, String]) = {
      assert(!project.scalars.contains(params("to")),
        s"""A scalar named '${params("to")}' already exists,
            please discard it or choose another name""")
      project.scalars(params("to")) = project.scalars(params("from"))
      project.scalars(params("from")) = null
    }
  })

  register("Copy edge attribute", new UtilityOperation(_, _) {
    def parameters = List(
      Choice("from", "Old name", options = edgeAttributes),
      Param("to", "New name"))
    def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    override def summary(params: Map[String, String]) = {
      val from = params("from")
      val to = params("to")
      s"Copy edge attribute $from to $to"
    }
    def apply(params: Map[String, String]) = {
      project.edgeAttributes(params("to")) = project.edgeAttributes(params("from"))
    }
  })

  register("Copy vertex attribute", new UtilityOperation(_, _) {
    def parameters = List(
      Choice("from", "Old name", options = vertexAttributes),
      Param("to", "New name"))
    def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes")
    override def summary(params: Map[String, String]) = {
      val from = params("from")
      val to = params("to")
      s"Copy vertex attribute $from to $to"
    }
    def apply(params: Map[String, String]) = {
      assert(params("to").nonEmpty, "Please set the new attribute name.")
      project.vertexAttributes(params("to")) = project.vertexAttributes(params("from"))
    }
  })

  register("Copy segmentation", new UtilityOperation(_, _) {
    def parameters = List(
      Choice("from", "Old name", options = segmentations),
      Param("to", "New name"))
    def enabled = FEStatus.assert(segmentations.nonEmpty, "No segmentations")
    override def summary(params: Map[String, String]) = {
      val from = params("from")
      val to = params("to")
      s"Copy segmentation $from to $to"
    }
    def apply(params: Map[String, String]) = {
      val from = project.segmentation(params("from"))
      val to = project.segmentation(params("to"))
      from.project.copy(to.project)
      to.belongsTo = from.belongsTo
    }
  })

  register("Copy scalar", new UtilityOperation(_, _) {
    def parameters = List(
      Choice("from", "Old name", options = scalars),
      Param("to", "New name"))
    def enabled = FEStatus.assert(scalars.nonEmpty, "No scalars")
    override def summary(params: Map[String, String]) = {
      val from = params("from")
      val to = params("to")
      s"Copy scalar $from to $to"
    }
    def apply(params: Map[String, String]) = {
      project.scalars(params("to")) = project.scalars(params("from"))
    }
  })

  register("Import project as segmentation", new CreateSegmentationOperation(_, _) {
    def parameters = List(
      Choice("them", "Other project's name", options = otherProjects))
    private def otherProjects = readableProjects.filter(_.id != project.projectName)
    def enabled =
      hasVertexSet &&
        FEStatus.assert(otherProjects.size > 0, "This is the only project")
    override def summary(params: Map[String, String]) = {
      val them = params("them")
      s"Import $them as segmentation"
    }
    def apply(params: Map[String, String]) = {
      val themName = params("them")
      assert(otherProjects.map(_.id).contains(themName), s"Unknown project: $themName")
      val them = Project(themName)
      assert(them.vertexSet != null, s"No vertex set in $them")
      val segmentation = project.segmentation(params("them"))
      them.copy(segmentation.project)
      val op = graph_operations.EmptyEdgeBundle()
      segmentation.belongsTo = op(op.src, project.vertexSet)(op.dst, them.vertexSet).result.eb
      segmentation.project.discardCheckpoints()
    }
  })

  abstract class LoadSegmentationLinksOperation(t: String, c: Context)
      extends SegmentationOperation(t, c) with RowReader {
    def parameters = sourceParameters ++ List(
      Choice(
        "base-id-attr",
        s"Identifying vertex attribute in $parent",
        options = UIValue.list(parent.vertexAttributeNames[String].toList)),
      Param("base-id-field", s"Identifying field for $parent"),
      Choice(
        "seg-id-attr",
        s"Identifying vertex attribute in $project",
        options = vertexAttributes[String]),
      Param("seg-id-field", s"Identifying field for $project"))
    def enabled =
      FEStatus.assert(
        vertexAttributes[String].nonEmpty, "No string vertex attributes in this segmentation") &&
        FEStatus.assert(
          parent.vertexAttributeNames[String].nonEmpty, "No string vertex attributes in parent")
    def apply(params: Map[String, String]) = {
      val baseIdAttr = parent.vertexAttributes(params("base-id-attr")).runtimeSafeCast[String]
      val segIdAttr = project.vertexAttributes(params("seg-id-attr")).runtimeSafeCast[String]
      val op = graph_operations.ImportEdgeListForExistingVertexSet(
        source(params), params("base-id-field"), params("seg-id-field"))
      seg.belongsTo = op(op.srcVidAttr, baseIdAttr)(op.dstVidAttr, segIdAttr).result.edges
    }
  }
  register("Load segmentation links from CSV",
    new LoadSegmentationLinksOperation(_, _) with CSVRowReader)
  register("Load segmentation links from a database",
    new LoadSegmentationLinksOperation(_, _) with SQLRowReader)

  register("Define segmentation links from matching attributes",
    new SegmentationOperation(_, _) {
      def parameters = List(
        Choice(
          "base-id-attr",
          s"Identifying vertex attribute in $parent",
          options = UIValue.list(parent.vertexAttributeNames[String].toList)),
        Choice(
          "seg-id-attr",
          s"Identifying vertex attribute in $project",
          options = vertexAttributes[String]))
      def enabled =
        FEStatus.assert(
          vertexAttributes[String].nonEmpty, "No string vertex attributes in this segmentation") &&
          FEStatus.assert(
            parent.vertexAttributeNames[String].nonEmpty, "No string vertex attributes in parent")
      def apply(params: Map[String, String]) = {
        val baseIdAttr = parent.vertexAttributes(params("base-id-attr")).runtimeSafeCast[String]
        val segIdAttr = project.vertexAttributes(params("seg-id-attr")).runtimeSafeCast[String]
        val op = graph_operations.EdgesFromBipartiteAttributeMatches[String]()
        seg.belongsTo = op(op.fromAttr, baseIdAttr)(op.toAttr, segIdAttr).result.edges
      }
    })

  register("Union with another project", new VertexOperation(_, _) {
    def parameters = List(
      Choice("other", "Other project's name", options = readableProjects),
      Param("id-attr", "ID attribute name", defaultValue = "new_id"))
    def enabled = hasVertexSet
    override def summary(params: Map[String, String]) = {
      val other = params("other")
      s"Union with $other"
    }
    def apply(params: Map[String, String]): Unit = {
      val otherName = params("other")
      assert(readableProjects.map(_.id).contains(otherName), s"Unknown project: $otherName")
      val other = Project(otherName)
      if (other.vertexSet == null) {
        // Nothing to do
        return
      }
      val vsUnion = {
        val op = graph_operations.VertexSetUnion(2)
        op(op.vss, Seq(project.vertexSet, other.vertexSet)).result
      }
      val newVertexAttributes = unifyAttributes(
        project.vertexAttributes
          .map {
            case (name, attr) =>
              name -> graph_operations.PulledOverVertexAttribute.pullAttributeVia(
                attr,
                reverse(vsUnion.injections(0)))
          },
        other.vertexAttributes
          .map {
            case (name, attr) =>
              name -> graph_operations.PulledOverVertexAttribute.pullAttributeVia(
                attr,
                reverse(vsUnion.injections(1)))
          })
      val ebInduced = Option(project.edgeBundle).map { eb =>
        val op = graph_operations.InducedEdgeBundle()
        val mapping = vsUnion.injections(0)
        op(op.srcMapping, mapping)(op.dstMapping, mapping)(op.edges, project.edgeBundle).result
      }
      val otherEbInduced = Option(other.edgeBundle).map { eb =>
        val op = graph_operations.InducedEdgeBundle()
        val mapping = vsUnion.injections(1)
        op(op.srcMapping, mapping)(op.dstMapping, mapping)(op.edges, other.edgeBundle).result
      }

      val (newEdgeBundle, myEbInjection, otherEbInjection): (EdgeBundle, EdgeBundle, EdgeBundle) =
        if (ebInduced.isDefined && !otherEbInduced.isDefined) {
          (ebInduced.get.induced, ebInduced.get.embedding, null)
        } else if (!ebInduced.isDefined && otherEbInduced.isDefined) {
          (otherEbInduced.get.induced, null, otherEbInduced.get.embedding)
        } else if (ebInduced.isDefined && otherEbInduced.isDefined) {
          val idUnion = {
            val op = graph_operations.VertexSetUnion(2)
            op(
              op.vss,
              Seq(ebInduced.get.induced.idSet, otherEbInduced.get.induced.idSet))
              .result
          }
          val ebUnion = {
            val op = graph_operations.EdgeBundleUnion(2)
            op(
              op.ebs, Seq(ebInduced.get.induced.entity, otherEbInduced.get.induced.entity))(
                op.injections, idUnion.injections.map(_.entity)).result.union
          }
          (ebUnion,
            concat(reverse(idUnion.injections(0).entity), ebInduced.get.embedding),
            concat(reverse(idUnion.injections(1).entity), otherEbInduced.get.embedding))
        } else {
          (null, null, null)
        }
      val newEdgeAttributes = unifyAttributes(
        project.edgeAttributes
          .map {
            case (name, attr) => {
              name -> graph_operations.PulledOverVertexAttribute.pullAttributeVia(
                attr,
                myEbInjection)
            }
          },
        other.edgeAttributes
          .map {
            case (name, attr) =>
              name -> graph_operations.PulledOverVertexAttribute.pullAttributeVia(
                attr,
                otherEbInjection)
          })

      project.vertexSet = vsUnion.union
      project.vertexAttributes = newVertexAttributes
      val idAttr = params("id-attr")
      assert(
        !project.vertexAttributes.contains(idAttr),
        s"The project already contains a field called '$idAttr'. Please pick a different name.")
      project.vertexAttributes(idAttr) = idAsAttribute(project.vertexSet)
      project.edgeBundle = newEdgeBundle
      project.edgeAttributes = newEdgeAttributes
    }
  })

  register("Fingerprinting based on attributes", new VertexOperation(_, _) {
    def parameters = List(
      Choice("leftName", "First ID attribute", options = vertexAttributes[String]),
      Choice("rightName", "Second ID attribute", options = vertexAttributes[String]),
      Choice("weights", "Edge weights",
        options = UIValue("!no weight", "no weight") +: edgeAttributes[Double]),
      NonNegDouble("mrew", "Minimum relative edge weight", defaultValue = "0.0"),
      NonNegInt("mo", "Minimum overlap", default = 1),
      Ratio("ms", "Minimum similarity", defaultValue = "0.5"))
    def enabled =
      hasEdgeBundle &&
        FEStatus.assert(vertexAttributes[String].size >= 2, "Two string attributes are needed.")
    def apply(params: Map[String, String]): Unit = {
      val mrew = params("mrew").toDouble
      val mo = params("mo").toInt
      val ms = params("ms").toDouble
      assert(mo >= 1, "Minimum overlap cannot be less than 1.")
      val leftName = project.vertexAttributes(params("leftName")).runtimeSafeCast[String]
      val rightName = project.vertexAttributes(params("rightName")).runtimeSafeCast[String]
      val weights =
        if (params("weights") == "!no weight") const(project.edgeBundle)
        else project.edgeAttributes(params("weights")).runtimeSafeCast[Double]

      // TODO: Calculate relative edge weight, filter the edge bundle and pull over the weights.
      assert(mrew == 0, "Minimum relative edge weight is not implemented yet.")

      val candidates = {
        val op = graph_operations.FingerprintingCandidates()
        op(op.es, project.edgeBundle)(op.leftName, leftName)(op.rightName, rightName)
          .result.candidates
      }
      val fingerprinting = {
        val op = graph_operations.Fingerprinting(mo, ms)
        op(
          op.leftEdges, project.edgeBundle)(
            op.leftEdgeWeights, weights)(
              op.rightEdges, project.edgeBundle)(
                op.rightEdgeWeights, weights)(
                  op.candidates, candidates)
          .result
      }
      val newLeftName = graph_operations.PulledOverVertexAttribute.pullAttributeVia(
        leftName, reverse(fingerprinting.matching))
      val newRightName = graph_operations.PulledOverVertexAttribute.pullAttributeVia(
        rightName, fingerprinting.matching)

      project.scalars("fingerprinting matches found") = count(fingerprinting.matching)
      project.vertexAttributes(params("leftName")) = unifyAttribute(newLeftName, leftName)
      project.vertexAttributes(params("rightName")) = unifyAttribute(newRightName, rightName)
      project.vertexAttributes(params("leftName") + " similarity score") = fingerprinting.leftSimilarities
      project.vertexAttributes(params("rightName") + " similarity score") = fingerprinting.rightSimilarities
    }
  })

  register("Copy vertex attributes from segmentation", new SegmentationOperation(_, _) {
    def parameters = List(
      Param("prefix", "Attribute name prefix", defaultValue = seg.name))
    def enabled =
      FEStatus.assert(vertexAttributes.size > 0, "No vertex attributes") &&
        FEStatus.assert(parent.vertexSet != null, s"No vertices on $parent") &&
        FEStatus.assert(seg.belongsTo.properties.isFunction,
          s"Vertices of $parent are not guaranteed to have only one edge to this segmentation")
    def apply(params: Map[String, String]): Unit = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((name, attr) <- project.vertexAttributes.toMap) {
        parent.vertexAttributes(prefix + name) =
          graph_operations.PulledOverVertexAttribute.pullAttributeVia(
            attr, seg.belongsTo)
      }
    }
  })

  register("Copy vertex attributes to segmentation", new SegmentationOperation(_, _) {
    def parameters = List(
      Param("prefix", "Attribute name prefix"))
    def enabled =
      hasVertexSet &&
        FEStatus.assert(parent.vertexAttributes.size > 0, "No vertex attributes on $parent") &&
        FEStatus.assert(seg.belongsTo.properties.isReversedFunction,
          s"Vertices of this segmentation are not guaranteed to have only one edge from $parent")
    def apply(params: Map[String, String]): Unit = {
      val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
      for ((name, attr) <- parent.vertexAttributes.toMap) {
        project.vertexAttributes(prefix + name) =
          graph_operations.PulledOverVertexAttribute.pullAttributeVia(
            attr, reverse(seg.belongsTo))
      }
    }
  })

  register("Fingerprinting between project and segmentation", new SegmentationOperation(_, _) {
    def parameters = List(
      NonNegDouble("mrew", "Minimum relative edge weight", defaultValue = "0.0"),
      NonNegInt("mo", "Minimum overlap", default = 1),
      Ratio("ms", "Minimum similarity", defaultValue = "0.5"))
    def enabled =
      hasEdgeBundle && FEStatus.assert(parent.edgeBundle != null, s"No edges on $parent")
    def apply(params: Map[String, String]): Unit = {
      val mrew = params("mrew").toDouble
      val mo = params("mo").toInt
      val ms = params("ms").toDouble

      // TODO: Calculate relative edge weight, filter the edge bundle and pull over the weights.
      assert(mrew == 0, "Minimum relative edge weight is not implemented yet.")

      val candidates = seg.belongsTo
      val segNeighborsInParent = concat(project.edgeBundle, reverse(seg.belongsTo))
      val fingerprinting = {
        val op = graph_operations.Fingerprinting(mo, ms)
        op(
          op.leftEdges, parent.edgeBundle)(
            op.leftEdgeWeights, const(parent.edgeBundle))(
              op.rightEdges, segNeighborsInParent)(
                op.rightEdgeWeights, const(segNeighborsInParent))(
                  op.candidates, candidates)
          .result
      }

      project.scalars("fingerprinting matches found") = count(fingerprinting.matching)
      seg.belongsTo = fingerprinting.matching
      parent.vertexAttributes("fingerprinting_similarity_score") = fingerprinting.leftSimilarities
      project.vertexAttributes("fingerprinting_similarity_score") = fingerprinting.rightSimilarities
    }
  })

  register("Change project notes", new UtilityOperation(_, _) {
    def parameters = List(
      Param("notes", "New contents"))
    def enabled = FEStatus.enabled
    def apply(params: Map[String, String]) = {
      project.notes = params("notes")
    }
  })

  register("Viral modeling", new SegmentationWorkflowOperation(_, _) {
    def parameters = List(
      Param("prefix", "Generated name prefix", defaultValue = "viral"),
      Choice("target", "Target attribute",
        options = UIValue.list(parentDoubleAttributes)),
      Ratio("test_set_ratio", "Test set ratio", defaultValue = "0.1"),
      RandomSeed("seed", "Random seed for test set selection"),
      NonNegDouble("max_deviation", "Maximal segment deviation", defaultValue = "1.0"),
      NonNegInt("min_num_defined", "Minimum number of defined attributes in a segment", default = 3),
      Ratio("min_ratio_defined", "Minimal ratio of defined attributes in a segment", defaultValue = "0.25"),
      NonNegInt("iterations", "Iterations", default = 3))
    def parentDoubleAttributes = parent.vertexAttributeNames[Double].toList
    def enabled = hasVertexSet &&
      FEStatus.assert(UIValue.list(parentDoubleAttributes).nonEmpty,
        "No numeric vertex attributes.")
    def apply(params: Map[String, String]) = {
      // partition target attribute to test and train sets
      val targetName = params("target")
      val target = parent.vertexAttributes(targetName).runtimeSafeCast[Double]
      val roles = {
        val op = graph_operations.CreateRole(params("test_set_ratio").toDouble, params("seed").toInt)
        op(op.vertices, target.vertexSet).result.role
      }
      val parted = {
        val op = graph_operations.PartitionAttribute[Double]()
        op(op.attr, target)(op.role, roles).result
      }
      val prefix = params("prefix")
      parent.vertexAttributes(s"${prefix}_roles") = roles
      parent.vertexAttributes(s"${prefix}_${targetName}_test") = parted.test
      var train = parted.train.entity
      val segSizes = computeSegmentSizes(seg)
      project.vertexAttributes("size") = segSizes
      val maxDeviation = params("max_deviation")

      val coverage = {
        val op = graph_operations.CountAttributes[Double]()
        op(op.attribute, train).result.count
      }
      parent.vertexAttributes(s"${prefix}_${targetName}_train") = train
      parent.scalars(s"$prefix $targetName coverage initial") = coverage

      var timeOfDefinition = {
        val op = graph_operations.DeriveJSDouble(JavaScript("0"), Seq("attr"))
        op(op.attrs, graph_operations.VertexAttributeToJSValue.seq(train)).result.attr.entity
      }

      // iterative prediction
      for (i <- 1 to params("iterations").toInt) {
        val segTargetAvg = {
          aggregateViaConnection(
            seg.belongsTo,
            AttributeWithLocalAggregator(train, "average"))
            .runtimeSafeCast[Double]
        }
        val segStdDev = {
          aggregateViaConnection(
            seg.belongsTo,
            AttributeWithLocalAggregator(train, "std_deviation"))
            .runtimeSafeCast[Double]
        }
        val segTargetCount = {
          aggregateViaConnection(
            seg.belongsTo,
            AttributeWithLocalAggregator(train, "count"))
            .runtimeSafeCast[Double]
        }
        val segStdDevDefined = {
          val op = graph_operations.DeriveJSDouble(
            JavaScript(s"""
                deviation <= $maxDeviation &&
                defined / ids >= ${params("min_ratio_defined")} &&
                defined >= ${params("min_num_defined")}
                ? deviation
                : undefined"""),
            Seq("deviation", "ids", "defined"))
          op(
            op.attrs,
            graph_operations.VertexAttributeToJSValue.seq(segStdDev, segSizes, segTargetCount))
            .result.attr
        }
        project.vertexAttributes(s"${prefix}_${targetName}_standard_deviation_after_iteration_$i") =
          segStdDev
        project.vertexAttributes(s"${prefix}_${targetName}_average_after_iteration_$i") =
          segTargetAvg // TODO: use median
        val predicted = {
          aggregateViaConnection(
            reverse(seg.belongsTo),
            AttributeWithWeightedAggregator(segStdDevDefined, segTargetAvg, "by_min_weight"))
            .runtimeSafeCast[Double]
        }
        train = unifyAttributeT(train, predicted)
        val partedTrain = {
          val op = graph_operations.PartitionAttribute[Double]()
          op(op.attr, train)(op.role, roles).result
        }
        val error = {
          val op = graph_operations.DeriveJSDouble(
            JavaScript("Math.abs(test - train)"), Seq("test", "train"))
          val mae = op(
            op.attrs,
            graph_operations.VertexAttributeToJSValue.seq(
              parted.test.entity, partedTrain.test.entity)).result.attr
          aggregate(AttributeWithAggregator(mae, "average"))
        }
        val coverage = {
          val op = graph_operations.CountAttributes[Double]()
          op(op.attribute, partedTrain.train).result.count
        }
        // the attribute we use for iteration can be defined on the test set as well
        parent.vertexAttributes(s"${prefix}_${targetName}_after_iteration_$i") = train
        parent.scalars(s"$prefix $targetName coverage after iteration $i") = coverage
        parent.scalars(s"$prefix $targetName mean absolute prediction error after iteration $i") =
          error

        timeOfDefinition = {
          val op = graph_operations.DeriveJSDouble(
            JavaScript(i.toString), Seq("attr"))
          val newDefinitions = op(
            op.attrs, graph_operations.VertexAttributeToJSValue.seq(train)).result.attr
          unifyAttributeT(timeOfDefinition, newDefinitions)
        }
      }
      parent.vertexAttributes(s"${prefix}_${targetName}_spread_over_iterations") = timeOfDefinition
      // TODO: in the end we should calculate with the fact that the real error where the
      // original attribute is defined is 0.0
    }
  })

  register("Correlate two attributes", new AttributeOperation(_, _) {
    def parameters = List(
      Choice("attrA", "First attribute", options = vertexAttributes[Double]),
      Choice("attrB", "Second attribute", options = vertexAttributes[Double]))
    def enabled =
      FEStatus.assert(vertexAttributes[Double].nonEmpty, "No numeric vertex attributes")
    def apply(params: Map[String, String]) = {
      val attrA = project.vertexAttributes(params("attrA")).runtimeSafeCast[Double]
      val attrB = project.vertexAttributes(params("attrB")).runtimeSafeCast[Double]
      val op = graph_operations.CorrelateAttributes()
      val res = op(op.attrA, attrA)(op.attrB, attrB).result
      project.scalars(s"correlation of ${params("attrA")} and ${params("attrB")}") =
        res.correlation
    }
  })

  register("Filter by attributes", new AttributeOperation(_, _) {
    def parameters =
      vertexAttributes.toList.map { attr => Param(s"filterva-${attr.id}", attr.id) } ++
        project.segmentations.toList.map { seg => Param(s"filterva-${seg.equivalentUIAttribute.title}", seg.name) } ++
        edgeAttributes.toList.map { attr => Param(s"filterea-${attr.id}", attr.id) }
    def enabled =
      FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes") ||
        FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes")
    val vaFilter = "filterva-(.*)".r
    val eaFilter = "filterea-(.*)".r

    override def summary(params: Map[String, String]) = {
      val filterStrings = params.collect {
        case (vaFilter(name), filter) if filter.nonEmpty => s"$name $filter"
        case (eaFilter(name), filter) if filter.nonEmpty => s"$name $filter"
      }
      "Filter " + filterStrings.mkString(", ")
    }
    def apply(params: Map[String, String]) = {
      val vertexFilters = params.collect {
        case (vaFilter(name), filter) if filter.nonEmpty =>
          // The filter may be for a segmentation's equivalent attribute or for a vertex attribute.
          val segAttrs = project.segmentations.map(_.equivalentUIAttribute)
          val segGUIDOpt = segAttrs.find(_.title == name).map(_.id)
          val gUID = segGUIDOpt.getOrElse(project.vertexAttributes(name).gUID)
          FEVertexAttributeFilter(gUID.toString, filter)
      }.toSeq
      val edgeFilters = params.collect {
        case (eaFilter(name), filter) if filter.nonEmpty =>
          val attr = project.edgeAttributes(name)
          FEVertexAttributeFilter(attr.gUID.toString, filter)
      }.toSeq
      assert(vertexFilters.nonEmpty || edgeFilters.nonEmpty, "No filters specified.")

      if (vertexFilters.nonEmpty) {
        val vertexEmbedding = FEFilters.embedFilteredVertices(
          project.vertexSet, vertexFilters, heavy = true)
        project.pullBack(vertexEmbedding)
      }
      if (edgeFilters.nonEmpty) {
        val edgeEmbedding = FEFilters.embedFilteredVertices(
          project.edgeBundle.idSet, edgeFilters, heavy = true)
        project.pullBackEdges(edgeEmbedding)
      }
    }
  })

  register("Save UI status as graph attribute", new UtilityOperation(_, _) {
    def parameters = List(
      // In the future we may want a special kind for this so that users don't see JSON.
      Param("scalarName", "Name of new graph attribute"),
      Param("uiStatusJson", "UI status as JSON"))

    def enabled = FEStatus.enabled

    def apply(params: Map[String, String]) = {
      import UIStatusSerialization._
      val uiStatusJson = json.Json.parse(params("uiStatusJson"))
      val uiStatus = json.Json.fromJson[UIStatus](uiStatusJson).get
      project.scalars(params("scalarName")) =
        graph_operations.CreateUIStatusScalar(uiStatus).result.created
    }
  })

  { // "Dirty operations", that is operations that use a data manager. Think twice if you really
    // need this before putting an operation here.
    implicit val dataManager = env.dataManager

    register("Export vertex attributes to file", new AttributeOperation(_, _) {
      override val dirty = true
      def parameters = List(
        Param("path", "Destination path", defaultValue = "<auto>"),
        Param("link", "Download link name", defaultValue = "vertex_attributes_csv"),
        Choice("attrs", "Attributes", options = vertexAttributes, multipleChoice = true),
        Choice("format", "File format", options = UIValue.list(List("CSV", "SQL dump"))))
      def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes.")
      def apply(params: Map[String, String]) = {
        assert(params("attrs").nonEmpty, "No attributes are selected for export.")
        val labels = params("attrs").split(",", -1)
        val attrs: Map[String, Attribute[_]] = labels.map {
          label => label -> project.vertexAttributes(label)
        }.toMap
        val path = getExportFilename(params("path"))
        params("format") match {
          case "CSV" =>
            val csv = graph_util.CSVExport.exportVertexAttributes(project.vertexSet, attrs)
            csv.saveToDir(path)
          case "SQL dump" =>
            val export = graph_util.SQLExport(project.projectName, project.vertexSet, attrs)
            export.saveAs(path)
        }
        project.scalars(params("link")) =
          downloadLink(path, project.projectName + "_" + params("link"))
      }
    })

    register("Export vertex attributes to database", new AttributeOperation(_, _) {
      override val dirty = true
      def parameters = List(
        Param("db", "Database"),
        Param("table", "Table"),
        Choice("attrs", "Attributes", options = vertexAttributes, multipleChoice = true),
        Choice("delete", "Overwrite table if it exists", options = UIValue.list(List("no", "yes"))))
      def enabled = FEStatus.assert(vertexAttributes.nonEmpty, "No vertex attributes.")
      def apply(params: Map[String, String]) = {
        assert(params("attrs").nonEmpty, "No attributes are selected for export.")
        val labels = params("attrs").split(",", -1)
        val attrs: Seq[(String, Attribute[_])] = labels.map {
          label => label -> project.vertexAttributes(label)
        }
        val export = graph_util.SQLExport(params("table"), project.vertexSet, attrs.toMap)
        export.insertInto(params("db"), delete = params("delete") == "yes")
      }
    })

    def getExportFilename(param: String): HadoopFile = {
      assert(param.nonEmpty, "No export path specified.")
      if (param == "<auto>") {
        dataManager.repositoryPath / "exports" / graph_util.Timestamp.toString
      } else {
        HadoopFile(param)
      }
    }

    register("Export edge attributes to file", new AttributeOperation(_, _) {
      override val dirty = true
      def parameters = List(
        Param("path", "Destination path", defaultValue = "<auto>"),
        Param("link", "Download link name", defaultValue = "edge_attributes_csv"),
        Choice("attrs", "Attributes", options = edgeAttributes, multipleChoice = true),
        Choice("format", "File format", options = UIValue.list(List("CSV", "SQL dump"))))
      def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes.")
      def apply(params: Map[String, String]) = {
        assert(params("attrs").nonEmpty, "No attributes are selected for export.")
        val labels = params("attrs").split(",", -1)
        val attrs: Map[String, Attribute[_]] = labels.map {
          label => label -> project.edgeAttributes(label)
        }.toMap
        val path = getExportFilename(params("path"))
        params("format") match {
          case "CSV" =>
            val csv = graph_util.CSVExport.exportEdgeAttributes(project.edgeBundle, attrs)
            csv.saveToDir(path)
          case "SQL dump" =>
            val export = graph_util.SQLExport(project.projectName, project.edgeBundle, attrs)
            export.saveAs(path)
        }
        project.scalars(params("link")) =
          downloadLink(path, project.projectName + "_" + params("link"))
      }
    })

    register("Export edge attributes to database", new AttributeOperation(_, _) {
      override val dirty = true
      def parameters = List(
        Param("db", "Database"),
        Param("table", "Table"),
        Choice("attrs", "Attributes", options = edgeAttributes, multipleChoice = true),
        Choice("delete", "Overwrite table if it exists", options = UIValue.list(List("no", "yes"))))
      def enabled = FEStatus.assert(edgeAttributes.nonEmpty, "No edge attributes.")
      def apply(params: Map[String, String]) = {
        assert(params("attrs").nonEmpty, "No attributes are selected for export.")
        val labels = params("attrs").split(",", -1)
        val attrs: Map[String, Attribute[_]] = labels.map {
          label => label -> project.edgeAttributes(label)
        }.toMap
        val export = graph_util.SQLExport(params("table"), project.edgeBundle, attrs)
        export.insertInto(params("db"), delete = params("delete") == "yes")
      }
    })

    register("Export segmentation to file", new SegmentationOperation(_, _) {
      override val dirty = true
      def parameters = List(
        Param("path", "Destination path", defaultValue = "<auto>"),
        Param("link", "Download link name", defaultValue = "segmentation_csv"),
        Choice("format", "File format", options = UIValue.list(List("CSV", "SQL dump"))))
      def enabled = FEStatus.enabled
      def apply(params: Map[String, String]) = {
        val path = getExportFilename(params("path"))
        val name = project.asSegmentation.name
        params("format") match {
          case "CSV" =>
            val csv = graph_util.CSVExport.exportEdgeAttributes(
              seg.belongsTo, attributes = Map(),
              srcColumnName = "vertex_id", dstColumnName = s"${name}_id")
            csv.saveToDir(path)
          case "SQL dump" =>
            val export = graph_util.SQLExport(
              name, seg.belongsTo, attributes = Map[String, Attribute[_]](),
              srcColumnName = "vertex_id", dstColumnName = s"${name}_id")
            export.saveAs(path)
        }
        project.scalars(params("link")) =
          downloadLink(path, project.projectName + "_" + params("link"))
      }
    })

    register("Export segmentation to database", new SegmentationOperation(_, _) {
      override val dirty = true
      def parameters = List(
        Param("db", "Database"),
        Param("table", "Table"),
        Choice("delete", "Overwrite table if it exists", options = UIValue.list(List("no", "yes"))))
      def enabled = FEStatus.enabled
      def apply(params: Map[String, String]) = {
        val export = graph_util.SQLExport(params("table"), seg.belongsTo, Map[String, Attribute[_]]())
        export.insertInto(params("db"), delete = params("delete") == "yes")
      }
    })
  }

  def joinAttr[A, B](a: Attribute[A], b: Attribute[B]): Attribute[(A, B)] = {
    graph_operations.JoinAttributes.run(a, b)
  }

  def computeSegmentSizes(segmentation: Segmentation): Attribute[Double] = {
    val op = graph_operations.OutDegree()
    op(op.es, reverse(segmentation.belongsTo)).result.outDegree
  }

  def toDouble(attr: Attribute[_]): Attribute[Double] = {
    if (attr.is[String])
      graph_operations.VertexAttributeToDouble.run(attr.runtimeSafeCast[String])
    else if (attr.is[Long])
      graph_operations.LongAttributeToDouble.run(attr.runtimeSafeCast[Long])
    else
      throw new AssertionError(s"Unexpected type (${attr.typeTag}) on $attr")
  }

  private def attributeToString[T](attr: Attribute[T]): Attribute[String] = {
    val op = graph_operations.VertexAttributeToString[T]()
    op(op.attr, attr).result.attr
  }

  def parseAggregateParams(params: Map[String, String]) = {
    val aggregate = "aggregate-(.*)".r
    params.toSeq.collect {
      case (aggregate(attr), choices) if choices.nonEmpty => attr -> choices
    }.flatMap {
      case (attr, choices) => choices.split(",", -1).map(attr -> _)
    }
  }
  def aggregateParams(
    attrs: Iterable[(String, Attribute[_])],
    needsGlobal: Boolean = false,
    weighted: Boolean = false): List[OperationParameterMeta] = {
    attrs.toList.map {
      case (name, attr) =>
        val options = if (attr.is[Double]) {
          if (weighted) { // At the moment all weighted aggregators are global.
            UIValue.list(List("weighted_sum", "weighted_average", "by_max_weight", "by_min_weight"))
          } else if (needsGlobal) {
            UIValue.list(List("sum", "average", "min", "max", "count", "first", "std_deviation"))
          } else {
            UIValue.list(List("sum", "average", "min", "max", "most_common", "count_distinct", "count", "vector", "std_deviation"))
          }
        } else if (attr.is[String]) {
          if (weighted) { // At the moment all weighted aggregators are global.
            UIValue.list(List("by_max_weight", "by_min_weight"))
          } else if (needsGlobal) {
            UIValue.list(List("count", "first"))
          } else {
            UIValue.list(List("most_common", "count_distinct", "majority_50", "majority_100", "count", "vector"))
          }
        } else {
          if (weighted) { // At the moment all weighted aggregators are global.
            UIValue.list(List("by_max_weight", "by_min_weight"))
          } else if (needsGlobal) {
            UIValue.list(List("count", "first"))
          } else {
            UIValue.list(List("most_common", "count_distinct", "count", "vector"))
          }
        }
        TagList(s"aggregate-$name", name, options = options)
    }
  }

  // Performs AggregateAttributeToScalar.
  private def aggregate[From, Intermediate, To](
    attributeWithAggregator: AttributeWithAggregator[From, Intermediate, To]): Scalar[To] = {
    val op = graph_operations.AggregateAttributeToScalar(attributeWithAggregator.aggregator)
    op(op.attr, attributeWithAggregator.attr).result.aggregated
  }

  // Performs AggregateByEdgeBundle.
  private def aggregateViaConnection[From, To](
    connection: EdgeBundle,
    attributeWithAggregator: AttributeWithLocalAggregator[From, To]): Attribute[To] = {
    val op = graph_operations.AggregateByEdgeBundle(attributeWithAggregator.aggregator)
    op(op.connection, connection)(op.attr, attributeWithAggregator.attr).result.attr
  }

  // Performs AggregateFromEdges.
  private def aggregateFromEdges[From, To](
    edges: EdgeBundle,
    attributeWithAggregator: AttributeWithLocalAggregator[From, To]): Attribute[To] = {
    val op = graph_operations.AggregateFromEdges(attributeWithAggregator.aggregator)
    val res = op(op.edges, edges)(op.eattr, attributeWithAggregator.attr).result
    res.dstAttr
  }

  def reverse(eb: EdgeBundle): EdgeBundle = {
    val op = graph_operations.ReverseEdges()
    op(op.esAB, eb).result.esBA
  }

  def removeNonSymmetric(eb: EdgeBundle): EdgeBundle = {
    val op = graph_operations.RemoveNonSymmetricEdges()
    op(op.es, eb).result.symmetric
  }

  def addReversed(eb: EdgeBundle): EdgeBundle = {
    val op = graph_operations.AddReversedEdges()
    op(op.es, eb).result.esPlus
  }

  object Direction {
    // Options suitable when edge attributes are involved.
    val attrOptions = UIValue.list(List(
      "incoming edges",
      "outgoing edges",
      "all edges"))
    // Options suitable when edge attributes are not involved.
    val options = attrOptions :+ UIValue("symmetric edges", "symmetric edges")
  }
  case class Direction(direction: String, origEB: EdgeBundle, reversed: Boolean = false) {
    val unchangedOut: (EdgeBundle, Option[EdgeBundle]) = (origEB, None)
    val reversedOut: (EdgeBundle, Option[EdgeBundle]) = {
      val op = graph_operations.ReverseEdges()
      val res = op(op.esAB, origEB).result
      (res.esBA, Some(res.injection))
    }
    val (edgeBundle, pullBundleOpt): (EdgeBundle, Option[EdgeBundle]) = direction match {
      case "incoming edges" => if (reversed) reversedOut else unchangedOut
      case "outgoing edges" => if (reversed) unchangedOut else reversedOut
      case "all edges" =>
        val op = graph_operations.AddReversedEdges()
        val res = op(op.es, origEB).result
        (res.esPlus, Some(res.newToOriginal))
      case "symmetric edges" =>
        // Use "null" as the injection because it is an error to use
        // "symmetric edges" with edge attributes.
        (removeNonSymmetric(origEB), Some(null))
    }

    def pull[T](attribute: Attribute[T]): Attribute[T] = {
      pullBundleOpt.map { pullBundle =>
        graph_operations.PulledOverVertexAttribute.pullAttributeVia(attribute, pullBundle)
      }.getOrElse(attribute)
    }
  }

  def count(eb: EdgeBundle): Scalar[Long] = graph_operations.Count.run(eb)

  private def unifyAttributeT[T](a1: Attribute[T], a2: Attribute[_]): Attribute[T] = {
    val op = graph_operations.AttributeFallback[T]()
    op(op.originalAttr, a1)(op.defaultAttr, a2.runtimeSafeCast(a1.typeTag)).result.defaultedAttr
  }
  def unifyAttribute(a1: Attribute[_], a2: Attribute[_]): Attribute[_] = {
    unifyAttributeT(a1, a2)
  }

  def unifyAttributes(
    as1: Iterable[(String, Attribute[_])],
    as2: Iterable[(String, Attribute[_])]): Map[String, Attribute[_]] = {

    val m1 = as1.toMap
    val m2 = as2.toMap
    m1.keySet.union(m2.keySet)
      .map(k => k -> (m1.get(k) ++ m2.get(k)).reduce(unifyAttribute _))
      .toMap
  }

  def concat(eb1: EdgeBundle, eb2: EdgeBundle): EdgeBundle = {
    new graph_util.BundleChain(Seq(eb1, eb2)).getCompositeEdgeBundle._1
  }

  def const(eb: EdgeBundle, value: Double = 1.0): Attribute[Double] = {
    graph_operations.AddConstantAttribute.run(eb.idSet, value)
  }

  def newScalar(data: String): Scalar[String] = {
    val op = graph_operations.CreateStringScalar(data)
    op.result.created
  }

  def downloadLink(fn: HadoopFile, name: String) = {
    val urlPath = java.net.URLEncoder.encode(fn.symbolicName, "utf-8")
    val urlName = java.net.URLEncoder.encode(name, "utf-8")
    val url = s"/download?path=$urlPath&name=$urlName"
    val quoted = '"' + url + '"'
    newScalar(s"<a href=$quoted>download</a>")
  }

  // Whether a JavaScript expression contains a given identifier.
  // It's a best-effort implementation with no guarantees of correctness.
  def containsIdentifierJS(expr: String, identifier: String): Boolean = {
    val re = ".*\\b" + java.util.regex.Pattern.quote(identifier) + "\\b.*"
    expr.matches(re)
  }
}

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

class EdgeAttributeOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Implicits._

  val category = Categories.EdgeAttributeOperations

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Add constant edge attribute")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "weight"),
      Param("value", "Value", defaultValue = "1"),
      Choice("type", "Type", options = FEOption.list("number", "String")))
    def enabled = project.hasEdgeBundle
    override def summary = {
      val name = params("name")
      val value = params("value")
      s"Add constant edge attribute: $name = $value"
    }
    def apply() = {
      val value = params("value")
      val res = {
        if (params("type") == "number") {
          project.edgeBundle.const(value.toDouble)
        } else {
          project.edgeBundle.const(value)
        }
      }
      project.newEdgeAttribute(params("name"), res, s"constant $value")
    }
  })

  register("Add random edge attribute")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "random"),
      Choice("dist", "Distribution", options = FEOption.list(graph_operations.RandomDistribution.getNames)),
      RandomSeed("seed", "Seed", context.box))
    def enabled = project.hasEdgeBundle
    override def summary = {
      val dist = params("dist").toLowerCase
      s"Add $dist edge attribute"
    }
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.AddRandomAttribute(params("seed").toInt, params("dist"))
      project.newEdgeAttribute(
        params("name"), op(op.vs, project.edgeBundle.idSet).result.attr, help)
    }
  })

  register("Convert edge attribute to String")(new ProjectTransformation(_) {
    params +=
      Choice("attr", "Edge attribute", options = project.edgeAttrList, multipleChoice = true)
    def enabled = project.hasEdgeBundle
    def apply() = {
      for (attr <- splitParam("attr")) {
        project.edgeAttributes(attr) = project.edgeAttributes(attr).asString
      }
    }
  })

  register("Convert edge attribute to number")(new ProjectTransformation(_) {
    def eligible =
      project.edgeAttrList[String] ++
        project.edgeAttrList[Long] ++
        project.edgeAttrList[Int]
    params += Choice("attr", "Edge attribute", options = eligible, multipleChoice = true,
      hiddenOptions = project.edgeAttrList[Double])
    def enabled = project.hasEdgeBundle
    def apply() = {
      for (name <- splitParam("attr")) {
        val attr = project.edgeAttributes(name)
        project.edgeAttributes(name) = toDouble(attr)
      }
    }
  })

  register("Derive edge attribute")(new ProjectTransformation(_) {
    params ++= List(
      Param("output", "Save as"),
      Choice("defined_attrs", "Only run on defined attributes",
        options = FEOption.bools), // Default is true.
      Code("expr", "Value", defaultValue = "", language = "scala"),
      Choice("persist", "Persist result", options = FEOption.bools)) // Default is true.
    def enabled = project.hasEdgeBundle
    override def summary = {
      val name = if (params("output").nonEmpty) params("output") else "?"
      val expr = if (params("expr").nonEmpty) params("expr") else "?"
      s"Derive edge attribute: $name = $expr"
    }
    def apply(): Unit = {
      val output = params("output")
      val expr = params("expr")
      if (output.isEmpty || expr.isEmpty) return
      val edgeBundle = project.edgeBundle
      val idSet = project.edgeBundle.idSet
      val namedEdgeAttributes = ScalaUtilities.collectIdentifiers[Attribute[_]](project.edgeAttributes, expr)
      val namedSrcVertexAttributes =
        ScalaUtilities.collectIdentifiers[Attribute[_]](project.vertexAttributes, expr, "src$")
          .map {
            case (name, attr) =>
              "src$" + name -> graph_operations.VertexToEdgeAttribute.srcAttribute(attr, edgeBundle)
          }
      val namedScalars = ScalaUtilities.collectIdentifiers[Scalar[_]](project.scalars, expr)
      val namedDstVertexAttributes =
        ScalaUtilities.collectIdentifiers[Attribute[_]](project.vertexAttributes, expr, "dst$")
          .map {
            case (name, attr) =>
              "dst$" + name -> graph_operations.VertexToEdgeAttribute.dstAttribute(attr, edgeBundle)
          }

      val namedAttributes =
        namedEdgeAttributes ++ namedSrcVertexAttributes ++ namedDstVertexAttributes
      val onlyOnDefinedAttrs = params("defined_attrs").toBoolean
      val persist = params("persist").toBoolean

      val result = graph_operations.DeriveScala.deriveAndInferReturnType(
        expr, namedAttributes, idSet, namedScalars, onlyOnDefinedAttrs, persist)

      project.newEdgeAttribute(output, result, expr + help)
    }
  })

  register("Expose internal edge ID")(new ProjectTransformation(_) {
    params += Param("name", "Attribute name", defaultValue = "id")
    def enabled = project.hasEdgeBundle
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      project.newEdgeAttribute(params("name"), project.edgeBundle.idSet.idAttribute.asString, help)
    }
  })

  register(
    "Fill edge attributes with constant default values")(new ProjectTransformation(_) {
      params += new DummyParam("text", "The default values for each attribute:")
      params ++= project.edgeAttrList.map {
        attr => Param(s"fill_${attr.id}", attr.id)
      }
      def enabled = project.hasEdgeBundle
      val attrParams: Map[String, String] = params.toMap.collect {
        case (name, value) if name.startsWith("fill_") && value.nonEmpty => (name.stripPrefix("fill_"), value)
      }
      override def summary = {
        val fillStrings = attrParams.map {
          case (name, const) => s"${name} with ${const}"
        }
        s"Fill ${fillStrings.mkString(", ")}"
      }
      def apply() = {
        for ((name, const) <- attrParams.toMap) {
          val attr = project.edgeAttributes(name)
          val op: graph_operations.AddConstantAttribute[_] =
            graph_operations.AddConstantAttribute.doubleOrString(
              isDouble = attr.is[Double], const)
          val default = op(op.vs, project.edgeBundle.idSet).result
          project.newEdgeAttribute(
            name, unifyAttribute(attr, default.attr.entity),
            project.viewer.getEdgeAttributeNote(name) + s" (filled with default $const)" + help)
        }
      }
    })

  register("Merge two edge attributes")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "New attribute name", defaultValue = ""),
      Choice("attr1", "Primary attribute", options = project.edgeAttrList),
      Choice("attr2", "Secondary attribute", options = project.edgeAttrList))
    def enabled = FEStatus.assert(
      project.edgeAttrList.size >= 2, "Not enough edge attributes.")
    override def summary = {
      val name1 = params("attr1")
      val name2 = params("attr2")
      s"Merge two edge attributes: $name1, $name2"
    }
    def apply() = {
      val name = params("name")
      assert(name.nonEmpty, "You must specify a name for the new attribute.")
      val attr1 = project.edgeAttributes(params("attr1"))
      val attr2 = project.edgeAttributes(params("attr2"))
      assert(
        attr1.typeTag.tpe =:= attr2.typeTag.tpe,
        "The two attributes must have the same type.")
      project.newEdgeAttribute(name, unifyAttribute(attr1, attr2), s"primary: $attr1, secondary: $attr2" + help)
    }
  })

  register(
    "Use table as edge attributes", List(projectInput, "attributes"))(new ProjectOutputOperation(_) {
      override lazy val project = projectInput("graph")
      lazy val attributes = tableLikeInput("attributes").asProject
      params ++= List(
        Choice("id_attr", "Edge attribute",
          options = FEOption.unset +: project.edgeAttrList[String]),
        Choice("id_column", "ID column", options = FEOption.unset +: attributes.vertexAttrList),
        Param("prefix", "Name prefix for the imported edge attributes"),
        Choice("unique_keys", "Assert unique edge attribute values", options = FEOption.bools))
      def enabled =
        project.hasEdgeBundle &&
          FEStatus.assert(project.edgeAttrList[String].nonEmpty, "No edge attributes to use as key.")
      def apply() = {
        val columnName = params("id_column")
        assert(columnName != FEOption.unset.id, "The ID column parameter must be set.")
        val attrName = params("id_attr")
        assert(attrName != FEOption.unset.id, "The edge attribute parameter must be set.")
        val idAttr = project.edgeAttributes(attrName).runtimeSafeCast[String]
        val idColumn = attributes.vertexAttributes(columnName).runtimeSafeCast[String]
        val projectAttrNames = project.edgeAttributeNames
        val uniqueKeys = params("unique_keys").toBoolean
        val edges = if (uniqueKeys) {
          val op = graph_operations.EdgesFromUniqueBipartiteAttributeMatches()
          op(op.fromAttr, idAttr)(op.toAttr, idColumn).result.edges
        } else {
          val op = graph_operations.EdgesFromLookupAttributeMatches()
          op(op.fromAttr, idAttr)(op.toAttr, idColumn).result.edges
        }
        val prefix = if (params("prefix").nonEmpty) params("prefix") + "_" else ""
        for ((name, attr) <- attributes.vertexAttributes) {
          assert(
            !projectAttrNames.contains(prefix + name),
            s"Cannot import column `${prefix + name}`. Attribute already exists.")
          project.newEdgeAttribute(prefix + name, attr.pullVia(edges), "imported")
        }
      }
    })
}

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

class VertexAttributeOperations(env: SparkFreeEnvironment) extends ProjectOperations(env) {
  import Operation.Context
  import Operation.Implicits._

  val category = Categories.VertexAttributeOperations

  import com.lynxanalytics.biggraph.controllers.OperationParams._

  register("Add constant vertex attribute")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name"),
      Param("value", "Value", defaultValue = "1"),
      Choice("type", "Type", options = FEOption.list("number", "String")))
    def enabled = project.hasVertexSet
    override def summary = {
      val name = params("name")
      val value = params("value")
      s"Add constant vertex attribute: $name = $value"
    }
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val value = params("value")
      val op: graph_operations.AddConstantAttribute[_] =
        graph_operations.AddConstantAttribute.doubleOrString(
          isDouble = (params("type") == "number"), value)
      project.newVertexAttribute(
        params("name"), op(op.vs, project.vertexSet).result.attr, s"constant $value")
    }
  })

  register("Add random vertex attribute")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "random"),
      Choice("dist", "Distribution", options = FEOption.list(graph_operations.RandomDistribution.getNames)),
      RandomSeed("seed", "Seed", context.box))
    def enabled = project.hasVertexSet
    override def summary = {
      val dist = params("dist").toLowerCase
      s"Add $dist vertex attribute"
    }
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      val op = graph_operations.AddRandomAttribute(params("seed").toInt, params("dist"))
      project.newVertexAttribute(
        params("name"), op(op.vs, project.vertexSet).result.attr, help)
    }
  })

  register("Add rank attribute")(new ProjectTransformation(_) {
    def attrs = (
      project.vertexAttrList[String] ++
      project.vertexAttrList[Double] ++
      project.vertexAttrList[Long] ++
      project.vertexAttrList[Int]).sortBy(_.title)
    params ++= List(
      Param("rankattr", "Rank attribute name", defaultValue = "ranking"),
      Choice("keyattr", "Key attribute name", options = attrs),
      Choice("order", "Order", options = FEOption.list("ascending", "descending")))

    def enabled = FEStatus.assert(attrs.nonEmpty, "No sortable vertex attributes")
    override def summary = {
      val name = params("keyattr")
      s"Add rank attribute for '$name'"
    }
    def apply() = {
      val keyAttr = params("keyattr")
      val rankAttr = params("rankattr")
      val ascending = params("order") == "ascending"
      assert(rankAttr.nonEmpty, "Please set a name for the rank attribute")
      val sortKey = project.vertexAttributes(keyAttr)
      val rank = graph_operations.AddRankingAttribute.run(sortKey, ascending)
      project.newVertexAttribute(rankAttr, rank, s"rank by $keyAttr" + help)
    }
  })

  register("Convert vertex attribute to number")(new ProjectTransformation(_) {
    def eligible =
      project.vertexAttrList[String] ++
        project.vertexAttrList[Long] ++
        project.vertexAttrList[Int]
    params += Choice("attr", "Vertex attribute", options = eligible, multipleChoice = true,
      hiddenOptions = project.vertexAttrList[Double])
    def enabled = project.hasVertexSet
    def apply() = {
      for (name <- splitParam("attr")) {
        val attr = project.vertexAttributes(name)
        project.vertexAttributes(name) = toDouble(attr)
      }
    }
  })

  register(
    "Convert vertex attribute to String")(new ProjectTransformation(_) {
      params +=
        Choice("attr", "Vertex attribute", options = project.vertexAttrList, multipleChoice = true)
      def enabled = project.hasVertexSet
      def apply() = {
        for (attr <- splitParam("attr")) {
          project.vertexAttributes(attr) = project.vertexAttributes(attr).asString
        }
      }
    })

  register("Derive vertex attribute")(new ProjectTransformation(_) {
    params ++= List(
      Param("output", "Save as"),
      Choice("defined_attrs", "Only run on defined attributes",
        options = FEOption.bools), // Default is true.
      Code("expr", "Value", defaultValue = "", language = "scala"),
      Choice("persist", "Persist result", options = FEOption.bools)) // Default is true.
    def enabled = project.hasVertexSet
    override def summary = {
      val name = if (params("output").nonEmpty) params("output") else "?"
      val expr = if (params("expr").nonEmpty) params("expr") else "?"
      s"Derive vertex attribute: $name = $expr"
    }
    def apply(): Unit = {
      val output = params("output")
      val expr = params("expr")
      if (output.isEmpty || expr.isEmpty) return
      val vertexSet = project.vertexSet
      val namedAttributes =
        ScalaUtilities.collectIdentifiers[Attribute[_]](project.vertexAttributes, expr)
      val namedScalars = ScalaUtilities.collectIdentifiers[Scalar[_]](project.scalars, expr)
      val onlyOnDefinedAttrs = params("defined_attrs").toBoolean
      val persist = params("persist").toBoolean

      val result = graph_operations.DeriveScala.deriveAndInferReturnType(
        expr, namedAttributes, vertexSet, namedScalars, onlyOnDefinedAttrs, persist)

      project.newVertexAttribute(output, result, expr + help)
    }
  })

  register("Expose internal vertex ID")(new ProjectTransformation(_) {
    params += Param("name", "Attribute name", defaultValue = "id")
    def enabled = project.hasVertexSet
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      project.newVertexAttribute(params("name"), project.vertexSet.idAttribute.asString, help)
    }
  })

  register(
    "Fill vertex attributes with constant default values")(new ProjectTransformation(_) {
      params += new DummyParam("text", "The default values for each attribute:")
      params ++= project.vertexAttrList.map {
        attr => Param(s"fill_${attr.id}", attr.id)
      }
      def enabled = project.hasVertexSet
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
          val attr = project.vertexAttributes(name)
          val op: graph_operations.AddConstantAttribute[_] =
            graph_operations.AddConstantAttribute.doubleOrString(
              isDouble = attr.is[Double], const)
          val default = op(op.vs, project.vertexSet).result
          project.newVertexAttribute(
            name, unifyAttribute(attr, default.attr.entity),
            project.viewer.getVertexAttributeNote(name) + s" (filled with default $const)" + help)
        }
      }
    })

  register("Hash vertex attribute")(new ProjectTransformation(_) {
    params ++= List(
      Choice("attr", "Vertex attribute", options = project.vertexAttrList, multipleChoice = true),
      Param("salt", "Salt",
        defaultValue = graph_operations.HashVertexAttribute.makeSecret("")))
    def enabled = FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes.")

    def apply() = {
      assert(params("attr").nonEmpty, "Please choose at least one vertex attribute to hash.")
      val salt = params("salt")
      graph_operations.HashVertexAttribute.assertSecret(salt)
      assert(
        graph_operations.HashVertexAttribute.getSecret(salt).nonEmpty, "Please set a salt value.")
      val op = graph_operations.HashVertexAttribute(salt)
      for (attribute <- splitParam("attr")) {
        val attr = project.vertexAttributes(attribute).asString
        project.newVertexAttribute(
          attribute, op(op.vs, project.vertexSet)(op.attr, attr).result.hashed, "hashed")
      }
    }
  })

  register("Lookup region")(new ProjectTransformation(_) {
    params ++= List(
      Choice("position", "Position", options = project.vertexAttrList[Vector[Double]]),
      Choice("shapefile", "Shapefile", options = listShapefiles(), allowUnknownOption = true),
      Param("attribute", "Attribute from the Shapefile"),
      Choice("ignoreUnsupportedShapes", "Ignore unsupported shape types",
        options = FEOption.boolsDefaultFalse),
      Param("output", "Output name"))
    def enabled = FEStatus.assert(
      project.vertexAttrList[Vector[Double]].nonEmpty, "No vector vertex attributes.")

    def apply() = {
      val shapeFilePath = getShapeFilePath(params)
      val position = project.vertexAttributes(params("position")).runtimeSafeCast[Vector[Double]]
      val op = graph_operations.LookupRegion(
        shapeFilePath,
        params("attribute"),
        params("ignoreUnsupportedShapes").toBoolean)
      val result = op(op.coordinates, position).result
      project.newVertexAttribute(params("output"), result.attribute)
    }
  })

  register("Merge two vertex attributes")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "New attribute name", defaultValue = ""),
      Choice("attr1", "Primary attribute", options = project.vertexAttrList),
      Choice("attr2", "Secondary attribute", options = project.vertexAttrList))
    def enabled = FEStatus.assert(
      project.vertexAttrList.size >= 2, "Not enough vertex attributes.")
    override def summary = {
      val name1 = params("attr1")
      val name2 = params("attr2")
      s"Merge two vertex attributes: $name1, $name2"
    }
    def apply() = {
      val name = params("name")
      assert(name.nonEmpty, "You must specify a name for the new attribute.")
      val attr1 = project.vertexAttributes(params("attr1"))
      val attr2 = project.vertexAttributes(params("attr2"))
      assert(
        attr1.typeTag.tpe =:= attr2.typeTag.tpe,
        "The two attributes must have the same type.")
      project.newVertexAttribute(name, unifyAttribute(attr1, attr2), s"primary: $attr1, secondary: $attr2" + help)
    }
  })

  register(
    "Use table as vertex attributes", List(projectInput, "attributes"))(new ProjectOutputOperation(_) {
      override lazy val project = projectInput("graph")
      lazy val attributes = tableLikeInput("attributes").asProject
      params ++= List(
        Choice("id_attr", "Vertex attribute",
          options = FEOption.unset +: project.vertexAttrList[String]),
        Choice("id_column", "ID column",
          options = FEOption.unset +: attributes.vertexAttrList[String]),
        Param("prefix", "Name prefix for the imported vertex attributes"),
        Choice("unique_keys", "Assert unique vertex attribute values", options = FEOption.bools))
      def enabled =
        project.hasVertexSet &&
          FEStatus.assert(project.vertexAttrList[String].nonEmpty, "No vertex attributes to use as key.")
      def apply() = {
        val idAttrName = params("id_attr")
        val idColumnName = params("id_column")
        assert(idAttrName != FEOption.unset.id, "The vertex attribute parameter must be set.")
        assert(idColumnName != FEOption.unset.id, "The ID column parameter must be set.")
        val idAttr = project.vertexAttributes(idAttrName).runtimeSafeCast[String]
        val idColumn = attributes.vertexAttributes(idColumnName).runtimeSafeCast[String]
        val projectAttrNames = project.vertexAttributeNames
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
          project.newVertexAttribute(prefix + name, attr.pullVia(edges), "imported")
        }
      }
    })

  register("Bundle vertex attributes into a Vector")(new ProjectTransformation(_) {
    params ++= List(
      Param("output", "Save as"),
      Choice("elements", "Elements", options =
        project.vertexAttrList[Double] ++ project.vertexAttrList[Vector[Double]], multipleChoice = true))
    override def summary = {
      val elementNames = splitParam("elements")
      val output = params("output")
      s"Bundle ${elementNames.mkString(", ")} as $output"
    }
    def enabled = FEStatus.enabled
    def apply(): Unit = {
      val output = params("output")
      val elementNames = splitParam("elements")
      if (output.isEmpty) return
      var doubleElements: Seq[Attribute[Double]] = Seq()
      var vectorElements: Seq[Attribute[Vector[Double]]] = Seq()
      for (name <- elementNames) {
        val attr = project.vertexAttributes(name)
        val tt = attr.typeTag
        tt match {
          case _ if tt == scala.reflect.runtime.universe.typeTag[Double] =>
            doubleElements = doubleElements :+ attr.runtimeSafeCast[Double]
          case _ if tt == scala.reflect.runtime.universe.typeTag[Vector[Double]] =>
            vectorElements = vectorElements :+ attr.runtimeSafeCast[Vector[Double]]
        }
      }
      val vectorAttr = {
        val op = graph_operations.BundleVertexAttributesIntoVector(
          doubleElements.size, vectorElements.size)
        op(op.vs, project.vertexSet)(op.doubleElements, doubleElements)(op.vectorElements, vectorElements).result.vectorAttr
      }
      project.newVertexAttribute(output, vectorAttr)
    }
  })

  register("One-hot encode attribute")(new ProjectTransformation(_) {
    params ++= List(
      Param("output", "Save as"),
      Choice("catAttr", "Categorical attribute", options = project.vertexAttrList[String]),
      Param("categories", "Categories"))
    def enabled = FEStatus.assert(
      project.vertexAttrList[String].nonEmpty, "No String vertex attributes.")
    override def summary = {
      val catAttr = params("catAttr").toString
      s"One-hot encode $catAttr"
    }
    def apply(): Unit = {
      val output = params("output")
      if (output.isEmpty) return
      val catAttrName = params("catAttr")
      val catAttr = project.vertexAttributes(catAttrName).runtimeSafeCast[String]
      val categories = splitParam("categories").toSeq
      val oneHotVector = {
        val op = graph_operations.OneHotEncoder(categories)
        op(op.catAttr, catAttr).result.oneHotVector
      }
      project.newVertexAttribute(output, oneHotVector)
    }
  })
}

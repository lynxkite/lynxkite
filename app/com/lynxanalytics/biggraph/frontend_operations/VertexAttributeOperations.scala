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
      Choice("type", "Type", options = FEOption.list("Double", "String")))
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
          isDouble = (params("type") == "Double"), value)
      project.newVertexAttribute(
        params("name"), op(op.vs, project.vertexSet).result.attr, s"constant $value")
    }
  })

  register("Add random vertex attribute")(new ProjectTransformation(_) {
    params ++= List(
      Param("name", "Attribute name", defaultValue = "random"),
      Choice("dist", "Distribution", options = FEOption.list(graph_operations.RandomDistribution.getNames)),
      RandomSeed("seed", "Seed"))
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
      project.newVertexAttribute(rankAttr, rank.asDouble, s"rank by $keyAttr" + help)
    }
  })

  register("Convert vertex attribute to Double")(new ProjectTransformation(_) {
    def eligible =
      project.vertexAttrList[String] ++
        project.vertexAttrList[Long] ++
        project.vertexAttrList[Int]
    params += Choice("attr", "Vertex attribute", options = eligible, multipleChoice = true)
    def enabled = FEStatus.assert(eligible.nonEmpty, "No eligible vertex attributes.")
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
      def enabled = FEStatus.assert(project.vertexAttrList.nonEmpty, "No vertex attributes.")
      def apply() = {
        for (attr <- splitParam("attr")) {
          project.vertexAttributes(attr) = project.vertexAttributes(attr).asString
        }
      }
    })

  register("Convert vertex attributes to position")(new ProjectTransformation(_) {
    params ++= List(
      Param("output", "Save as", defaultValue = "position"),
      Choice("x", "X or latitude", options = project.vertexAttrList[Double]),
      Choice("y", "Y or longitude", options = project.vertexAttrList[Double]))
    def enabled = FEStatus.assert(
      project.vertexAttrList[Double].nonEmpty, "No numeric vertex attributes.")
    def apply() = {
      assert(params("output").nonEmpty, "Please set an attribute name.")
      val paramX = params("x")
      val paramY = params("y")
      val pos = {
        val op = graph_operations.JoinAttributes[Double, Double]()
        val x = project.vertexAttributes(paramX).runtimeSafeCast[Double]
        val y = project.vertexAttributes(paramY).runtimeSafeCast[Double]
        op(op.a, x)(op.b, y).result.attr
      }
      project.newVertexAttribute(params("output"), pos, s"($paramX, $paramY)" + help)
    }
  })

  register("Derive vertex attribute")(new ProjectTransformation(_) {
    params ++= List(
      Param("output", "Save as"),
      Choice("defined_attrs", "Only run on defined attributes",
        options = FEOption.bools), // Default is true.
      Code("expr", "Value", defaultValue = "", language = "javascript"))
    def enabled = project.hasVertexSet
    override def summary = {
      val name = params("output")
      val expr = params("expr")
      s"Derive vertex attribute: $name = $expr"
    }
    def apply() = {
      assert(params("output").nonEmpty, "Please set an output attribute name.")
      val expr = params("expr")
      val vertexSet = project.vertexSet
      val namedAttributes =
        ScalaUtilities.collectIdentifiers[Attribute[_]](project.vertexAttributes, expr)
      val namedScalars = ScalaUtilities.collectIdentifiers[Scalar[_]](project.scalars, expr)
      val onlyOnDefinedAttrs = params("defined_attrs").toBoolean

      val result = graph_operations.DeriveScala.deriveAndInferReturnType(
        expr, namedAttributes, vertexSet, namedScalars, onlyOnDefinedAttrs)

      project.newVertexAttribute(params("output"), result, expr + help)
    }
  })

  register("Expose internal vertex ID")(new ProjectTransformation(_) {
    params += Param("name", "Attribute name", defaultValue = "id")
    def enabled = project.hasVertexSet
    def apply() = {
      assert(params("name").nonEmpty, "Please set an attribute name.")
      project.newVertexAttribute(params("name"), project.vertexSet.idAttribute, help)
    }
  })

  register(
    "Fill vertex attributes with constant default values")(new ProjectTransformation(_) {
      params += new DummyParam("text", "Attributes:", "Default values:")
      params ++= project.vertexAttrList.map {
        attr => Param(s"fill_${attr.id}", attr.id)
      }
      def enabled = FEStatus.assert(
        (project.vertexAttrList[String] ++ project.vertexAttrList[Double]).nonEmpty,
        "No vertex attributes.")
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
        defaultValue = graph_operations.HashVertexAttribute.makeSecret(nextString(15))))
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

    // To have a more secure default salt value than just using Random.nextInt.toString
    def nextString(length: Int): String = {
      val validCharacters: Array[Char] = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toArray
      val srand: java.security.SecureRandom = new java.security.SecureRandom()
      val rand: java.util.Random = new java.util.Random()

      rand.setSeed(srand.nextLong())

      val chars: Array[Char] = new Array[Char](length)
      chars.map(_ => validCharacters(rand.nextInt(validCharacters.length))).mkString("")
    }
  })

  register("Lookup region")(new ProjectTransformation(_) {
    params ++= List(
      Choice("position", "Position", options = project.vertexAttrList[(Double, Double)]),
      Choice("shapefile", "Shapefile", options = listShapefiles(), allowUnknownOption = true),
      Param("attribute", "Attribute from the Shapefile"),
      Choice("ignoreUnsupportedShapes", "Ignore unsupported shape types",
        options = FEOption.boolsDefaultFalse),
      Param("output", "Output name"))
    def enabled = FEStatus.assert(
      project.vertexAttrList[(Double, Double)].nonEmpty, "No position vertex attributes.")

    def apply() = {
      val shapeFilePath = getShapeFilePath(params)
      val position = project.vertexAttributes(params("position")).runtimeSafeCast[(Double, Double)]
      val op = graph_operations.LookupRegion(
        shapeFilePath,
        params("attribute"),
        params("ignoreUnsupportedShapes").toBoolean)
      val result = op(op.coordinates, position).result
      project.newVertexAttribute(params("output"), result.attribute)
    }
  })

  register("Map hyperbolic coordinates")(new ProjectTransformation(_) {
    params ++= List(
      RandomSeed("seed", "Seed"))
    def enabled = project.hasEdgeBundle
    def apply() = {
      val result = {
        val direction = Direction("all neighbors", project.edgeBundle)
        val clusterOp = graph_operations.ApproxClusteringCoefficient(8)
        val degreeOp = graph_operations.OutDegree()
        val degree = degreeOp(degreeOp.es, direction.edgeBundle).result.outDegree
        val clus = clusterOp(clusterOp.vs, project.vertexSet)(
          clusterOp.es, direction.edgeBundle).result.clustering
        val op = graph_operations.HyperMap(params("seed").toLong)
        op(op.vs, project.vertexSet)(op.es, direction.edgeBundle
        )(op.degree, degree)(op.clustering, clus).result
      }
      project.newVertexAttribute("radial", result.radial)
      project.newVertexAttribute("angular", result.angular)
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
      assert(attr1.typeTag.tpe =:= attr2.typeTag.tpe,
        "The two attributes must have the same type.")
      project.newVertexAttribute(name, unifyAttribute(attr1, attr2), s"primary: $attr1, secondary: $attr2" + help)
    }
  })

  register(
    "Use table as vertex attributes", List(projectInput, "attributes"))(new ProjectOutputOperation(_) {
      override lazy val project = projectInput("project")
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
          assert(!projectAttrNames.contains(prefix + name),
            s"Cannot import column `${prefix + name}`. Attribute already exists.")
          project.newVertexAttribute(prefix + name, attr.pullVia(edges), "imported")
        }
      }
    })
}

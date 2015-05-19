package com.lynxanalytics.biggraph.controllers
import com.lynxanalytics.biggraph.graph_util.RootRepository

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.serving

class OperationsTest extends FunSuite with TestGraphOp with BigGraphEnvironment {
  val res = getClass.getResource("/controllers/OperationsTest/").toString
  RootRepository.registerRoot("OPERATIONSTEST$", res)
  val ops = new Operations(this)
  def createProject(name: String) = {
    val controller = new BigGraphController(this)
    val request = CreateProjectRequest(name = name, notes = name, privacy = "public-write")
    controller.createProject(null, request)
    Project(name)
  }
  val project = createProject("Test_Project")

  def run(op: String, params: Map[String, String] = Map(), on: Project = project) =
    ops.apply(
      serving.User.fake,
      ProjectOperationRequest(on.projectName, FEOperationSpec(Operation.titleToID(op), params)))

  def remapIDs[T](attr: Attribute[T], origIDs: Attribute[String]) =
    attr.rdd.sortedJoin(origIDs.rdd).map { case (id, (num, origID)) => origID -> num }

  test("merge_parallel edges by attribute works for String") {
    run("Import vertices and edges from single CSV fileset", Map(
      "files" -> "OPERATIONSTEST$/merge-parallel-edges.csv",
      "header" -> "src,dst,call",
      "delimiter" -> ",",
      "src" -> "src",
      "dst" -> "dst",
      "filter" -> ""))
    run("Merge parallel edges by attribute", Map(
      "key" -> "call",
      "aggregate-src" -> "",
      "aggregate-dst" -> "",
      "aggregate-call" -> ""
    ))
    val call = project.edgeAttributes("call").runtimeSafeCast[String]
    assert(call.rdd.values.collect.toSeq.sorted == Seq(
      "Monday", // Mary->John, Wednesday
      // "Monday",  // Mary->John, Wednesday - duplicate
      "Saturday", // Mary->John, Saturday
      //"Saturday", // Mary->John, Saturday - duplicate
      "Tuesday", // John->Mary, Tuesday
      //"Tuesday",  // John->Mary, Tuesday - duplicate
      "Wednesday", // Mary->John, Wednesday
      "Wednesday" // John->Mary, Wednesday
    ))
  }

  test("merge parallel edges by attribute works for Double") {
    run("Import vertices and edges from single CSV fileset", Map(
      "files" -> "OPERATIONSTEST$/merge-parallel-edges-double.csv",
      "header" -> "src,dst,call",
      "delimiter" -> ",",
      "src" -> "src",
      "dst" -> "dst",
      "filter" -> ""))
    run("Edge attribute to double", Map("attr" -> "call"))
    run("Merge parallel edges by attribute", Map(
      "key" -> "call",
      "aggregate-src" -> "",
      "aggregate-dst" -> "",
      "aggregate-call" -> ""
    ))
    val call = project.edgeAttributes("call").runtimeSafeCast[Double]
    assert(call.rdd.values.collect.toSeq.sorted == Seq(
      1.0, // Mary->John, 1.0
      // 1.0,  // Mary->John, 1.0 - duplicate
      2.0, // John->Mary, 2.0
      // 2.0,  // John->Mary, 2.0 - duplicate
      3.0, // Mary->John, 3.0
      3.0, // John->Mary, 3.0
      6.0 // Mary->John, 6.0
    // ,6.0 // Mary->John, 6.0 - duplicate

    ))
  }

  test("Merge parallel edges works") {
    run("Import vertices and edges from single CSV fileset", Map(
      "files" -> "OPERATIONSTEST$/merge-parallel-edges.csv",
      "header" -> "src,dst,call",
      "delimiter" -> ",",
      "src" -> "src",
      "dst" -> "dst",
      "filter" -> ""))
    run("Merge parallel edges", Map(
      "aggregate-src" -> "",
      "aggregate-dst" -> "",
      "aggregate-call" -> "count"
    ))
    val call = project.edgeAttributes("call_count").runtimeSafeCast[Double]
    assert(call.rdd.values.collect.toSeq.sorted == Seq(3.0, 5.0))
  }

  test("Derived vertex attribute (Double)") {
    run("Example Graph")
    run("Derived vertex attribute",
      Map("type" -> "double", "output" -> "output", "expr" -> "100 + age + 10 * name.length"))
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.toMap == Map(0 -> 160.3, 1 -> 148.2, 2 -> 180.3, 3 -> 222.0))
  }

  test("Derived vertex attribute with substring conflict (#1676)") {
    run("Example Graph")
    run("Rename vertex attribute", Map("from" -> "income", "to" -> "nam"))
    run("Derived vertex attribute",
      Map("type" -> "double", "output" -> "output", "expr" -> "100 + age + 10 * name.length"))
    val attr = project.vertexAttributes("output").runtimeSafeCast[Double]
    assert(attr.rdd.collect.size == 4)
  }

  test("Derived vertex attribute (String)") {
    run("Example Graph")
    // Test dropping values.
    run("Derived vertex attribute",
      Map("type" -> "string", "output" -> "gender",
        "expr" -> "name == 'Isolated Joe' ? undefined : gender"))
    run("Derived vertex attribute",
      Map("type" -> "string", "output" -> "output",
        "expr" -> "gender == 'Male' ? 'Mr ' + name : 'Ms ' + name"))
    val attr = project.vertexAttributes("output").runtimeSafeCast[String]
    assert(attr.rdd.collect.toMap == Map(0 -> "Mr Adam", 1 -> "Ms Eve", 2 -> "Mr Bob"))
  }

  // TODO: Re-enable this test. See #1037.
  ignore("Derived edge attribute") {
    run("Example Graph")
    // Test dropping values.
    run("Derived edge attribute",
      Map("type" -> "string", "output" -> "tripletke",
        "expr" -> "src$name + ':' + comment + ':' + dst$age + '#' + weight"))
    val attr = project.edgeAttributes("tripletke").runtimeSafeCast[String]
    assert(attr.rdd.collect.toSeq == Seq(
      (0, "Adam:Adam loves Eve:18.2#1"),
      (1, "Eve:Eve loves Adam:20.3#2"),
      (2, "Bob:Bob envies Adam:20.3#3"),
      (3, "Bob:Bob loves Eve:18.2#4")))
  }

  test("Aggregate to segmentation") {
    run("Example Graph")
    run("Connected components", Map("name" -> "cc", "directions" -> "require both directions"))
    val seg = project.segmentation("cc").project
    run("Aggregate to segmentation",
      Map("aggregate-age" -> "average", "aggregate-name" -> "count", "aggregate-gender" -> "majority_100",
        "aggregate-id" -> "", "aggregate-location" -> "", "aggregate-income" -> ""),
      on = seg)
    val age = seg.vertexAttributes("age_average").runtimeSafeCast[Double]
    assert(age.rdd.collect.toMap.values.toSet == Set(19.25, 50.3, 2.0))
    val count = seg.vertexAttributes("name_count").runtimeSafeCast[Double]
    assert(count.rdd.collect.toMap.values.toSet == Set(2.0, 1.0, 1.0))
    val gender = seg.vertexAttributes("gender_majority_100").runtimeSafeCast[String]
    assert(gender.rdd.collect.toMap.values.toSeq.sorted == Seq("", "Male", "Male"))
  }

  test("Merge vertices by attribute") {
    run("Example Graph")
    run("Merge vertices by attribute",
      Map("key" -> "gender", "aggregate-age" -> "average", "aggregate-name" -> "count",
        "aggregate-id" -> "", "aggregate-location" -> "", "aggregate-gender" -> "", "aggregate-income" -> ""))
    val age = project.vertexAttributes("age_average").runtimeSafeCast[Double]
    assert(age.rdd.collect.toMap.values.toSet == Set(24.2, 18.2))
    val count = project.vertexAttributes("name_count").runtimeSafeCast[Double]
    assert(count.rdd.collect.toMap.values.toSet == Set(3.0, 1.0))
    val gender = project.vertexAttributes("gender").runtimeSafeCast[String]
    assert(gender.rdd.collect.toMap.values.toSet == Set("Male", "Female"))
    val edges = project.edgeBundle
    assert(edges.rdd.values.collect.toSeq.sorted ==
      Seq(Edge(0, 0), Edge(0, 1), Edge(0, 1), Edge(1, 0)))
  }

  test("Merge vertices by attribute, no edge bundle") {
    run("Example Graph")
    run("Discard edges")
    assert(project.edgeBundle == null)
    run("Merge vertices by attribute",
      Map("key" -> "gender", "aggregate-age" -> "average", "aggregate-id" -> "", "aggregate-name" -> "",
        "aggregate-location" -> "", "aggregate-gender" -> "", "aggregate-income" -> ""))
    val age = project.vertexAttributes("age_average").runtimeSafeCast[Double]
    assert(age.rdd.collect.toMap.values.toSet == Set(24.2, 18.2))
    assert(project.edgeBundle == null)
  }

  test("Aggregate edge attribute") {
    run("Example Graph")
    run("Aggregate edge attribute globally",
      Map("prefix" -> "", "aggregate-weight" -> "sum", "aggregate-comment" -> ""))
    assert(project.scalars("weight_sum").value == 10.0)
  }

  test("Restore checkpoint after failing operation") {
    class Bug extends Exception("simulated bug")
    ops.register("Buggy op", new Operation(_, _, Operation.Category("Test", "test")) {
      def enabled = ???
      def parameters = List()
      def apply(params: Map[String, String]) = {
        project.vertexSet = null
        throw new Bug
      }
    })
    run("Example Graph")
    assert(project.vertexSet != null)
    try {
      run("Buggy op")
    } catch {
      case _: Bug =>
    }
    assert(project.vertexSet != null)
  }

  test("Project union") {
    run("Example Graph")
    val other = Project("ExampleGraph2")
    project.copy(other)
    run("Rename vertex attribute", Map("from" -> "age", "to" -> "newage"), on = other)
    run("Rename edge attribute", Map("from" -> "comment", "to" -> "newcomment"), on = other)
    run("Union with another project", Map("other" -> "ExampleGraph2", "id-attr" -> "new_id"))

    assert(project.vertexSet.rdd.count == 8)
    assert(project.edgeBundle.rdd.count == 8)

    val vAttrs = project.vertexAttributes.toMap
    // 6 original +1 renamed +1 new_id
    assert(vAttrs.size == 8)
    val eAttrs = project.edgeAttributes.toMap
    // 2 original +1 renamed
    assert(eAttrs.size == 3)

    // Not renamed vertex attr is defined on all.
    assert(vAttrs("name").rdd.count == 8)
    // Renamed vertex attr is defined on half.
    assert(vAttrs("age").rdd.count == 4)
    assert(vAttrs("newage").rdd.count == 4)

    // Not renamed edge attr is defined on all.
    assert(eAttrs("weight").rdd.count == 8)
    // Renamed edge attr is defined on half.
    assert(eAttrs("comment").rdd.count == 4)
    assert(eAttrs("newcomment").rdd.count == 4)
  }

  test("Project union on vertex sets") {
    run("New vertex set", Map("size" -> "10"))
    val other = Project("Copy")
    project.copy(other)
    run("Union with another project", Map("other" -> "Copy", "id-attr" -> "new_id"))

    assert(project.vertexSet.rdd.count == 20)
    assert(project.edgeBundle == null)
  }

  test("Fingerprinting based on attributes") {
    run("Import vertices from CSV files", Map(
      "files" -> "OPERATIONSTEST$/fingerprint-100-vertices.csv",
      "header" -> "id,email,name",
      "delimiter" -> ",",
      "id-attr" -> "delete me",
      "filter" -> ""))
    run("Import edges for existing vertices from CSV files", Map(
      "files" -> "OPERATIONSTEST$/fingerprint-100-edges.csv",
      "header" -> "src,dst",
      "delimiter" -> ",",
      "attr" -> "id",
      "src" -> "src",
      "dst" -> "dst",
      "filter" -> ""))
    // Turn empty strings into "undefined".
    run("Derived vertex attribute", Map(
      "output" -> "email",
      "type" -> "string",
      "expr" -> "email ? email : undefined"))
    run("Derived vertex attribute", Map(
      "output" -> "name",
      "type" -> "string",
      "expr" -> "name ? name : undefined"))
    run("Fingerprinting based on attributes", Map(
      "leftName" -> "email",
      "rightName" -> "name",
      "weights" -> "!no weight",
      "mrew" -> "0.0",
      "mo" -> "1",
      "ms" -> "0.5"))
    assert(project.scalars("fingerprinting matches found").value == 9)
    run("Discard edges")
    run("Connect vertices on attribute", Map("fromAttr" -> "email", "toAttr" -> "email"))
    assert(project.scalars("edge_count").value == 18)
    assert(project.scalars("vertex_count").value == 109)
    run("Merge vertices by attribute", Map(
      "key" -> "name",
      "aggregate-email" -> "",
      "aggregate-id" -> "",
      "aggregate-name" -> "",
      "aggregate-delete me" -> "",
      "aggregate-email similarity score" -> "",
      "aggregate-name similarity score" -> ""))
    assert(project.scalars("vertex_count").value == 100)
  }

  test("Fingerprinting between project and segmentation") {
    run("Example Graph")
    val other = Project("ExampleGraph2")
    project.copy(other)
    run("Import project as segmentation", Map(
      "them" -> "ExampleGraph2"))
    val seg = project.segmentation("ExampleGraph2").project
    run("Load segmentation links from CSV", Map(
      "files" -> "OPERATIONSTEST$/fingerprint-example-connections.csv",
      "header" -> "src,dst",
      "delimiter" -> ",",
      "filter" -> "",
      "base-id-attr" -> "name",
      "base-id-field" -> "src",
      "seg-id-attr" -> "name",
      "seg-id-field" -> "dst"),
      on = seg)
    run("Fingerprinting between project and segmentation", Map(
      "mrew" -> "0.0",
      "mo" -> "1",
      "ms" -> "0.5"),
      on = seg)
    run("Aggregate from segmentation",
      Map("prefix" -> "seg",
        "aggregate-age" -> "average",
        "aggregate-id" -> "",
        "aggregate-name" -> "",
        "aggregate-location" -> "",
        "aggregate-gender" -> "",
        "aggregate-fingerprinting_similarity_score" -> "",
        "aggregate-income" -> ""),
      on = seg)
    val newAge = project.vertexAttributes("seg_age_average")
      .runtimeSafeCast[Double].rdd.collect.toSeq.sorted
    // Two mappings.
    assert(newAge == Seq(0 -> 20.3, 1 -> 18.2))
    val oldAge = project.vertexAttributes("age")
      .runtimeSafeCast[Double].rdd.collect.toMap
    // They map Adam to Adam, Eve to Eve.
    for ((k, v) <- newAge) {
      assert(v == oldAge(k))
    }
  }

  test("Fingerprinting between project and segmentation by attribute") {
    run("Import vertices and edges from single CSV fileset", Map(
      "files" -> "OPERATIONSTEST$/fingerprint-edges-2.csv",
      "header" -> "src,dst,src_link",
      "delimiter" -> ",",
      "src" -> "src",
      "dst" -> "dst",
      "filter" -> ""))
    run("Aggregate edge attribute to vertices", Map(
      "prefix" -> "",
      "direction" -> "outgoing edges",
      "aggregate-src_link" -> "most_common",
      "aggregate-dst" -> "",
      "aggregate-src" -> ""))
    run("Rename vertex attribute", Map("from" -> "src_link_most_common", "to" -> "link"))
    val other = Project("other")
    project.copy(other)
    run("Import vertices and edges from single CSV fileset", Map(
      "files" -> "OPERATIONSTEST$/fingerprint-edges-1.csv",
      "header" -> "src,dst",
      "delimiter" -> ",",
      "src" -> "src",
      "dst" -> "dst",
      "filter" -> ""))
    run("Import project as segmentation", Map(
      "them" -> "other"))
    val seg = project.segmentation("other").project
    run("Define segmentation links from matching attributes", Map(
      "base-id-attr" -> "stringID",
      "seg-id-attr" -> "link"),
      on = seg)
    def belongsTo = project.segmentation("other").belongsTo.toPairSeq
    assert(belongsTo.size == 6)
    run("Fingerprinting between project and segmentation", Map(
      "mrew" -> "0",
      "mo" -> "0",
      "ms" -> "0"),
      on = seg)
    assert(belongsTo.size == 5)
    val similarity = seg.vertexAttributes("fingerprinting_similarity_score")
      .runtimeSafeCast[Double].rdd.values.collect
    assert(similarity.size == 5)
    assert(similarity.filter(_ > 0).size == 2)
  }

  test("Discard loop edges") {
    run("Import vertices and edges from single CSV fileset", Map(
      "files" -> "OPERATIONSTEST$/loop-edges.csv",
      "header" -> "src,dst,color",
      "delimiter" -> ",",
      "src" -> "src",
      "dst" -> "dst",
      "filter" -> ""))
    def colors =
      project.edgeAttributes("color").runtimeSafeCast[String].rdd.values.collect.toSeq.sorted
    assert(colors == Seq("blue", "green", "red"))
    run("Discard loop edges")
    assert(colors == Seq("blue", "green")) // "red" was the loop edge.
  }

  test("Convert vertices into edges") {
    run("Import vertices from CSV files", Map(
      "files" -> "OPERATIONSTEST$/loop-edges.csv",
      "header" -> "src,dst,color",
      "delimiter" -> ",",
      "id-attr" -> "id",
      "filter" -> ""))
    var colors =
      project.vertexAttributes("color").runtimeSafeCast[String].rdd.values.collect.toSeq.sorted
    assert(colors == Seq("blue", "green", "red"))
    run("Convert vertices into edges", Map("src" -> "src", "dst" -> "dst"))
    colors =
      project.edgeAttributes("color").runtimeSafeCast[String].rdd.values.collect.toSeq.sorted
    assert(colors == Seq("blue", "green", "red"))
    val stringIDs =
      project.vertexAttributes("stringID").runtimeSafeCast[String].rdd.values.collect.toSeq.sorted
    assert(stringIDs == Seq("0", "1", "2"))
  }

  test("Viral modeling segment logic") {
    run("Import vertices from CSV files", Map(
      "files" -> "OPERATIONSTEST$/viral-vertices-1.csv",
      "header" -> "id,num",
      "delimiter" -> ",",
      "id-attr" -> "internalID",
      "filter" -> ""))
    run("Import edges for existing vertices from CSV files", Map(
      "files" -> "OPERATIONSTEST$/viral-edges-1.csv",
      "header" -> "src,dst",
      "delimiter" -> ",",
      "attr" -> "id",
      "src" -> "src",
      "dst" -> "dst",
      "filter" -> ""))
    run("Maximal cliques", Map(
      "name" -> "cliques",
      "bothdir" -> "false",
      "min" -> "3"))
    run("Vertex attribute to double", Map(
      "attr" -> "num"))

    run("Viral modeling", Map(
      "prefix" -> "viral",
      "target" -> "num",
      "test_set_ratio" -> "0",
      "max_deviation" -> "0.75",
      "seed" -> "0",
      "iterations" -> "1",
      "min_num_defined" -> "1",
      "min_ratio_defined" -> "0.5"), on = project.segmentation("cliques").project)
    val viral = project.vertexAttributes("viral_num_after_iteration_1").runtimeSafeCast[Double]
    val stringID = project.vertexAttributes("id").runtimeSafeCast[String]
    assert(remapIDs(viral, stringID).collect.toMap == Map(
      "0" -> 0.5,
      "1" -> 0.0,
      "2" -> 1.0,
      "3" -> 2.0,
      "4" -> 0.0,
      "7" -> 3.0))
    assert(project.scalars("viral num coverage initial").value == 5)
    assert(project.scalars("viral num coverage after iteration 1").value == 6)
  }

  test("Viral modeling iteration logic") {
    run("Import vertices from CSV files", Map(
      "files" -> "OPERATIONSTEST$/viral-vertices-2.csv",
      "header" -> "id,num",
      "delimiter" -> ",",
      "id-attr" -> "internalID",
      "filter" -> ""))
    run("Import edges for existing vertices from CSV files", Map(
      "files" -> "OPERATIONSTEST$/viral-edges-2.csv",
      "header" -> "src,dst",
      "delimiter" -> ",",
      "attr" -> "id",
      "src" -> "src",
      "dst" -> "dst",
      "filter" -> ""))
    run("Maximal cliques", Map(
      "name" -> "cliques",
      "bothdir" -> "false",
      "min" -> "3"))
    run("Vertex attribute to double", Map(
      "attr" -> "num"))
    val stringID = project.vertexAttributes("id").runtimeSafeCast[String]

    run("Viral modeling", Map(
      "prefix" -> "viral",
      "target" -> "num",
      "test_set_ratio" -> "0",
      "max_deviation" -> "1.5", // more than square root of 2 to let 100-200 spread on 1000
      "seed" -> "0",
      "iterations" -> "3",
      "min_num_defined" -> "1",
      "min_ratio_defined" -> "0.5"), on = project.segmentation("cliques").project)
    val viral = project.vertexAttributes("viral_num_after_iteration_3").runtimeSafeCast[Double]
    assert(remapIDs(viral, stringID).collect.toSeq.sorted == Seq(
      "0" -> 0.0,
      "1" -> 0.0,
      "10" -> 0.0,
      "100" -> 0.5,
      "2" -> 1.0,
      "20" -> 1.0,
      "200" -> 0.75,
      "3" -> 1.0,
      "30" -> 3.5,
      "4" -> 3.0,
      "5" -> 4.0))
    val spread = project.vertexAttributes("viral_num_spread_over_iterations").runtimeSafeCast[Double]
    assert(remapIDs(spread, stringID).collect.toSeq.sorted == Seq(
      "0" -> 0.0,
      "1" -> 0.0,
      "10" -> 1.0,
      "100" -> 2.0,
      "2" -> 0.0,
      "20" -> 1.0,
      "200" -> 3.0,
      "3" -> 0.0,
      "30" -> 1.0,
      "4" -> 0.0,
      "5" -> 0.0))

    run("Viral modeling", Map(
      "prefix" -> "viral2",
      "target" -> "num",
      "test_set_ratio" -> "0",
      "max_deviation" -> "0.75", // lower deviation changes viral spread to 200 from 30-100
      "seed" -> "0",
      "iterations" -> "3",
      "min_num_defined" -> "1",
      "min_ratio_defined" -> "0.5"), on = project.segmentation("cliques").project)
    val viral2 = project.vertexAttributes("viral2_num_after_iteration_3").runtimeSafeCast[Double]
    assert(remapIDs(viral2, stringID).collect.toSeq.sorted == Seq(
      "0" -> 0.0,
      "1" -> 0.0,
      "10" -> 0.0,
      "100" -> 0.5,
      "2" -> 1.0,
      "20" -> 1.0,
      "200" -> 0.75,
      "3" -> 1.0,
      "30" -> 3.5,
      "4" -> 3.0,
      "5" -> 4.0))
    val spread2 = project.vertexAttributes("viral2_num_spread_over_iterations").runtimeSafeCast[Double]
    assert(remapIDs(spread2, stringID).collect.toSeq.sorted == Seq(
      "0" -> 0.0,
      "1" -> 0.0,
      "10" -> 1.0,
      "100" -> 2.0,
      "2" -> 0.0,
      "20" -> 1.0,
      "200" -> 3.0,
      "3" -> 0.0,
      "30" -> 1.0,
      "4" -> 0.0,
      "5" -> 0.0))

    run("Viral modeling", Map(
      "prefix" -> "viral3",
      "target" -> "num",
      "test_set_ratio" -> "0.4", // to check validation
      "max_deviation" -> "10",
      "seed" -> "0",
      "iterations" -> "2",
      "min_num_defined" -> "1",
      "min_ratio_defined" -> "0.0"), on = project.segmentation("cliques").project)
    val roles3 = project.vertexAttributes("viral3_roles").runtimeSafeCast[String]
    assert(remapIDs(roles3, stringID).collect.toSeq.sorted == Seq(
      "0" -> "train",
      "1" -> "train",
      "10" -> "train",
      "100" -> "train",
      "1000" -> "train",
      "2" -> "train",
      "20" -> "test",
      "200" -> "test",
      "3" -> "test",
      "30" -> "train",
      "4" -> "train",
      "5" -> "test"))
    assert(project.scalars("viral3 num mean absolute prediction error after iteration 1").value == 0.5)
  }

  test("Merge two attributes") {
    run("Example Graph")
    // The unification is used everywhere, I'm just worried about the type equality check.
    intercept[java.lang.AssertionError] {
      run("Merge two attributes", Map("name" -> "x", "attr1" -> "name", "attr2" -> "age"))
    }
    run("Merge two attributes", Map("name" -> "x", "attr1" -> "name", "attr2" -> "gender"))
  }

  test("Aggregate edge attribute to vertices, all directions") {
    run("Example Graph")
    run("Aggregate edge attribute to vertices", Map(
      "prefix" -> "incoming",
      "direction" -> "incoming edges",
      "aggregate-weight" -> "sum",
      "aggregate-comment" -> ""))
    run("Aggregate edge attribute to vertices", Map(
      "prefix" -> "outgoing",
      "direction" -> "outgoing edges",
      "aggregate-weight" -> "sum",
      "aggregate-comment" -> ""))
    run("Aggregate edge attribute to vertices", Map(
      "prefix" -> "all",
      "direction" -> "all edges",
      "aggregate-weight" -> "sum",
      "aggregate-comment" -> ""))
    def value(direction: String) = {
      val attr = project.vertexAttributes(s"${direction}_weight_sum").runtimeSafeCast[Double]
      attr.rdd.collect.toSeq.sorted
    }
    assert(value("incoming") == Seq(0L -> 5.0, 1L -> 5.0))
    assert(value("outgoing") == Seq(0L -> 1.0, 1L -> 2.0, 2L -> 7.0))
    assert(value("all") == Seq(0L -> 6.0, 1L -> 7.0, 2L -> 7.0))
  }

  test("SQL import & export vertices") {
    run("Example Graph")
    val db = s"sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    run("Export vertex attributes to database", Map(
      "db" -> db,
      "table" -> "example_graph",
      "delete" -> "no",
      "attrs" -> "id,name,age,income,gender"))
    run("Import vertices from a database", Map(
      "db" -> db,
      "table" -> "example_graph",
      "columns" -> "name,age,income,gender",
      "key" -> "id",
      "id-attr" -> "x"))
    val name = project.vertexAttributes("name").runtimeSafeCast[String]
    val income = project.vertexAttributes("income").runtimeSafeCast[String]
    assert(name.rdd.values.collect.toSeq.sorted == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
    assert(income.rdd.values.collect.toSeq.sorted == Seq("1000.0", "2000.0"))
  }

  test("SQL import & export edges") {
    run("Example Graph")
    val db = s"sqlite:${dataManager.repositoryPath.resolvedNameWithNoCredentials}/test-db"
    run("Export edge attributes to database", Map(
      "db" -> db,
      "table" -> "example_graph",
      "delete" -> "yes",
      "attrs" -> "weight,comment"))
    run("Import vertices and edges from single database table", Map(
      "db" -> db,
      "table" -> "example_graph",
      "columns" -> "srcVertexId,dstVertexId,weight,comment",
      "key" -> "srcVertexId",
      "src" -> "srcVertexId",
      "dst" -> "dstVertexId"))
    assert(project.vertexSet.rdd.count == 3) // Isolated Joe is lost.
    val weight = project.edgeAttributes("weight").runtimeSafeCast[String]
    val comment = project.edgeAttributes("comment").runtimeSafeCast[String]
    assert(weight.rdd.values.collect.toSeq.sorted == Seq("1.0", "2.0", "3.0", "4.0"))
    assert(comment.rdd.values.collect.toSeq.sorted == Seq("Adam loves Eve", "Bob envies Adam", "Bob loves Eve", "Eve loves Adam"))
  }

  test("CSV import & export vertices") {
    run("Example Graph")
    val path = dataManager.repositoryPath + "/csv-export-test"
    run("Export vertex attributes to file", Map(
      "path" -> path.symbolicName,
      "link" -> "link",
      "attrs" -> "id,name,age,income,gender",
      "format" -> "CSV"))
    val header = (path + "/header").readAsString
    run("Import vertices from CSV files", Map(
      "files" -> (path + "/data/*").symbolicName,
      "header" -> header,
      "delimiter" -> ",",
      "filter" -> "",
      "id-attr" -> "x"))
    val name = project.vertexAttributes("name").runtimeSafeCast[String]
    val income = project.vertexAttributes("income").runtimeSafeCast[String]
    assert(name.rdd.values.collect.toSeq.sorted == Seq("Adam", "Bob", "Eve", "Isolated Joe"))
    assert(income.rdd.values.collect.toSeq.sorted == Seq("", "", "1000.0", "2000.0"))
  }

}

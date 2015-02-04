package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_operations.DynamicValue

class GraphDrawingControllerTest extends FunSuite with TestGraphOp with BigGraphEnvironment {
  val controller = new GraphDrawingController(this)
  val user = com.lynxanalytics.biggraph.serving.User.fake

  test("get center of ExampleGraph with no filters") {
    val g = graph_operations.ExampleGraph()().result
    val req = CenterRequest(
      vertexSetId = g.vertices.gUID.toString,
      count = 1,
      filters = Seq())
    val res = controller.getCenter(user, req)
    assert(res.centers.toSet == Set("0"))
  }

  test("get 5 centers of ExampleGraph with no filters") {
    val g = graph_operations.ExampleGraph()().result
    val req = CenterRequest(
      vertexSetId = g.vertices.gUID.toString,
      count = 5,
      filters = Seq())
    val res = controller.getCenter(user, req)
    assert(res.centers.toSet == Set("0", "1", "2", "3"))
  }

  test("get center of ExampleGraph with filters set") {
    val g = graph_operations.ExampleGraph()().result
    val f = FEVertexAttributeFilter(
      attributeId = g.age.gUID.toString,
      valueSpec = "<=10")
    val req = CenterRequest(
      vertexSetId = g.vertices.gUID.toString,
      count = 1,
      filters = Seq(f))
    val res = controller.getCenter(user, req)
    assert(res.centers.toSet == Set("3"))
  }

  test("get sampled vertex diagram of ExampleGraph with no filters, no attrs") {
    val g = graph_operations.ExampleGraph()().result
    val req = FEGraphRequest(
      vertexSets = Seq(VertexDiagramSpec(
        vertexSetId = g.vertices.gUID.toString,
        filters = Seq(),
        mode = "sampled",
        centralVertexIds = Seq("0", "3"),
        sampleSmearEdgeBundleId = g.edges.gUID.toString,
        attrs = Seq(),
        radius = 1)),
      edgeBundles = Seq(EdgeDiagramSpec(
        srcDiagramId = "idx[0]",
        dstDiagramId = "idx[0]",
        srcIdx = 0,
        dstIdx = 0,
        edgeBundleId = g.edges.gUID.toString,
        filters = Seq(),
        layout3D = false)))
    val res = controller.getComplexView(user, req)
    assert(res.vertexSets.length == 1)
    assert(res.edgeBundles.length == 1)
    assert(res.vertexSets(0).mode == "sampled")
    assert(res.vertexSets(0).vertices.size == 4)
    assert(res.vertexSets(0).vertices.toSet == Set(
      FEVertex(0.0, 0, 0, id = "0", attrs = Map()),
      FEVertex(0.0, 0, 0, id = "1", attrs = Map()),
      FEVertex(0.0, 0, 0, id = "2", attrs = Map()),
      FEVertex(0.0, 0, 0, id = "3", attrs = Map())))
    assert(res.edgeBundles(0).edges.size == 4)
    assert(res.edgeBundles(0).edges.toSet == Set(
      FEEdge(0, 1, 1.0), FEEdge(1, 0, 1.0), FEEdge(2, 0, 1.0), FEEdge(2, 1, 1.0)))
  }

  test("get sampled vertex diagram of ExampleGraph with filters and attrs") {
    val g = graph_operations.ExampleGraph()().result
    val age = g.age.gUID.toString
    val gender = g.gender.gUID.toString

    val vf = FEVertexAttributeFilter(
      attributeId = age,
      valueSpec = "<=25")
    val ef = FEVertexAttributeFilter(
      attributeId = g.comment.gUID.toString,
      valueSpec = "Adam loves Eve")
    val req = FEGraphRequest(
      vertexSets = Seq(VertexDiagramSpec(
        vertexSetId = g.vertices.gUID.toString,
        filters = Seq(vf),
        mode = "sampled",
        centralVertexIds = Seq("0"),
        sampleSmearEdgeBundleId = g.edges.gUID.toString,
        attrs = Seq(age, gender),
        radius = 1)),
      edgeBundles = Seq(EdgeDiagramSpec(
        srcDiagramId = "idx[0]",
        dstDiagramId = "idx[0]",
        srcIdx = 0,
        dstIdx = 0,
        edgeBundleId = g.edges.gUID.toString,
        filters = Seq(ef),
        layout3D = false)))
    val res = controller.getComplexView(user, req)
    assert(res.vertexSets.length == 1)
    assert(res.edgeBundles.length == 1)
    assert(res.vertexSets(0).mode == "sampled")
    assert(res.vertexSets(0).vertices.size == 2)
    assert(res.vertexSets(0).vertices.toSet == Set(
      FEVertex(0.0, 0, 0, id = "0", attrs = Map(
        age -> DynamicValue(20.3, "20.3"),
        gender -> DynamicValue(0.0, "Male"))),
      FEVertex(0.0, 0, 0, id = "1", attrs = Map(
        age -> DynamicValue(18.2, "18.2"),
        gender -> DynamicValue(0.0, "Female")))))
    assert(res.edgeBundles(0).edges.size == 1)
    assert(res.edgeBundles(0).edges.toSet == Set(
      FEEdge(0, 1, 1.0)))
  }

  test("small bucketed view") {
    val g = graph_operations.ExampleGraph()().result
    val req = FEGraphRequest(
      vertexSets = Seq(VertexDiagramSpec(
        vertexSetId = g.vertices.gUID.toString,
        filters = Seq(),
        mode = "bucketed",
        xBucketingAttributeId = g.age.gUID.toString,
        xNumBuckets = 2,
        yBucketingAttributeId = g.gender.gUID.toString,
        yNumBuckets = 2)),
      edgeBundles = Seq(EdgeDiagramSpec(
        srcDiagramId = "idx[0]",
        dstDiagramId = "idx[0]",
        srcIdx = 0,
        dstIdx = 0,
        edgeBundleId = g.edges.gUID.toString,
        filters = Seq(),
        layout3D = false)))
    val res = controller.getComplexView(user, req)
    assert(res.vertexSets.length == 1)
    assert(res.edgeBundles.length == 1)
    assert(res.vertexSets(0).mode == "bucketed")
    assert(res.vertexSets(0).vertices.size == 4)
    assert(res.vertexSets(0).vertices.toSet == Set(
      FEVertex(1.0, 0, 0), FEVertex(2.0, 0, 1), FEVertex(0.0, 1, 0), FEVertex(1.0, 1, 1)))
    assert(res.edgeBundles(0).edges.size == 4)
    assert(res.edgeBundles(0).edges.toSet == Set(
      FEEdge(0, 1, 1.0), FEEdge(3, 0, 1.0), FEEdge(1, 0, 1.0), FEEdge(3, 1, 1.0)))
  }

  test("big bucketed view") {
    val vs = graph_operations.CreateVertexSet(100)().result.vs
    val eop = graph_operations.FastRandomEdgeBundle(0, 2)
    val es = eop(eop.vs, vs).result.es
    val req = FEGraphRequest(
      vertexSets = Seq(VertexDiagramSpec(
        vertexSetId = vs.gUID.toString,
        filters = Seq(),
        mode = "bucketed")),
      edgeBundles = Seq(EdgeDiagramSpec(
        srcDiagramId = "idx[0]",
        dstDiagramId = "idx[0]",
        srcIdx = 0,
        dstIdx = 0,
        edgeBundleId = es.gUID.toString,
        filters = Seq(),
        layout3D = false)))
    val res = controller.getComplexView(user, req)
    assert(res.vertexSets.length == 1)
    assert(res.edgeBundles.length == 1)
    assert(res.vertexSets(0).mode == "bucketed")
    assert(res.vertexSets(0).vertices.size == 1)
    assert(res.vertexSets(0).vertices.toSet == Set(FEVertex(100.0, 0, 0)))
    assert(res.edgeBundles(0).edges.size == 1)
    assert(res.edgeBundles(0).edges.toSet == Set(FEEdge(0, 0, 191.0)))
  }

  test("big bucketed view with filters") {
    val vs = graph_operations.CreateVertexSet(100)().result.vs
    val es = {
      val op = graph_operations.FastRandomEdgeBundle(0, 2)
      op(op.vs, vs).result.es
    }
    val rndVA = {
      val op = graph_operations.AddGaussianVertexAttribute(1)
      op(op.vertices, vs).result.attr
    }
    val rndEA = {
      val op = graph_operations.AddGaussianVertexAttribute(2)
      op(op.vertices, es.asVertexSet).result.attr
    }
    val vf = FEVertexAttributeFilter(
      attributeId = rndVA.gUID.toString,
      valueSpec = ">0")
    val ef = FEVertexAttributeFilter(
      attributeId = rndEA.gUID.toString,
      valueSpec = ">0")
    val req = FEGraphRequest(
      vertexSets = Seq(VertexDiagramSpec(
        vertexSetId = vs.gUID.toString,
        filters = Seq(vf),
        mode = "bucketed")),
      edgeBundles = Seq(EdgeDiagramSpec(
        srcDiagramId = "idx[0]",
        dstDiagramId = "idx[0]",
        srcIdx = 0,
        dstIdx = 0,
        edgeBundleId = es.gUID.toString,
        filters = Seq(ef),
        layout3D = false)))
    val res = controller.getComplexView(user, req)
    assert(res.vertexSets.length == 1)
    assert(res.edgeBundles.length == 1)
    assert(res.vertexSets(0).mode == "bucketed")
    assert(res.vertexSets(0).vertices.size == 1)
    // Should be about 50% of 100.
    assert(res.vertexSets(0).vertices.toSet == Set(FEVertex(49.0, 0, 0)))
    assert(res.edgeBundles(0).edges.size == 1)
    // Should be about 12.5% of 191. (50% src is removed, 50% dst is removed, 50% attribute is <0)
    assert(res.edgeBundles(0).edges.toSet == Set(FEEdge(0, 0, 20.0)))
  }

  test("histogram for double") {
    val g = graph_operations.ExampleGraph()().result
    val req = HistogramSpec(
      attributeId = g.age.gUID.toString,
      vertexFilters = Seq(),
      numBuckets = 4,
      axisOptions = AxisOptions())
    val res = controller.getHistogram(user, req)
    assert(res.labelType == "between")
    assert(res.labels == Seq("2", "14", "26", "38", "50"))
    assert(res.sizes == Seq(1, 2, 0, 1))
  }

  test("histogram for double (partially defined)") {
    val g = graph_operations.ExampleGraph()().result
    val req = HistogramSpec(
      attributeId = g.income.gUID.toString,
      vertexFilters = Seq(),
      numBuckets = 4,
      axisOptions = AxisOptions())
    val res = controller.getHistogram(user, req)
    assert(res.labelType == "between")
    assert(res.labels == Seq("1000", "1250", "1500", "1750", "2000"))
    assert(res.sizes == Seq(1, 0, 0, 1))
  }

  test("histogram for double (logarithmic)") {
    val g = graph_operations.ExampleGraph()().result
    val req = HistogramSpec(
      attributeId = g.age.gUID.toString,
      vertexFilters = Seq(),
      numBuckets = 4,
      axisOptions = AxisOptions(logarithmic = true))
    val res = controller.getHistogram(user, req)
    assert(res.labelType == "between")
    assert(res.labels == Seq("2", "4", "10", "22", "50"))
    assert(res.sizes == Seq(1, 0, 2, 1))
  }

  test("histogram for string") {
    val g = graph_operations.ExampleGraph()().result
    val req = HistogramSpec(
      attributeId = g.gender.gUID.toString,
      vertexFilters = Seq(),
      numBuckets = 4,
      axisOptions = AxisOptions())
    val res = controller.getHistogram(user, req)
    assert(res.labelType == "bucket")
    assert(res.labels == Seq("Female", "Male"))
    assert(res.sizes == Seq(1, 3))
  }

  test("histogram for edges") {
    val g = graph_operations.ExampleGraph()().result
    val req = HistogramSpec(
      attributeId = g.weight.gUID.toString,
      vertexFilters = Seq(),
      edgeFilters = Seq(),
      numBuckets = 4,
      axisOptions = AxisOptions(),
      edgeBundleId = g.edges.gUID.toString)
    val res = controller.getHistogram(user, req)
    assert(res.labelType == "between")
    assert(res.labels == Seq("1.0", "1.8", "2.5", "3.3", "4.0"))
    assert(res.sizes == Seq(1, 1, 1, 1))
  }

  test("histogram for edges with filter") {
    val g = graph_operations.ExampleGraph()().result
    val f = FEVertexAttributeFilter(
      attributeId = g.weight.gUID.toString,
      valueSpec = ">1")
    val req = HistogramSpec(
      attributeId = g.weight.gUID.toString,
      vertexFilters = Seq(),
      edgeFilters = Seq(f),
      numBuckets = 4,
      axisOptions = AxisOptions(),
      edgeBundleId = g.edges.gUID.toString)
    val res = controller.getHistogram(user, req)
    assert(res.labelType == "between")
    assert(res.labels == Seq("1.0", "1.8", "2.5", "3.3", "4.0"))
    assert(res.sizes == Seq(0, 1, 1, 1))
  }

  test("scalar") {
    val g = graph_operations.ExampleGraph()().result
    val op = graph_operations.CountVertices()
    val scalar = op(op.vertices, g.vertices).result.count
    val req = ScalarValueRequest(
      scalarId = scalar.gUID.toString,
      calculate = true)
    val res = controller.getScalarValue(user, req)
    assert(res.defined == true)
    assert(res.double == 4)
    assert(res.string == "4")
  }
}

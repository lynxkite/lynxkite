package com.lynxanalytics.biggraph.controllers

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._
import com.lynxanalytics.biggraph.graph_operations
import com.lynxanalytics.biggraph.graph_operations.DynamicValue
import com.lynxanalytics.biggraph.graph_util.Scripting._

import scala.concurrent.{ Await, duration }

class GraphDrawingControllerTest extends FunSuite with TestGraphOp {
  val controller = new GraphDrawingController(this)
  val user = com.lynxanalytics.biggraph.serving.User.fake

  test("get center of ExampleGraph with no filters") {
    val g = graph_operations.ExampleGraph()().result
    val req = CenterRequest(
      vertexSetId = g.vertices.gUID.toString,
      count = 1,
      filters = Seq())
    val res = Await.result(controller.getCenter(user, req), duration.Duration.Inf)
    assert(res.centers.toSet == Set("0"))
  }

  test("get 5 centers of ExampleGraph with no filters") {
    val g = graph_operations.ExampleGraph()().result
    val req = CenterRequest(
      vertexSetId = g.vertices.gUID.toString,
      count = 5,
      filters = Seq())
    val res = Await.result(controller.getCenter(user, req), duration.Duration.Inf)
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
    val res = Await.result(controller.getCenter(user, req), duration.Duration.Inf)
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
        layout3D = false,
        relativeEdgeDensity = false)))
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

  test("edge diagram with incomplete edge attribute as weight") {
    val g = graph_operations.ExampleGraph()().result
    val incomplete =
      g.comment.deriveX[Double]("x.indexOf('love') == -1 ? undefined : 1").gUID.toString
    val req = FEGraphRequest(
      vertexSets = Seq(VertexDiagramSpec(
        vertexSetId = g.vertices.gUID.toString,
        filters = Seq(),
        mode = "sampled",
        centralVertexIds = Seq("0"),
        sampleSmearEdgeBundleId = g.edges.gUID.toString,
        radius = 1)),
      edgeBundles = Seq(EdgeDiagramSpec(
        srcDiagramId = "idx[0]",
        dstDiagramId = "idx[0]",
        srcIdx = 0,
        dstIdx = 0,
        edgeBundleId = g.edges.gUID.toString,
        filters = Seq(),
        layout3D = false,
        relativeEdgeDensity = false,
        edgeWeightId = incomplete)))
    val res = controller.getComplexView(user, req)
    assert(res.vertexSets(0).vertices.toSet == Set(
      FEVertex(0.0, 0, 0, id = "0", attrs = Map()),
      FEVertex(0.0, 0, 0, id = "1", attrs = Map()),
      FEVertex(0.0, 0, 0, id = "2", attrs = Map())))
    assert(res.edgeBundles(0).edges.toSet == Set(
      FEEdge(0, 1, 1.0, Map()),
      FEEdge(1, 0, 1.0, Map()),
      FEEdge(2, 0, 0.0, Map()),
      FEEdge(2, 1, 1.0, Map())))
  }

  test("get sampled vertex diagram of ExampleGraph with filters and attrs") {
    val g = graph_operations.ExampleGraph()().result
    val age = g.age.gUID.toString
    val gender = g.gender.gUID.toString
    val weight = g.weight.gUID.toString

    val vf = FEVertexAttributeFilter(
      attributeId = age,
      valueSpec = "<=25")
    val ef = FEVertexAttributeFilter(
      attributeId = g.comment.gUID.toString,
      // Second option should be dropped due to connected vertices being filtered.
      valueSpec = "Adam loves Eve,Bob envies Adam")
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
        layout3D = false,
        relativeEdgeDensity = false,
        attrs = Seq(AggregatedAttribute(weight, "sum")))))
    val res = controller.getComplexView(user, req)
    assert(res.vertexSets.length == 1)
    assert(res.edgeBundles.length == 1)
    assert(res.vertexSets(0).mode == "sampled")
    assert(res.vertexSets(0).vertices.size == 2)
    assert(res.vertexSets(0).vertices.toSet == Set(
      FEVertex(0.0, 0, 0, id = "0", attrs = Map(
        age -> DynamicValue("20.3", double = Some(20.3)),
        gender -> DynamicValue("Male"))),
      FEVertex(0.0, 0, 0, id = "1", attrs = Map(
        age -> DynamicValue("18.2", double = Some(18.2)),
        gender -> DynamicValue("Female")))))
    assert(res.edgeBundles(0).edges.size == 1)
    assert(res.edgeBundles(0).edges.toSet == Set(
      FEEdge(0, 1, 1.0, Map(weight + ":sum" -> DynamicValue("1", double = Some(1.0))))))
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
        layout3D = false,
        relativeEdgeDensity = false)))
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

  test("relative edge density") {
    val ed = EdgeDiagramResponse(
      srcDiagramId = "vertexDiagramId",
      dstDiagramId = "vertexDiagramId",
      srcIdx = 0,
      dstIdx = 0,
      edges = Seq(FEEdge(0, 1, 1.0), FEEdge(0, 2, 1.0), FEEdge(1, 2, 1.0), FEEdge(1, 3, 1.0)),
      layout3D = Map()
    )
    val vd = VertexDiagramResponse(
      diagramId = "vertexDiagramId",
      vertices = Seq(FEVertex(1.0, 0, 0), FEVertex(5.0, 0, 1), FEVertex(10.0, 1, 0), FEVertex(2.0, 1, 1)),
      mode = "bucketed"
    )
    val res = controller.relativeEdgeDensity(ed, vd, vd)
    val edgeSizes = res.edges.map(_.size)
    assert(edgeSizes == Seq(0.2, 0.1, 0.02, 0.1))
  }

  test("relative edge density with zeros") {
    val ed = EdgeDiagramResponse(
      srcDiagramId = "vertexDiagramId",
      dstDiagramId = "vertexDiagramId",
      srcIdx = 0,
      dstIdx = 0,
      edges = Seq(FEEdge(0, 1, 1.0), FEEdge(0, 2, 1.0), FEEdge(1, 2, 1.0), FEEdge(1, 3, 1.0)),
      layout3D = Map()
    )
    val vd = VertexDiagramResponse(
      diagramId = "vertexDiagramId",
      vertices = Seq(FEVertex(0.0, 0, 0), FEVertex(5.0, 0, 1), FEVertex(10.0, 1, 0), FEVertex(2.0, 1, 1)),
      mode = "bucketed"
    )
    val res = controller.relativeEdgeDensity(ed, vd, vd)
    val edgeSizes = res.edges.map(_.size)
    assert(edgeSizes == Seq(0.0, 0.0, 0.02, 0.1))
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
        layout3D = false,
        relativeEdgeDensity = false)))
    val res = controller.getComplexView(user, req)
    assert(res.vertexSets.length == 1)
    assert(res.edgeBundles.length == 1)
    assert(res.vertexSets(0).mode == "bucketed")
    assert(res.vertexSets(0).vertices.size == 1)
    assert(res.vertexSets(0).vertices.toSet == Set(FEVertex(100.0, 0, 0)))
    assert(res.edgeBundles(0).edges.size == 1)
    // Approximately two edges per vertex.
    assert(res.edgeBundles(0).edges.toSet == Set(FEEdge(0, 0, 182.0)))
  }

  test("big bucketed view with buckets") {
    val vs = graph_operations.CreateVertexSet(100)().result.vs
    val eop = graph_operations.FastRandomEdgeBundle(0, 2)
    val rnd = vs.randomAttribute(1)
    val es = eop(eop.vs, vs).result.es
    val req = FEGraphRequest(
      vertexSets = Seq(VertexDiagramSpec(
        vertexSetId = vs.gUID.toString,
        filters = Seq(),
        mode = "bucketed",
        xBucketingAttributeId = rnd.gUID.toString,
        xNumBuckets = 2)),
      edgeBundles = Seq(EdgeDiagramSpec(
        srcDiagramId = "idx[0]",
        dstDiagramId = "idx[0]",
        srcIdx = 0,
        dstIdx = 0,
        edgeBundleId = es.gUID.toString,
        filters = Seq(),
        layout3D = false,
        relativeEdgeDensity = false)))
    val res = controller.getComplexView(user, req)
    assert(res.vertexSets.length == 1)
    assert(res.edgeBundles.length == 1)
    assert(res.vertexSets(0).mode == "bucketed")
    assert(res.vertexSets(0).vertices.size == 2)
    // Roughly 50, 50.
    assert(res.vertexSets(0).vertices.toSet == Set(FEVertex(49.0, 0, 0), FEVertex(51.0, 1, 0)))
    assert(res.edgeBundles(0).edges.size == 4)
    // Roughly 25% each of 182 edges (=45). Bigger buckets should have more edges in them.
    assert(res.edgeBundles(0).edges.toSet ==
      Set(FEEdge(0, 0, 47.0), FEEdge(0, 1, 42.0), FEEdge(1, 0, 45.0), FEEdge(1, 1, 48.0)))
  }

  test("small bucketed view with filters") {
    val vs = graph_operations.CreateVertexSet(100)().result.vs
    val es = {
      val op = graph_operations.FastRandomEdgeBundle(0, 2)
      op(op.vs, vs).result.es
    }
    val rndVA = vs.randomAttribute(1)
    val rndEA = es.idSet.randomAttribute(2)
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
        layout3D = false,
        relativeEdgeDensity = false)))
    val res = controller.getComplexView(user, req)
    assert(res.vertexSets.length == 1)
    assert(res.edgeBundles.length == 1)
    assert(res.vertexSets(0).mode == "bucketed")
    assert(res.vertexSets(0).vertices.size == 1)
    // Should be about 50% of 100.
    assert(res.vertexSets(0).vertices.toSet == Set(FEVertex(51.0, 0, 0)))
    assert(res.edgeBundles(0).edges.size == 1)
    // Should be about 12.5% of 182. (50% src is removed, 50% dst is removed, 50% attribute is <0)
    assert(res.edgeBundles(0).edges.toSet == Set(FEEdge(0, 0, 26.0)))
  }

  test("big bucketed view with filters") {
    val vs = graph_operations.CreateVertexSet(500)().result.vs
    val es = {
      val op = graph_operations.FastRandomEdgeBundle(0, 2)
      op(op.vs, vs).result.es
    }
    val rndVA = vs.randomAttribute(1)
    val rndEA = es.idSet.randomAttribute(2)
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
        layout3D = false,
        relativeEdgeDensity = false)))
    val res = controller.getComplexView(user, req)
    assert(res.vertexSets.length == 1)
    assert(res.edgeBundles.length == 1)
    assert(res.vertexSets(0).mode == "bucketed")
    assert(res.vertexSets(0).vertices.size == 1)
    // Should be about 50% of 500.
    assert(res.vertexSets(0).vertices.toSet == Set(FEVertex(253.0, 0, 0)))
    assert(res.edgeBundles(0).edges.size == 1)
    // Should be about 12.5% of 990. (50% src is removed, 50% dst is removed, 50% attribute is <0)
    assert(res.edgeBundles(0).edges.toSet == Set(FEEdge(0, 0, 117.0)))
  }

  test("histogram for double") {
    val g = graph_operations.ExampleGraph()().result
    val req = HistogramSpec(
      attributeId = g.age.gUID.toString,
      vertexFilters = Seq(),
      numBuckets = 4,
      axisOptions = AxisOptions(),
      sampleSize = 50000)
    val res = Await.result(controller.getHistogram(user, req), duration.Duration.Inf)
    assert(res.labelType == "between")
    assert(res.labels == Seq("2.0", "14.1", "26.2", "38.2", "50.3"))
    assert(res.sizes == Seq(1, 2, 0, 1))
  }

  test("histogram for double (partially defined)") {
    val g = graph_operations.ExampleGraph()().result
    val req = HistogramSpec(
      attributeId = g.income.gUID.toString,
      vertexFilters = Seq(),
      numBuckets = 4,
      axisOptions = AxisOptions(),
      sampleSize = 50000)
    val res = Await.result(controller.getHistogram(user, req), duration.Duration.Inf)
    assert(res.labelType == "between")
    assert(res.labels == Seq("1000.0", "1250.0", "1500.0", "1750.0", "2000.0"))
    assert(res.sizes == Seq(1, 0, 0, 1))
  }

  test("histogram for double (logarithmic)") {
    val g = graph_operations.ExampleGraph()().result
    val req = HistogramSpec(
      attributeId = g.age.gUID.toString,
      vertexFilters = Seq(),
      numBuckets = 4,
      axisOptions = AxisOptions(logarithmic = true),
      sampleSize = 50000)
    val res = Await.result(controller.getHistogram(user, req), duration.Duration.Inf)
    assert(res.labelType == "between")
    assert(res.labels == Seq("2.0", "4.5", "10.0", "22.5", "50.3"))
    assert(res.sizes == Seq(1, 0, 2, 1))
  }

  test("histogram for string") {
    val g = graph_operations.ExampleGraph()().result
    val req = HistogramSpec(
      attributeId = g.gender.gUID.toString,
      vertexFilters = Seq(),
      numBuckets = 4,
      axisOptions = AxisOptions(),
      sampleSize = 50000)
    val res = Await.result(controller.getHistogram(user, req), duration.Duration.Inf)
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
      edgeBundleId = g.edges.gUID.toString,
      sampleSize = 50000)
    val res = Await.result(controller.getHistogram(user, req), duration.Duration.Inf)
    assert(res.labelType == "between")
    assert(res.labels == Seq("1.00", "1.75", "2.50", "3.25", "4.00"))
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
      edgeBundleId = g.edges.gUID.toString,
      sampleSize = 50000)
    val res = Await.result(controller.getHistogram(user, req), duration.Duration.Inf)
    assert(res.labelType == "between")
    assert(res.labels == Seq("1.00", "1.75", "2.50", "3.25", "4.00"))
    assert(res.sizes == Seq(0, 1, 1, 1))
  }

  test("histogram with smaller sample size as data size") {
    val vs = graph_operations.CreateVertexSet(100)().result.vs
    val rndVA = vs.randomAttribute(1)
    val req = HistogramSpec(
      attributeId = rndVA.gUID.toString,
      vertexFilters = Seq(),
      numBuckets = 10,
      axisOptions = AxisOptions(),
      sampleSize = 1)
    val res = Await.result(controller.getHistogram(user, req), duration.Duration.Inf)
    assert(res.labelType == "between")
    assert(res.sizes.count(x => x == 100) == 1)
  }

  test("histogram without sampling") {
    val g = graph_operations.ExampleGraph()().result
    val req = HistogramSpec(
      attributeId = g.name.gUID.toString,
      vertexFilters = Seq(),
      numBuckets = 4,
      axisOptions = AxisOptions(),
      sampleSize = -1)
    val res = Await.result(controller.getHistogram(user, req), duration.Duration.Inf)
    assert(res.labelType == "bucket")
    assert(res.sizes == Seq(1, 1, 1, 1))
  }

  test("scalar") {
    val g = graph_operations.ExampleGraph()().result
    val scalar = graph_operations.Count.run(g.vertices)
    val req = ScalarValueRequest(scalarId = scalar.gUID.toString)
    val res = Await.result(controller.getScalarValue(user, req), duration.Duration.Inf)
    assert(res.defined == true)
    assert(res.string == "4")
    assert(res.double == Some(4))
  }

}

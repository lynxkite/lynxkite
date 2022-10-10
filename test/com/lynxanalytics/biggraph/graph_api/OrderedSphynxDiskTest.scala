package com.lynxanalytics.biggraph.graph_api

import org.scalatest.funsuite.AnyFunSuite

import com.lynxanalytics.biggraph.graph_api.GraphTestUtils._
import com.lynxanalytics.biggraph.graph_operations.ExampleGraph

class OrderedSphynxDiskTest extends AnyFunSuite with TestMetaGraphManager with TestDataManager {
  test(
    "Writing and reloading entities to ordered Sphynx disk does not change entities",
    com.lynxanalytics.biggraph.SphynxOnly) {
    implicit val metaManager = cleanMetaManager
    implicit val dataManager = cleanDataManager
    val operation = ExampleGraph()
    val outputs = metaManager.apply(operation).outputs

    val origAttributes = outputs.attributes.mapValues(attr => get(attr))
    val origVertexSets = outputs.vertexSets.mapValues(vs => get(vs))
    val origEdgeBundles = outputs.edgeBundles.mapValues(es => get(es))
    val origScalars = outputs.scalars.mapValues(s => get(s))
    // Trigger actual computation. Entities are written to ordered Sphynx disk in the background.
    println(origVertexSets)

    // Clean up everything other than ordered Sphynx disk.
    implicit val newDataManager = new DataManager(dataManager.domains.map(d =>
      d match {
        case d: SparkDomain => cleanSparkDomain
        case d: OrderedSphynxDisk => d
        case d: SphynxDomain => {
          d.clear()
          d
        }
        case d: ScalaDomain => new ScalaDomain
      }))
    val orderedSphynxDisk = dataManager.domains(0)

    val readAttributes = outputs.attributes.mapValues(attr => get(attr)(mm = metaManager, dm = newDataManager))
    val readVertexSets = outputs.vertexSets.mapValues(vs => get(vs)(mm = metaManager, dm = newDataManager))
    val readEdgeBundles = outputs.edgeBundles.mapValues(es => get(es)(mm = metaManager, dm = newDataManager))
    val readScalars = outputs.scalars.mapValues(s => get(s)(dm = newDataManager))

    assert(readAttributes == origAttributes)
    assert(readVertexSets == origVertexSets)
    assert(readEdgeBundles == origEdgeBundles)
    assert(readScalars == origScalars)
  }
}

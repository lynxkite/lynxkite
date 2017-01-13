package com.lynxanalytics.biggraph.graph_operations

import java.io.File

import com.lynxanalytics.biggraph.graph_api.DataSet
import com.lynxanalytics.biggraph.graph_api.OutputBuilder
import com.lynxanalytics.biggraph.graph_api.RuntimeContext
import com.lynxanalytics.biggraph.graph_api._
import org.geotools.data.FileDataStoreFinder
import org.geotools.data.simple.SimpleFeatureIterator
import org.opengis.feature.simple.SimpleFeature

object LookupRegion extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    val coordinates = vertexAttribute[Tuple2[Double, Double]](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val region = vertexAttribute[String](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = LookupRegion()
}

import com.lynxanalytics.biggraph.graph_operations.LookupRegion._

case class LookupRegion() extends TypedMetaGraphOp[Input, Output] {

  implicit class ScalaFeatureIterator(it: SimpleFeatureIterator) extends Iterator[SimpleFeature] {
    override def hasNext: Boolean = it.hasNext
    override def next(): SimpleFeature = it.next()
  }

  @transient override lazy val inputs = new Input()
  override def toJson = Json.obj()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas
    val resource = getClass.getResource("/graph_operations/FindRegionTest/earth.shp")
    val finder = FileDataStoreFinder.getDataStore(new File(resource.getPath))
    val x = (for (feature <- finder.getFeatureSource.getFeatures().features())
      yield (feature.getDefaultGeometryProperty.getBounds,
      feature.getAttribute("TZID").asInstanceOf[String])).toVector

    output(o.region, inputs.coordinates.rdd.flatMapValues {
      case (lat, lon) => x.find(f => f._1.contains(lon, lat)).map(f => f._2)
    })
  }
}
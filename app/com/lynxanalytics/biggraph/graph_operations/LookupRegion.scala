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
    val latitude = vertexAttribute[Double](vertices)
    val longitude = vertexAttribute[Double](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val region = vertexAttribute[String](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = LookupRegion(
    (j \ "shapefile").as[String], (j \ "attribute").as[String])
}

import com.lynxanalytics.biggraph.graph_operations.LookupRegion._

case class LookupRegion(shapefile: String, attribute: String) extends TypedMetaGraphOp[Input, Output] {

  implicit class ScalaFeatureIterator(it: SimpleFeatureIterator) extends Iterator[SimpleFeature] {
    override def hasNext: Boolean = it.hasNext
    override def next(): SimpleFeature = it.next()
  }

  @transient override lazy val inputs = new Input()
  override def toJson = Json.obj("shapefile" -> shapefile, "attribute" -> attribute)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas

    val coordinates = inputs.latitude.rdd.sortedJoin(inputs.longitude.rdd)
    val finder = FileDataStoreFinder.getDataStore(new File(shapefile))
    val x = (for (feature <- finder.getFeatureSource.getFeatures().features())
      yield (feature.getDefaultGeometryProperty.getBounds,
      feature.getAttribute(attribute).asInstanceOf[String])).toVector

    output(o.region, coordinates.flatMapValues {
      case (lat, lon) => x.find(f => f._1.contains(lon, lat)).map(f => f._2)
    })
  }
}

// Looks up a location in a Shapefile and returns a specified Shapefile attribute.
package com.lynxanalytics.biggraph.graph_operations

import java.io.File

import com.lynxanalytics.biggraph.graph_api.DataSet
import com.lynxanalytics.biggraph.graph_api.OutputBuilder
import com.lynxanalytics.biggraph.graph_api.RuntimeContext
import com.lynxanalytics.biggraph.graph_api._
import org.geotools.data.FileDataStoreFinder
import org.geotools.data.simple.SimpleFeatureIterator
import org.opengis.feature.simple.SimpleFeature
import org.opengis.geometry.BoundingBox

import scala.collection.immutable.Seq

object LookupRegion extends OpFromJson {
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    val coordinates = vertexAttribute[Tuple2[Double, Double]](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val attribute = vertexAttribute[String](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = LookupRegion(
    (j \ "shapefile").as[String], (j \ "attribute").as[String])
}

import com.lynxanalytics.biggraph.graph_operations.LookupRegion._

case class LookupRegion(shapefile: String, attribute: String) extends TypedMetaGraphOp[Input, Output] {

  // This class makes it possible to use scalaic tools on SimpleFeatureIterators automatically
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

    val dataStore = FileDataStoreFinder.getDataStore(new File(shapefile))
    val iterator = dataStore.getFeatureSource.getFeatures().features()
    val regionAttributeMapping: Seq[(BoundingBox, String)] =
      iterator.map(feature =>
        (
          feature.getDefaultGeometryProperty.getBounds,
          feature.getAttribute(attribute).asInstanceOf[String]
        )
      ).toVector
    iterator.close()
    dataStore.dispose()

    output(o.attribute, inputs.coordinates.rdd.flatMapValues {
      case (lat, lon) => regionAttributeMapping.find(f => f._1.contains(lon, lat)).map(f => f._2)
    })
  }
}

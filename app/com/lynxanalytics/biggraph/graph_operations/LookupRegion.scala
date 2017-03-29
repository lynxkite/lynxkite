// Looks up a location in a Shapefile and returns a specified Shapefile attribute.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api.DataSet
import com.lynxanalytics.biggraph.graph_api.OutputBuilder
import com.lynxanalytics.biggraph.graph_api.RuntimeContext
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Shapefile
import com.lynxanalytics.biggraph.graph_util.Shapefile._

import org.opengis.geometry.BoundingBox

import scala.collection.immutable.Seq

object LookupRegion extends OpFromJson {
  private val ignoreUnsupportedShapesParameter =
    NewParameter[Boolean]("ignoreUnsupportedShapes", false)
  class Input extends MagicInputSignature {
    val vertices = vertexSet
    val coordinates = vertexAttribute[Tuple2[Double, Double]](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val attribute = vertexAttribute[String](inputs.vertices.entity)
  }
  def fromJson(j: JsValue) = LookupRegion(
    (j \ "shapefile").as[String],
    (j \ "attribute").as[String],
    ignoreUnsupportedShapesParameter.fromJson(j))
}

import com.lynxanalytics.biggraph.graph_operations.LookupRegion._

case class LookupRegion(
    shapefile: String,
    attribute: String,
    ignoreUnsupportedShapes: Boolean) extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true

  @transient override lazy val inputs = new Input()
  override def toJson = Json.obj(
    "shapefile" -> shapefile,
    "attribute" -> attribute) ++
    LookupRegion.ignoreUnsupportedShapesParameter.toJson(ignoreUnsupportedShapes)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas

    val sf = Shapefile(shapefile)
    sf.assertHasAttributeName(attribute)
    // Only keep necessary values to minimize serialization. BoundingBox is for speed optimization.
    val regionAttributeMapping: Seq[(BoundingBox, AnyRef /* Geometry */ , String)] =
      sf.iterator.map(feature =>
        (
          feature.getDefaultGeometryProperty.getBounds,
          feature.getDefaultGeometryProperty.getValue,
          // A feature may not have the specified attribute.
          Option(feature.getAttribute(attribute)).map(_.toString())
        )
      ).filter(_._3.nonEmpty).map { case (b, g, a) => (b, g, a.get) }.toVector
    sf.close()

    val factory = new com.vividsolutions.jts.geom.GeometryFactory()
    output(o.attribute, inputs.coordinates.rdd.flatMapValues {
      case (lat, lon) => regionAttributeMapping
        .find {
          case (bounds, geometry, _) =>
            // Do the faster BoundingBox check first.
            bounds.contains(lon, lat) && (geometry match {
              // The actual classes and ways to check differ for implementations. These 2 cases
              // are probably not the complete list.
              case g: org.opengis.geometry.Geometry =>
                g.contains(new org.geotools.geometry.DirectPosition2D(lon, lat))
              case g: com.vividsolutions.jts.geom.Geometry =>
                g.contains(factory.createPoint(new com.vividsolutions.jts.geom.Coordinate(lon, lat)))
              case _ =>
                assert(ignoreUnsupportedShapes, "Unknown shape type found in Shapefile.")
                false
            })
        }.map(_._3)
    })
  }
}

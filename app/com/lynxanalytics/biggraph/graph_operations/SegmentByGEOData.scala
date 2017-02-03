// Creates a segmentation from features and their attributes in a Shapefile. The vertices and
// the segments are connected based on the distances of the position vertex attributes and the
// geometry attributes of the segments (Shapefile features).
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.graph_api.DataSet
import com.lynxanalytics.biggraph.graph_api.OutputBuilder
import com.lynxanalytics.biggraph.graph_api.RuntimeContext
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_util.Shapefile
import com.lynxanalytics.biggraph.graph_util.Shapefile._

import com.lynxanalytics.biggraph.spark_util.Implicits._

import org.opengis.geometry.BoundingBox

import org.apache.spark

import scala.collection.immutable.Seq

object SegmentByGEOData extends OpFromJson {
  class Input(val attrNames: Seq[String]) extends MagicInputSignature {
    val vertices = vertexSet
    val coordinates = vertexAttribute[Tuple2[Double, Double]](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val segments = vertexSet
    val belongsTo = edgeBundle(inputs.vertices.entity, segments)
    val attributes = inputs.attrNames.map(attrName => vertexAttribute[String](segments, Symbol(attrName)))
  }
  def fromJson(j: JsValue) = SegmentByGEOData(
    (j \ "shapefile").as[String], (j \ "distance").as[Double], (j \ "attrNames").as[Seq[String]])
}

import com.lynxanalytics.biggraph.graph_operations.SegmentByGEOData._

case class SegmentByGEOData(shapefile: String, distance: Double, attrNames: Seq[String]) extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true

  @transient override lazy val inputs = new Input(attrNames)
  override def toJson = Json.obj("shapefile" -> shapefile, "distance" -> distance, "attrNames" -> attrNames)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val ds = inputDatas

    val sf = Shapefile(shapefile)
    assert(sf.attrNames == attrNames, "Number of output attributes does not match.")
    val geometries: Seq[(Long, AnyRef /* Geometry */ , Vector[Option[String]])] =
      sf.iterator.map(feature => (
        feature.getDefaultGeometryProperty.getValue,
        sf.attrNames.zipWithIndex.map {
          case (a, i) => Option(feature.getAttribute(a)).map(_.toString)
        }.toVector
      )).toVector.zipWithIndex.map { case ((g, a), i) => (i.toLong, g, a) }
    sf.close()

    val partitioner = rc.partitionerForNRows(geometries.size)
    val segmentAttributes = rc.sparkContext
      .parallelize(geometries.map { case (i, _, a) => (i, a) }, partitioner.numPartitions)
      .sortUnique(partitioner)

    val factory = new com.vividsolutions.jts.geom.GeometryFactory()
    val links = inputs.coordinates.rdd.mapValues {
      case (lat, lon) => geometries
        .filter {
          case (_, geometry, _) =>
            // TODO(gsvigruha): check distance not contains.
            geometry match {
              // The actual classes and ways to check differ for implementations. These 2 cases
              // are probably not the complete list.
              case g: org.opengis.geometry.Geometry =>
                g.contains(new org.geotools.geometry.DirectPosition2D(lon, lat))
              case g: com.vividsolutions.jts.geom.Geometry =>
                g.contains(factory.createPoint(new com.vividsolutions.jts.geom.Coordinate(lon, lat)))
              case _ => false
            }
        }.map { case (sid, _, _) => sid }
    }.flatMapValues(sids => sids)

    output(o.segments, segmentAttributes.mapValues(_ => ()))
    for (i <- 0 until attrNames.size) {
      output(o.attributes(i), segmentAttributes.mapValues(v => v(i)).flatMapValues(s => s))
    }
    output(o.belongsTo, links
      .randomNumbered(inputs.vertices.rdd.partitioner.get.numPartitions)
      .mapValues { case (vid, sid) => Edge(vid, sid) })
  }
}

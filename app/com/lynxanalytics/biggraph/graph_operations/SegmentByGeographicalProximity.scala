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

object SegmentByGeographicalProximity extends OpFromJson {
  class Input(val attrNames: Seq[String]) extends MagicInputSignature {
    val vertices = vertexSet
    val coordinates = vertexAttribute[Tuple2[Double, Double]](vertices)
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val segments = vertexSet
    val belongsTo = edgeBundle(inputs.vertices.entity, segments)
    val attributes = inputs.attrNames.map(
      attrName => vertexAttribute[String](segments, Symbol(attrName)))
  }
  def fromJson(j: JsValue) = SegmentByGeographicalProximity(
    (j \ "shapefile").as[String],
    (j \ "distance").as[Double],
    (j \ "attrNames").as[Seq[String]],
    (j \ "onlyknownFeatures").as[Boolean])
}

import com.lynxanalytics.biggraph.graph_operations.SegmentByGeographicalProximity._

case class SegmentByGeographicalProximity(
    shapefile: String,
    distance: Double,
    attrNames: Seq[String],
    onlyknownFeatures: Boolean) extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true

  @transient override lazy val inputs = new Input(attrNames)
  override def toJson = Json.obj(
    "shapefile" -> shapefile,
    "distance" -> distance,
    "attrNames" -> attrNames,
    "onlyknownFeatures" -> onlyknownFeatures)
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
        // This returns a Java Object representing a some kind of geometry.
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
      .persist(spark.storage.StorageLevel.DISK_ONLY)

    val factory = new com.vividsolutions.jts.geom.GeometryFactory()
    val links = inputs.coordinates.rdd.mapValues {
      case (lat, lon) => getSegmentIdForPosition(lat, lon, geometries, factory)
    }.flatMapValues(sids => sids)

    output(o.segments, segmentAttributes.mapValues(_ => ()))
    for (i <- 0 until attrNames.size) {
      output(o.attributes(i), segmentAttributes.mapValues(v => v(i)).flatMapValues(s => s))
    }
    output(o.belongsTo, links
      .randomNumbered(inputs.vertices.rdd.partitioner.get.numPartitions)
      .mapValues { case (vid, sid) => Edge(vid, sid) })
  }

  private def getSegmentIdForPosition(
    lat: Double,
    lon: Double,
    geometries: Seq[(Long, AnyRef /* Geometry */ , Vector[Option[String]])],
    factory: com.vividsolutions.jts.geom.GeometryFactory): Iterable[ID] = {
    geometries
      .filter {
        case (_, geometry, _) =>
          geometry match {
            // The actual classes and ways to check differ for implementations.
            case g: com.vividsolutions.jts.geom.Geometry => g.isWithinDistance(
              factory.createPoint(new com.vividsolutions.jts.geom.Coordinate(lon, lat)), distance)
            case _ =>
              assert(!onlyknownFeatures, "Unknown shape type found in Shapefile.")
              false
          }
      }.map { case (sid, _, _) => sid }
  }
}

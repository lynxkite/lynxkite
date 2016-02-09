// The metagraph is the graph of MetaGraphEntities and MetaGraphOperationInstances.
// This is useful for debugging and demonstration purposes.
package com.lynxanalytics.biggraph.graph_operations

import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object MetaGraph extends OpFromJson {
  class Output(implicit instance: MetaGraphOperationInstance) extends MagicOutput(instance) {
    val vs = vertexSet
    val vGUID = vertexAttribute[String](vs)
    val vKind = vertexAttribute[String](vs)
    val vName = vertexAttribute[String](vs)
    val vProgress = vertexAttribute[Double](vs)
    val es = edgeBundle(vs, vs)
    val eKind = edgeAttribute[String](es)
    val eName = edgeAttribute[String](es)
  }
  def fromJson(j: JsValue) = MetaGraph(
    (j \ "timestamp").as[String], None)
}
import MetaGraph._
case class MetaGraph(timestamp: String, env: Option[SparkFreeEnvironment])
    extends TypedMetaGraphOp[NoInput, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new NoInput()
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance)
  override def toJson = Json.obj("timestamp" -> timestamp)

  private def shortClass(o: Any) = o.getClass.getName.split('.').last

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    assert(env.nonEmpty, "You cannot rerun a MetaGraph operation.")
    val ops = env.get.metaGraphManager.getOperationInstances.toSeq

    val vs = {
      val attrs = ops.flatMap {
        case (guid, inst) =>
          val op = (guid.toString, "Operation", shortClass(inst.operation), Double.NaN)
          val outputs = inst.outputs.all.map {
            case (name, entity) =>
              val progress = env.get.entityProgressManager.computeProgress(entity)
              (entity.gUID.toString, shortClass(entity), name.name, progress)
          }
          op +: outputs.toSeq
      }
      val partitioner = rc.partitionerForNRows(attrs.size)
      rc.sparkContext.parallelize(attrs, partitioner.numPartitions).randomNumbered()
    }

    val es = {
      val attrsBySrc = ops.flatMap {
        case (guid, inst) =>
          val inputs = inst.inputs.all.map {
            case (name, entity) => entity.gUID.toString -> (guid.toString, "Input", name.name)
          }
          val outputs = inst.outputs.all.map {
            case (name, entity) => guid.toString -> (entity.gUID.toString, "Output", name.name)
          }
          inputs ++ outputs
      }
      val partitioner = rc.partitionerForNRows(attrsBySrc.size)
      val bySrc = rc.sparkContext.parallelize(attrsBySrc, partitioner.numPartitions)
      val guidToID = vs.map { case (id, (guid, _, _, _)) => guid -> id }
      val byDst = bySrc.join(guidToID).map {
        case (srcGUID, ((dstGUID, kind, name), srcID)) => dstGUID -> (srcID, kind, name)
      }
      byDst.join(guidToID).map {
        case (dstGUID, ((srcID, kind, name), dstID)) => (Edge(srcID, dstID), kind, name)
      }.randomNumbered()
    }

    output(o.vs, vs.mapValues(_ => ()))
    output(o.vGUID, vs.mapValues(_._1))
    output(o.vKind, vs.mapValues(_._2))
    output(o.vName, vs.mapValues(_._3))
    output(o.vProgress, vs.filter(_._2._2 != "Operation").mapValues(_._4))
    output(o.es, es.mapValues(_._1))
    output(o.vKind, es.mapValues(_._2))
    output(o.vName, es.mapValues(_._3))
  }
}

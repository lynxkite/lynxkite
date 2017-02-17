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
  // "env" cannot be serialized, so this operation cannot be re-run and "env" is ignored for
  // equality.
  override def equals(other: Any): Boolean =
    other match {
      case otherOp: MetaGraph => otherOp.timestamp == timestamp
      case _ => false
    }
  override lazy val hashCode = gUID.hashCode

  private def shortClass(o: Any) = o.getClass.getName.split('.').last

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    assert(env.nonEmpty, "You cannot rerun a MetaGraph operation.")
    val ops = env.get.metaGraphManager.getOperationInstances.toSeq

    val vs = ops.flatMap {
      // For each operation we create a vertex for the operation itself and each of the outputs.
      case (guid, inst) =>
        val op = (guid.toString, "Operation", shortClass(inst.operation), Double.NaN)
        val outputs = inst.outputs.all.map {
          case (name, entity) =>
            val progress = env.get.entityProgressManager.computeProgress(entity)
            (entity.gUID.toString, shortClass(entity), name.name, progress)
        }
        op +: outputs.toSeq
    }
    val vsNumbered = (0L until vs.size).zip(vs)
    val vsRDD = {
      val partitioner = rc.partitionerForNRows(vs.size)
      rc.sparkContext.parallelize(vsNumbered, partitioner.numPartitions).sortUnique(partitioner)
    }
    val vsByGUID = vsNumbered.map { case (id, (guid, _, _, _)) => guid -> id }.toMap

    val es = ops.flatMap {
      // For each operation we create an edge from each input and to each output.
      case (guid, inst) =>
        val inputs = inst.inputs.all.map {
          case (i, e) => (Edge(vsByGUID(e.gUID.toString), vsByGUID(guid.toString)), "Input", i.name)
        }
        val outputs = inst.outputs.all.map {
          case (o, e) => (Edge(vsByGUID(guid.toString), vsByGUID(e.gUID.toString)), "Output", o.name)
        }
        inputs ++ outputs
    }
    val esNumbered = (0L until es.size).zip(es)
    val esRDD = {
      val partitioner = rc.partitionerForNRows(es.size)
      rc.sparkContext.parallelize(esNumbered, partitioner.numPartitions).sortUnique(partitioner)
    }

    output(o.vs, vsRDD.mapValues(_ => ()))
    output(o.vGUID, vsRDD.mapValues(_._1))
    output(o.vKind, vsRDD.mapValues(_._2))
    output(o.vName, vsRDD.mapValues(_._3))
    output(o.vProgress, vsRDD.filter(_._2._2 != "Operation").mapValues(_._4))
    output(o.es, esRDD.mapValues(_._1))
    output(o.eKind, esRDD.mapValues(_._2))
    output(o.eName, esRDD.mapValues(_._3))
  }
}

package com.lynxanalytics.biggraph.graph_operations

import org.apache.spark.SparkContext.rddToPairRDDFunctions

import com.lynxanalytics.biggraph.graph_api._

class EdgeAttributeInput[T] extends MagicInputSignature {
  val src = vertexSet
  val dst = vertexSet
  val es = edgeBundle(src, dst)
  val attr = edgeAttribute[T](es)
}

object EdgeAttributeToString {
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: EdgeAttributeInput[T])
      extends MagicOutput(instance) {
    val attr = edgeAttribute[String](inputs.es.entity)
  }
}
case class EdgeAttributeToString[T]()
    extends TypedMetaGraphOp[EdgeAttributeInput[T], EdgeAttributeToString.Output[T]] {
  import EdgeAttributeToString._
  @transient override lazy val inputs = new EdgeAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[T]()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag
    output(o.attr, inputs.attr.rdd.mapValues(_.toString))
  }
}

object EdgeAttributeToDouble {
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: EdgeAttributeInput[String])
      extends MagicOutput(instance) {
    val attr = edgeAttribute[Double](inputs.es.entity)
  }
}
case class EdgeAttributeToDouble()
    extends TypedMetaGraphOp[EdgeAttributeInput[String], EdgeAttributeToDouble.Output] {
  import EdgeAttributeToDouble._
  @transient override lazy val inputs = new EdgeAttributeInput[String]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.attr, inputs.attr.rdd.flatMapValues(str =>
      if (str.nonEmpty) Some(str.toDouble) else None))
  }
}

class VertexAttributeInput[T] extends MagicInputSignature {
  val vs = vertexSet
  val attr = vertexAttribute[T](vs)
}

object VertexAttributeToString {
  class Output[T](implicit instance: MetaGraphOperationInstance,
                  inputs: VertexAttributeInput[T])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[String](inputs.vs.entity)
  }
}
case class VertexAttributeToString[T]()
    extends TypedMetaGraphOp[VertexAttributeInput[T], VertexAttributeToString.Output[T]] {
  import VertexAttributeToString._
  @transient override lazy val inputs = new VertexAttributeInput[T]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output[T]()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output[T],
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    implicit val ct = inputs.attr.data.classTag
    output(o.attr, inputs.attr.rdd.mapValues(_.toString))
  }
}

object VertexAttributeToDouble {
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: VertexAttributeInput[String])
      extends MagicOutput(instance) {
    val attr = vertexAttribute[Double](inputs.vs.entity)
  }
}
case class VertexAttributeToDouble()
    extends TypedMetaGraphOp[VertexAttributeInput[String], VertexAttributeToDouble.Output] {
  import VertexAttributeToDouble._
  @transient override lazy val inputs = new VertexAttributeInput[String]
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    output(o.attr, inputs.attr.rdd.flatMapValues(str =>
      if (str.nonEmpty) Some(str.toDouble) else None))
  }
}

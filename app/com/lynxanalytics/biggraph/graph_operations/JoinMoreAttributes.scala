package com.lynxanalytics.biggraph.graph_operations

import scala.reflect.runtime.universe._

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.spark_util.Implicits._

object JoinMoreAttributes {
  type ByType = (Seq[Double], Seq[String], Seq[Vector[Any]])
  
  class Input(numAttrCount: Int, strAttrCount: Int, vecAttrCount: Int)
      extends MagicInputSignature {
    val vs = vertexSet
    val numAttrs = (0 until numAttrCount).map(i => vertexAttribute[Double](vs, Symbol("numAttr-" + i)))
    val strAttrs = (0 until strAttrCount).map(i => vertexAttribute[String](vs, Symbol("strAttr-" + i)))
    val vecAttrs = (0 until vecAttrCount).map(i => vertexAttribute[Vector[Any]](vs, Symbol("vecAttr-" + i)))
  }
  class Output(implicit instance: MetaGraphOperationInstance,
               inputs: Input) extends MagicOutput(instance) {
    val attr = vertexAttribute[ByType](inputs.vs.entity)
  }
}
import JoinMoreAttributes._
case class JoinMoreAttributes(numAttrCount: Int, strAttrCount: Int, vecAttrCount: Int)
    extends TypedMetaGraphOp[Input, Output] {
  override val isHeavy = true
  @transient override lazy val inputs = new Input(numAttrCount, strAttrCount, vecAttrCount)
  def outputMeta(instance: MetaGraphOperationInstance) = new Output()(instance, inputs)

  def execute(inputDatas: DataSet,
              o: Output,
              output: OutputBuilder,
              rc: RuntimeContext): Unit = {
    implicit val id = inputDatas
    val numJoined = {
      val noAttrs = inputs.vs.rdd.mapValues(_ => Seq[Double]())
      inputs.numAttrs.foldLeft(noAttrs) { (rdd, attr) =>
        rdd.sortedJoin(attr.rdd).mapValues {
          case (attrs, attr) => attrs :+ attr
        }
      }
    }
    val strJoined = {
      val noAttrs = inputs.vs.rdd.mapValues(_ => Seq[String]())
      inputs.strAttrs.foldLeft(noAttrs) { (rdd, attr) =>
        rdd.sortedJoin(attr.rdd).mapValues {
          case (attrs, attr) => attrs :+ attr
        }
      }
    }
    val vecJoined = {
      val noAttrs = inputs.vs.rdd.mapValues(_ => Seq[Vector[Any]]())
      inputs.vecAttrs.foldLeft(noAttrs) { (rdd, attr) =>
        rdd.sortedJoin(attr.rdd).mapValues {
          case (attrs, attr) => attrs :+ attr
        }
      }
    }
    val allJoined = numJoined.sortedJoin(strJoined).sortedJoin(vecJoined).mapValues {
      case ((nums, strs), vecs) => (nums, strs, vecs)
    }
    output(o.attr, allJoined)
  }
}
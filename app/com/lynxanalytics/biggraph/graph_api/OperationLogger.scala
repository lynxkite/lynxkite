package com.lynxanalytics.biggraph.graph_api

import play.api.libs.json

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutorService
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

trait OpLogger {
  def addOutput(output: SafeFuture[EntityData]): SafeFuture[EntityData]
}
object OperationLogger {
  var cnt = 0
  def getId(): Int = synchronized {
    cnt += 1
    cnt
  }
  def get(instance: MetaGraphOperationInstance, ec: ExecutionContextExecutorService): OpLogger = {
    new OperationLogger(instance, ec)
  }
}

case class OutputInfo(name: String, count: Option[Long], executionTimeInSeconds: Long)

class OperationLogger(instance: MetaGraphOperationInstance, ec: ExecutionContextExecutorService) extends OpLogger {
  val outputInfoList = new mutable.MutableList[OutputInfo]
  implicit val executionContext = ec
  private def outputsLoggedSoFar = outputInfoList.length
  private var closed = false
  private val creationTime = System.currentTimeMillis
  private val id = OperationLogger.getId()
  private val name = s"${instance.operation}[$id]"
  private val numOutput =
    if (instance.operation.isHeavy) instance.outputs.nonScalars.size
    else 0

  private def elapsedMs(): Long = {
    (System.currentTimeMillis() - creationTime)
  }
  private def threadId() = Thread.currentThread().getId.toString
  private def info(): String =
    s"name: $name thread: ${threadId()}" +
      s" numOutputs: $numOutput written: $outputsLoggedSoFar" +
      s" elasped: ${elapsedMs()}"

  override def addOutput(output: SafeFuture[EntityData]): SafeFuture[EntityData] = {
    output.map {
      o =>
        synchronized {
          val rddData = o.asInstanceOf[EntityRDDData[_]]
          assert(!closed, s"${info()}  output $o  was added after close!")
          val oi = OutputInfo(rddData.entity.toString, rddData.count, elapsedMs() / 1000)
          outputInfoList += oi
          if (outputsLoggedSoFar == numOutput) {
            close()
          }
          o
        }
    }
  }

  private def close() = {
    implicit val formatOutput = json.Json.format[OutputInfo]
    val formatter = implicitly[json.Format[List[OutputInfo]]]
    println(formatter.writes(outputInfoList.toList))
    closed = true
  }
  override def finalize() = {
    if (!closed && numOutput != 0) {
      val msg = s"${info()}  ***NOT CLOSED***"
      log.error(msg)
      println(msg)
    }
  }
}

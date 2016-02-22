// OperationLogger will log useful performance data about an operation, including information about
// the inputs and the outputs.

package com.lynxanalytics.biggraph.graph_api

import play.api.libs.json
import scala.concurrent.ExecutionContextExecutorService

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

abstract class OperationLogger {
  def addOutput(output: SafeFuture[EntityData]): Unit
  def addInput(input: SafeFuture[EntityData]): Unit
  def close(): Unit
}

object OperationLogger {
  def apply(instance: MetaGraphOperationInstance, ec: ExecutionContextExecutorService): OperationLogger = {
    new JsonOperationLogger(instance, ec)
  }
}

class JsonOperationLogger(instance: MetaGraphOperationInstance,
                          implicit val ec: ExecutionContextExecutorService) extends OperationLogger {
  private val marker = "JSON_OPERATION_LOGGER_MARKER"
  case class OutputInfo(name: String, count: Option[Long], executionTimeInSeconds: Long)
  case class InputInfo(name: String, count: Option[Long])

  private val outputInfoList = scala.collection.mutable.Queue[SafeFuture[OutputInfo]]()
  private val inputInfoList = scala.collection.mutable.Queue[SafeFuture[InputInfo]]()
  private val creationTime = System.currentTimeMillis

  private def elapsedMs(): Long = {
    (System.currentTimeMillis() - creationTime)
  }

  override def addOutput(output: SafeFuture[EntityData]): Unit = {
    outputInfoList += output.map {
      o =>
        synchronized {
          val rddData = o.asInstanceOf[EntityRDDData[_]]
          val oi = OutputInfo(rddData.entity.name.name, rddData.count, elapsedMs() / 1000)
          oi
        }
    }
  }

  override def addInput(input: SafeFuture[EntityData]): Unit = {
    inputInfoList += input.map {
      i =>
        synchronized {
          val rddData = i.asInstanceOf[EntityRDDData[_]]
          val count = rddData.count.getOrElse(rddData.rdd.count())
          val ii = InputInfo(rddData.entity.toString, Some(count))
          ii
        }
    }
  }

  override def close(): Unit = {
    val outputs = SafeFuture.sequence(outputInfoList)
    val inputs = SafeFuture.sequence(inputInfoList)

    val both = SafeFuture.sequence(List(outputs, inputs))
    both.onSuccess {
      case _ =>
        if (instance.operation.isHeavy) {
          dump(inputs.value.get.get, outputs.value.get.get)
        }
    }
    both.onFailure {
      case _ =>
        log.error("Error on closing OperationLog")
    }
  }

  private def dump(inputs: Seq[InputInfo], outputs: Seq[OutputInfo]): Unit = {

    implicit val formatInput = json.Json.format[InputInfo]
    implicit val formatOutput = json.Json.format[OutputInfo]

    val out = json.Json.obj(
      "name" -> instance.operation.toString,
      "guid" -> instance.operation.gUID.toString,
      "executionTimeInSeconds" -> json.JsNumber(elapsedMs() / 1000),
      "inputs" -> inputs,
      "outputs" -> outputs
    )
    log.info(s"$marker $out")
  }
}

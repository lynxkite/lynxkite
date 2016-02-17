package com.lynxanalytics.biggraph.graph_api

import play.api.libs.json
import scala.concurrent.ExecutionContextExecutorService

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

abstract class OperationLogger {
  def addOutput(output: SafeFuture[EntityData]): Unit
  def addInput(input: SafeFuture[EntityData]): Unit
  def close(): Unit
}

class NullOperationLogger extends OperationLogger {
  def addOutput(output: SafeFuture[EntityData]): Unit = {}
  def addInput(input: SafeFuture[EntityData]): Unit = {}
  def close(): Unit = {}
}

object OperationLogger {
  var cnt = 0
  def getId(): Int = synchronized {
    cnt += 1
    cnt
  }

  val nullLogger = util.Properties.envOrNone("KITE_NO_OPERATION_LOGGER").isDefined

  def apply(instance: MetaGraphOperationInstance, ec: ExecutionContextExecutorService): OperationLogger = {
    if (nullLogger) new NullOperationLogger
    else new JsonOperationLogger(instance, ec)
  }
}

class JsonOperationLogger(instance: MetaGraphOperationInstance, implicit val ec: ExecutionContextExecutorService) extends OperationLogger {
  private val marker = "JSON_OPERATION_LOGGER_MARKER"
  case class OutputInfo(name: String, count: Option[Long], executionTimeInSeconds: Long)
  case class InputInfo(name: String, count: Option[Long])

  private val outputInfoList = scala.collection.mutable.Queue[SafeFuture[OutputInfo]]()
  private val inputInfoList = scala.collection.mutable.Queue[SafeFuture[InputInfo]]()
  private def outputsLoggedSoFar = outputInfoList.length
  private var closed = false
  private val creationTime = System.currentTimeMillis
  private val id = OperationLogger.getId()
  private val name = s"${instance.operation}[$id]"
  private val numOutput =
    if (instance.operation.isHeavy) instance.outputs.nonScalars.size
    else 0
  private val numInput =
    if (instance.operation.isHeavy) instance.inputs.nonScalars.size
    else 0

  private def elapsedMs(): Long = {
    (System.currentTimeMillis() - creationTime)
  }
  private def threadId() = Thread.currentThread().getId.toString
  private def info(): String =
    s"name: $name thread: ${threadId()}" +
      s" numOutputs: $numOutput written: $outputsLoggedSoFar" +
      s" numInputs: $numInput" +
      s" elasped: ${elapsedMs()}"

  override def addOutput(output: SafeFuture[EntityData]): Unit = {
    outputInfoList += output.map {
      o =>
        synchronized {
          val rddData = o.asInstanceOf[EntityRDDData[_]]
          val oi = OutputInfo(rddData.entity.name.name, rddData.count, elapsedMs() / 1000)
          assert(!closed, s"${info()}  output $o  was added after close!")
          oi
        }
    }
  }

  def addInput(input: SafeFuture[EntityData]): Unit = {
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
    assert(outputInfoList.length == numOutput,
      s"Output info list mismatch: ${outputInfoList.length} != $numOutput")
    assert(inputInfoList.length == numInput,
      s"Input info list mismatch: ${inputInfoList.length} != $numInput")
    val outputs = SafeFuture.sequence(outputInfoList)
    val inputs = SafeFuture.sequence(inputInfoList)

    val both = SafeFuture.sequence(List(outputs, inputs))
    both.onSuccess {
      case _ =>
        if (instance.operation.isHeavy) {
          dump(inputs.value.get.get, outputs.value.get.get)
        }
        closed = true
    }
    both.onFailure {
      case _ =>
        log.error(s"Error on closing OperationLog ${info()}")
        closed = true
    }
  }

  def dump(inputs: Seq[InputInfo], outputs: Seq[OutputInfo]): Unit = {
    val inputJson = {
      implicit val formatOutput = json.Json.format[InputInfo]
      val formatter = implicitly[json.Format[Seq[InputInfo]]]
      formatter.writes(inputs)
    }

    val outputJson = {
      implicit val formatOutput = json.Json.format[OutputInfo]
      val formatter = implicitly[json.Format[Seq[OutputInfo]]]
      formatter.writes(outputs)
    }
    val out = json.Json.obj(
      "name" -> json.JsString(instance.operation.toString),
      "guid" -> json.JsString(instance.operation.gUID.toString),
      "executionTimeInSeconds" -> json.JsNumber(elapsedMs() / 1000),
      "inputs" -> inputJson,
      "outputs" -> outputJson
    )
    log.info(s"$marker $out")
    println(s"$marker $out")
  }

  override def finalize() = {
    if (!closed && numOutput != 0) {
      val msg = s"${info()}  ***NOT CLOSED***"
      log.error(msg)
      println(msg)
    }
  }
}

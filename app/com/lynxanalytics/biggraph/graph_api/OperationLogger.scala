package com.lynxanalytics.biggraph.graph_api

import com.google.cloud.hadoop.repackaged.com.google.common.io.BaseEncoding
import java.io.{ FileInputStream, File }
import play.api.libs.json
import scala.concurrent.ExecutionContextExecutorService

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

case class OutputInfo(name: String, count: Option[Long], executionTimeInSeconds: Long)
case class InputInfo(name: String, count: Option[Long])

abstract class OpLogger {
  def addOutput(output: SafeFuture[EntityData]): Unit
  def addInput(input: SafeFuture[EntityData]): Unit
  def close(): Unit
  def dump(inputs: Seq[InputInfo], outputs: Seq[OutputInfo]): Unit
}

object OperationLogger {
  var cnt = 0
  def getId(): Int = synchronized {
    cnt += 1
    cnt
  }

  def getAbsolutelyUniqueId(): String = {
    val randomSource = new File("/dev/urandom")
    assert(randomSource.exists, s"Can't open ${randomSource.getAbsolutePath}!")
    val fis = new FileInputStream(randomSource)
    val bytes = new Array[Byte](16)
    try {
      fis.read(bytes)
    } catch {
      case x: Throwable => throw x
    } finally {
      fis.close()
    }
    BaseEncoding.base16().encode(bytes)
  }

  def get(instance: MetaGraphOperationInstance, ec: ExecutionContextExecutorService): OpLogger = {
    new OperationLogger(instance, ec)
  }
}

class OperationLogger(instance: MetaGraphOperationInstance, implicit val ec: ExecutionContextExecutorService) extends OpLogger {
  val outputInfoList = scala.collection.mutable.Queue[SafeFuture[OutputInfo]]()
  val inputInfoList = scala.collection.mutable.Queue[SafeFuture[InputInfo]]()

  //  implicit val executionContext = ec
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

  override def dump(inputs: Seq[InputInfo], outputs: Seq[OutputInfo]): Unit = {
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
      "time" -> json.JsNumber(elapsedMs() / 1000),
      "inputs" -> inputJson,
      "outputs" -> outputJson
    )
    println(json.Json.prettyPrint(out))
  }

  override def finalize() = {
    if (!closed && numOutput != 0) {
      val msg = s"${info()}  ***NOT CLOSED***"
      log.error(msg)
      println(msg)
    }
  }
}

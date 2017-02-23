// OperationLogger will log useful performance data about an operation, including information about
// the inputs and the outputs.

package com.lynxanalytics.biggraph.graph_api

import com.lynxanalytics.biggraph.graph_util.KiteInstanceInfo
import com.lynxanalytics.biggraph.graph_util.ControlledFutures
import play.api.libs.json
import scala.concurrent.ExecutionContextExecutorService

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

class OperationLogger(instance: MetaGraphOperationInstance,
                      implicit val ec: ExecutionContextExecutorService) {
  private val marker = "OPERATION_LOGGER_MARKER"
  case class OutputInfo(name: String, gUID: String, partitions: Int, count: Option[Long])
  case class InputInfo(name: String, gUID: String, partitions: Int, count: Option[Long])

  private val outputInfoList = scala.collection.mutable.Queue[SafeFuture[OutputInfo]]()
  private val inputInfoList = scala.collection.mutable.Queue[InputInfo]()
  private var startTime = -1L
  private var stopTime = -1L

  private def elapsedMs(): Long = {
    assert(startTime != -1, s"elapsedMs() called before startTimer(), name: ${instance.operation.toString}")
    assert(stopTime != -1, s"elapsedMs() called before stopTimer(), name: ${instance.operation.toString}")
    stopTime - startTime
  }

  def addOutput(output: SafeFuture[EntityData]): Unit = {
    outputInfoList += output.map {
      o =>
        val rddData = o.asInstanceOf[EntityRDDData[_]]
        OutputInfo(
          rddData.entity.name.name,
          rddData.entity.gUID.toString,
          rddData.rdd.partitions.size,
          rddData.count)
    }
  }

  def startTimer(): Unit = {
    assert(startTime == -1, "startTimer() called more than once")
    startTime = System.currentTimeMillis()
  }

  def stopTimer(): Unit = {
    assert(stopTime == -1, "stopTimer() called more than once")
    stopTime = System.currentTimeMillis()
  }
  def addInput(name: String, input: EntityData): Unit = inputInfoList.synchronized {
    //    println(s"Adding $name  input: $input")
    if (instance.operation.isHeavy) input match {
      case rddData: EntityRDDData[_] =>
        inputInfoList +=
          InputInfo(
            name,
            rddData.entity.gUID.toString,
            rddData.rdd.partitions.size,
            rddData.count)
      case _ => // Ignore scalars
    }
  }

  def logWhenReady(loggedFutures: ControlledFutures): Unit = {
    val outputsFuture = SafeFuture.sequence(outputInfoList)
    outputsFuture.map {
      outputs =>
        try {
          loggedFutures.register {
            dump(outputs)
          }
        } catch {
          case t: Throwable => ()
        }
    }
  }

  private def dump(outputs: Seq[OutputInfo]): Unit = {
    if (outputs.nonEmpty) {
      try {
        implicit val formatInput = json.Json.format[InputInfo]
        implicit val formatOutput = json.Json.format[OutputInfo]

        val instanceProperties = json.Json.obj(
          "kiteVersion" -> KiteInstanceInfo.kiteVersion,
          "sparkVersion" -> KiteInstanceInfo.sparkVersion,
          "instanceName" -> KiteInstanceInfo.instanceName
        )

        val inputs = inputInfoList.synchronized { inputInfoList.sortBy(_.name) }

        val out = json.Json.obj(
          "instanceProperties" -> instanceProperties,
          "name" -> instance.operation.toString,
          "timestamp" -> com.lynxanalytics.biggraph.graph_util.Timestamp.toString,
          "guid" -> instance.operation.gUID.toString,
          "elapsedMs" -> elapsedMs(),
          "inputs" -> inputs,
          "outputs" -> outputs.sortBy(_.name)
        )
        log.info(s"$marker $out")
      } catch {
        case t: Throwable =>
          println("dump failed: " + t)
          println("name: " + instance.operation.toString)
          println(s"start time: $startTime")
          println(s"stop time: $stopTime")
          println("Outputs: ")
          for (o <- outputs) {
            println(s"  $o")
          }
          println("Inputs: ")
          try {
            for (i <- inputInfoList) {
              println(s"  $i")
            }
          } catch {
            case x: Throwable =>
              println("x trace")
              x.printStackTrace()
              println("x trace ends")
          }
          t.printStackTrace()
          println("*****")
      }
    }
  }
}

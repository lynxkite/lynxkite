// An API that allows controlling a running LynxKite instance via JSON commands through a Unix pipe.
package com.lynxanalytics.biggraph.serving

import play.api.libs.json
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import com.lynxanalytics.biggraph.BigGraphProductionEnvironment
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.graph_operations.DynamicValue

object PipeAPI {

  case class Command(kind: String, payload: json.JsObject)
  case class NewProjectRequest()
  case class NewCheckpointResponse(id: String)
  case class OperationRequest(checkpoint: String, operation: String, parameters: Map[String, String])
  case class ScalarRequest(checkpoint: String, scalar: String)
  implicit val rCommand = json.Json.reads[Command]
  //implicit val rNewProjectRequest = json.Json.reads[NewProjectRequest]
  implicit val wNewCheckpointResponse = json.Json.writes[NewCheckpointResponse]
  implicit val rOperationRequest = json.Json.reads[OperationRequest]
  implicit val rScalarRequest = json.Json.reads[ScalarRequest]
  implicit val wScalarResponse = json.Json.writes[DynamicValue]

  val env = BigGraphProductionEnvironment
  implicit val metaGraphManager = env.metaGraphManager
  implicit val dataManager = env.dataManager

  val pipeFile = LoggedEnvironment.envOrNone("KITE_API_PIPE").map(new java.io.File(_))

  // Starts a daemon thread if KITE_API_PIPE is configured.
  def maybeStart() = {
    for (f <- pipeFile) {
      if (!f.exists) {
        val mkfifo = sys.process.Process(Seq("mkfifo", f.getAbsolutePath)).run
        assert(mkfifo.exitValue == 0, s"Could not create Unix pipe $f")
      }
      val p = new PipeAPI(f)
      p.start
    }
  }
}
class PipeAPI(pipe: java.io.File) extends Thread("Pipe API") {
  import PipeAPI._
  setDaemon(true)

  override def run() = {
    while (true) {
      val input = scala.io.Source.fromFile(pipe, "utf-8")
      log.info(s"Pipe API reading from $pipe.")
      for (line <- input.getLines) {
        try {
          println(line)
          val cmd = json.Json.parse(line).as[Command]
          println(cmd.kind)
          println(cmd.payload)
        } catch {
          case t: Throwable => log.error(s"Error while processing $line:", t)
        }
      }
    }
  }
}

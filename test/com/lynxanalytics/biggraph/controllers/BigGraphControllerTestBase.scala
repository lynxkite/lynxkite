package com.lynxanalytics.biggraph.controllers

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import org.scalatest.{ FunSuite, BeforeAndAfterEach }

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class BigGraphControllerTestBase extends FunSuite with TestGraphOp with BeforeAndAfterEach {
  val controller = new BigGraphController(this)
  val user = com.lynxanalytics.biggraph.serving.User.fake

  def createDirectory(name: String, privacy: String = "public-write") = {
    controller.createDirectory(
      user,
      CreateDirectoryRequest(name = name, privacy = privacy))
  }

  override def beforeEach() = {
    val path = SymbolPath("projects")
    if (metaGraphManager.tagExists(path)) {
      for (t <- metaGraphManager.lsTag(path)) {
        metaGraphManager.rmTag(t)
      }
    }
  }
}

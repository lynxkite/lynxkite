package com.lynxanalytics.biggraph.controllers

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_api.Scripting._

class BigGraphControllerTestBase extends AnyFunSuite with TestGraphOp with BeforeAndAfterEach {
  val controller = new BigGraphController(this)
  val user = com.lynxanalytics.biggraph.serving.User.singleuser

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

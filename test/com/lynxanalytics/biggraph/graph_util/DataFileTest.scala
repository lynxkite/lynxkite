package com.lynxanalytics.biggraph.graph_util

import org.scalatest.FunSuite
import org.apache.hadoop

import scala.util.Random
import scala.util.matching.Regex

class RootInfo(val resolution: String, val accessKey: String = "", val secretKey: String = "") {
  override def toString = resolution + " [" + accessKey + "] [" + secretKey + "]"
}

object RootRepository {
  private val pathResolutions = scala.collection.mutable.Map[String, RootInfo]()
  private val symbolicRootPattern = "([$][A-Z]+)(.*)".r
  private val s3nPattern = "(s3n?)://(.+):(.+)@(.+)".r

  // To facilitate testing
  def randomRootSymbol = "$" + Random.nextString(20).map(x => ((x % 26) + 'A').toChar)
  def getDummyRootName(rootPath: String): String = {
    val name = randomRootSymbol
    if (rootPath.startsWith("file:"))
      registerRoot(name, rootPath)
    else
      registerRoot(name, "file:" + rootPath)
    name
  }

  private def rootSymbolSyntaxIsOK(rootSymbol: String): Boolean = {
    rootSymbol match {
      case symbolicRootPattern(_, rest) => rest.isEmpty
      case _ => false
    }
  }

  def getRootInfo(rootSymbol: String) = pathResolutions(rootSymbol)

  def registerRoot(rootSymbol: String, rootResolution: String) = {
    assert(!pathResolutions.contains(rootSymbol), s"Root symbol $rootSymbol already set")
    assert(rootSymbolSyntaxIsOK(rootSymbol), s"Invalid root symbol syntax: $rootSymbol")
    pathResolutions(rootSymbol) =
      rootResolution match {
        case s3nPattern(scheme, accessKey, secretKey, path) =>
          new RootInfo(scheme + "://" + path, accessKey, secretKey)
        case symbolicRootPattern(parent, path) =>
          new RootInfo(pathResolutions(parent).resolution + path,
            pathResolutions(parent).accessKey, pathResolutions(parent).secretKey)
        case _ =>
          new RootInfo(rootResolution)
      }
  }

}

class DataFileTest extends FunSuite {

  test("Test basic RootRepository asserts") {
    RootRepository.registerRoot("$BABABA", "mamam")
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("$BABABA", "mamam")
    }
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("@KJHKJSDDSJ", "mamam")
    }
    intercept[java.lang.AssertionError] {
      RootRepository.registerRoot("$KJHKJSDDSJ/haha", "mamam")
    }
  }

  test("Test basic RootRepository logic") {
    RootRepository.registerRoot("$HELLO", "/bello")
    assert(RootRepository.getRootInfo("$HELLO").resolution == "/bello")
    RootRepository.registerRoot("$UPLOAD", "$HELLO/uploads")
    assert(RootRepository.getRootInfo("$UPLOAD").resolution == "/bello/uploads")

    RootRepository.registerRoot("$AWAY", "s3n://access:secret@lynx-bnw-data")
    val r = RootRepository.getRootInfo("$AWAY")
    assert(r.resolution == "s3n://lynx-bnw-data")
    assert(r.accessKey == "access")
    assert(r.secretKey == "secret")

    RootRepository.registerRoot("$FARAWAY", "$AWAY/uploads")
    val rr = RootRepository.getRootInfo("$FARAWAY")
    assert(rr.resolution == "s3n://lynx-bnw-data/uploads")
    assert(rr.accessKey == "access")
    assert(rr.secretKey == "secret")
  }

}

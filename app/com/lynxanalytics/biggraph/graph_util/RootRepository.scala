// Singleton class that controls the symbolic root names for file handling.

package com.lynxanalytics.biggraph.graph_util

import scala.util.Random

object RootRepository {
  private val pathResolutions = scala.collection.mutable.Map[String, String]()
  private val symbolicRootPattern = "([A-Z]+[$])(.*)".r

  // To facilitate testing
  def randomRootSymbol = Random.nextString(20).map(x => ((x % 26) + 'A').toChar) + "$"
  def getDummyRootName(rootPath: String): String = {
    val name = randomRootSymbol
    if (rootPath.startsWith("/"))
      registerRoot(name, "file:" + rootPath)
    else
      registerRoot(name, rootPath)
    name
  }

  def splitSymbolicPattern(str: String): (String, String) = {
    str match {
      case symbolicRootPattern(rootSymbol, relativePath) =>
        (rootSymbol, relativePath)
    }
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
    val resolvedResolution = rootResolution match {
      case symbolicRootPattern(root, rest) => pathResolutions(root) + rest
      case _ => rootResolution
    }
    pathResolutions(rootSymbol) = resolvedResolution
  }

}

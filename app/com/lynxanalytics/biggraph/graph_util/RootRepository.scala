// Singleton class that controls the symbolic root names for file handling.

package com.lynxanalytics.biggraph.graph_util

import scala.io.Source

object RootRepository {
  private val pathResolutions = scala.collection.mutable.Map[String, String]()
  private val symbolicRootPattern = "([_A-Z][_A-Z0-9]*[$])(.*)".r

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

  private def extractUserDefinedRoot(rootDef: String) = {
    val pattern = "([_A-Z][_A-Z0-9]+)=\"([^\"]*)\"".r
    rootDef match {
      case pattern(rootSymbolNoDollar, path) =>
        rootSymbolNoDollar -> path
    }
  }

  private def parseInput(stringIterator: scala.collection.Iterator[scala.Predef.String]) = {
    stringIterator.map { line => line.trim }. // Strip leading and trailing blanks
      filter(line => line.nonEmpty && !line.startsWith("#")). // Stip empty lines and comments
      map(line => extractUserDefinedRoot(line))
  }

  def parseUserDefinedInputFromFile(filename: String) = {
    parseInput(Source.fromFile(filename).getLines)
  }
  def parseUserDefinedInputFromURI(filename: String) = {
    val URI = new java.net.URI(filename)
    parseInput(Source.fromURI(URI).getLines)
  }

  private def checkPathSanity(path: String) = {
    assert(path.isEmpty || path.endsWith("@") || path.endsWith("/"),
      s"path: $path should either be empty or end with a @ or with a slash.")
  }

  def addUserDefinedResolutions() = {
    val userDefinedRootResolutionFile = scala.util.Properties.envOrElse("KITE_ADDITIONAL_ROOT_DEFINITIONS", "")
    if (userDefinedRootResolutionFile.nonEmpty) {
      val userDefinedResolutions = parseUserDefinedInputFromFile(userDefinedRootResolutionFile)
      for ((rootSymbolNoDollar, path) <- userDefinedResolutions) {
        checkPathSanity(path)
        registerRoot(rootSymbolNoDollar + "$", path)
      }
    }
  }

  addUserDefinedResolutions()
}

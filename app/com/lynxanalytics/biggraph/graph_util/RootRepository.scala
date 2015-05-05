// Singleton class that controls the symbolic root names for file handling.

package com.lynxanalytics.biggraph.graph_util

import scala.io.Source

object PathNormalizer {
  def normalize(str: String) = {
    val s1 = "@/+".r.replaceAllIn(str, "@") // Collapes @/ sequences

    // Collapse slashes into one slash, unless in contexts such as s3n://
    val s2 = "([^:])//+".r.replaceAllIn(s1, "$1/")

    // Collapse initial slash sequences as well
    val s3 = "(\\A)//+".r.replaceAllIn(s2, "$1/")
    assert(!s3.contains(".."), "Double dots are not allowed in path names")
    s3
  }
}

object RootRepository {
  private val pathResolutions = scala.collection.mutable.Map[String, String]()
  private val symbolicRootPattern = "([_A-Z][_A-Z0-9]*[$])(.*)".r

  private def getBestCandidate(path: String): (String, String) = {

    val candidates = pathResolutions.filter { x => path.startsWith(x._2) }
    if (candidates.isEmpty) {
      assert(false, s"Cannot find a prefix notation for path $path. " +
        "See KITE_ADDITIONAL_ROOT_DEFINITIONS in .kiterc for a possible solution")
      ???
    } else {
      candidates.maxBy(_._2.length)
    }
  }

  def tryToSplitBasedOnTheAvailableRoots(str: String, legacyMode: Boolean): (String, String) = {
    assert(legacyMode)
    val (rootSym, resolution) = getBestCandidate(str)
    (rootSym, str.drop(resolution.length))
  }
  def splitSymbolicPattern(str: String, legacyMode: Boolean): (String, String) = {
    str match {
      case symbolicRootPattern(rootSymbol, relativePath) =>
        (rootSymbol, relativePath)
      case _ => tryToSplitBasedOnTheAvailableRoots(str, legacyMode)
    }
  }

  private def rootSymbolSyntaxIsOK(rootSymbol: String): Boolean = {
    rootSymbol match {
      case symbolicRootPattern(_, rest) => rest.isEmpty
      case _ => false
    }
  }

  def getRootInfo(rootSymbol: String) = pathResolutions(rootSymbol)

  // This is only used by the testing module
  def dropResolutions() = {
    pathResolutions.clear()
  }

  def registerRoot(rootSymbol: String, rootResolution: String) = {
    assert(!pathResolutions.contains(rootSymbol), s"Root symbol $rootSymbol already set")
    assert(rootSymbolSyntaxIsOK(rootSymbol), s"Invalid root symbol syntax: $rootSymbol")
    val resolvedResolution = rootResolution match {
      case symbolicRootPattern(root, rest) => pathResolutions(root) + rest
      case _ => rootResolution
    }
    pathResolutions(rootSymbol) = PathNormalizer.normalize(resolvedResolution)
  }

  private def extractUserDefinedRoot(rootDef: String) = {
    val pattern = "([_A-Z][_A-Z0-9]+)=\"([^\"]*)\"".r
    rootDef match {
      case pattern(rootSymbolNoDollar, path) =>
        rootSymbolNoDollar -> path
    }
  }

  private def parseInput(stringIterator: scala.collection.Iterator[String]) = {
    stringIterator.map { line => "[#].*$".r.replaceAllIn(line, "") } // Strip comments
      .map { line => line.trim } // Strip leading and trailing blanks
      .filter(line => line.nonEmpty) // Strip blank lines
      .map(line => extractUserDefinedRoot(line))
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
    val userDefinedRootResolutionFile =
      scala.util.Properties.envOrElse("KITE_ADDITIONAL_ROOT_DEFINITIONS", "")
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

// Singleton class that controls the symbolic root names for file handling.

package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

import org.apache.hadoop
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
  private val schemePattern = "[A-Za-z][-\\+\\.A-Za-z0-9]*:".r

  private def hasScheme(filename: String): Boolean = {
    schemePattern.findPrefixMatchOf(filename).nonEmpty
  }

  private def getBestCandidate(path: String): Option[(String, String)] = {

    val candidates = pathResolutions.filter { x => path.startsWith(x._2) }
    if (candidates.isEmpty) {
      None
    } else {
      Some(candidates.maxBy(_._2.length))
    }
  }

  private def fullyQualify(path: String): String =
    HadoopFile.defaultFs.makeQualified(new hadoop.fs.Path(path)).toString

  def tryToSplitBasedOnTheAvailableRoots(path: String): (String, String) =
    getBestCandidate(path)
      .map {
        case (rootSym, resolution) =>
          (rootSym, path.drop(resolution.length))
      }
      .getOrElse(throw new AssertionError(
        s"Cannot find a prefix notation for path $path. " +
          "See KITE_ADDITIONAL_ROOT_DEFINITIONS in .kiterc for a possible solution"))

  def splitSymbolicPattern(str: String, legacyMode: Boolean): (String, String) = {
    str match {
      case symbolicRootPattern(rootSymbol, relativePath) =>
        (rootSymbol, relativePath)
      case _ if legacyMode =>
        if (hasScheme(str)) tryToSplitBasedOnTheAvailableRoots(str)
        else tryToSplitBasedOnTheAvailableRoots(fullyQualify(str))
      case _ =>
        throw new AssertionError(
          "File name specifications should always start with a registered prefix (XYZ$)")
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
    assert(
      resolvedResolution.isEmpty || hasScheme(resolvedResolution),
      "Resolved prefix definition has to specify URI scheme (aka filesystem type) or be empty." +
        s"But ${rootSymbol}'s definition ${rootResolution} resolved to ${resolvedResolution}.")
    pathResolutions(rootSymbol) = PathNormalizer.normalize(resolvedResolution)
  }

  private def extractUserDefinedRoot(rootDef: String): (String, String) = {
    val pattern = "([_A-Z][_A-Z0-9]+)=\"([^\"]*)\"".r
    rootDef match {
      case pattern(rootSymbolNoDollar, path) =>
        rootSymbolNoDollar -> path
    }
  }

  private def parseInput(stringIterator: Iterator[String]): Iterator[(String, String)] = {
    stringIterator.map { line => "[#].*$".r.replaceAllIn(line, "") } // Strip comments
      .map { line => line.trim } // Strip leading and trailing blanks
      .filter(line => line.nonEmpty) // Strip blank lines
      .map(line => extractUserDefinedRoot(line))
  }

  def parseUserDefinedInputFromFile(filename: String): Iterator[(String, String)] = {
    parseInput(Source.fromFile(filename).getLines)
  }
  def parseUserDefinedInputFromURI(filename: String): Iterator[(String, String)] = {
    val URI = new java.net.URI(filename)
    parseInput(Source.fromURI(URI).getLines)
  }

  private def checkPathSanity(path: String) = {
    assert(path.isEmpty || path.endsWith("@") || path.endsWith("/"),
      s"path: $path should either be empty or end with a @ or with a slash.")
  }

  def addUserDefinedResolutions() = {
    val userDefinedRootResolutionFile =
      scala.util.Properties.envOrElse("KITE_ROOT_DEFINITIONS", "")
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

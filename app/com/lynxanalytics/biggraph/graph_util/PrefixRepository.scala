// Singleton class that controls the symbolic prefix names for file handling.

package com.lynxanalytics.biggraph.graph_util

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

class PrefixACLs {
  private val readACLs = scala.collection.mutable.Map[String, String]()
  private val writeACLs = scala.collection.mutable.Map[String, String]()

  def registerReadACL(prefix: String, readACL: String) = readACLs(prefix) = readACL
  def registerWriteACL(prefix: String, writeACL: String) = writeACLs(prefix) = writeACL

  def getWriteACL(prefix: String) =
    if (writeACLs.contains(prefix)) writeACLs(prefix)
    else "*"

  def getReadACL(prefix: String) =
    if (readACLs.contains(prefix)) readACLs(prefix)
    else "*"

  def clear() = {
    readACLs.clear()
    writeACLs.clear()
  }

  def checkSanity(possibleKeys: Seq[String]) = {
    val diffRead = readACLs.keys.toSet &~ possibleKeys.toSet
    assert(diffRead.isEmpty, s"READ_ACL defined for non existing prefixes: $diffRead")
    val diffWrite = writeACLs.keys.toSet &~ possibleKeys.toSet
    assert(diffWrite.isEmpty, s"WRITE_ACL defined for non existing prefixes: $diffWrite")

    val diffWriteRead = writeACLs.keys.toSet &~ readACLs.keys.toSet
    assert(diffWriteRead.isEmpty,
      s"No READ_ACL is defined for these prefixes: $diffWriteRead")

    val diffReadWrite = readACLs.keys.toSet &~ writeACLs.keys.toSet
    assert(diffReadWrite.isEmpty,
      s"No WRITE_ACL is defined for these prefixes: $diffWriteRead")
  }
}

object PrefixRepositoryImpl {
  private val symbolicPrefixPattern = "([_A-Z][_A-Z0-9]*[$])(.*)".r
  private val schemePattern = "[A-Za-z][-\\+\\.A-Za-z0-9]*:".r
  private def hasScheme(filename: String): Boolean = {
    schemePattern.findPrefixMatchOf(filename).nonEmpty
  }
  private def fullyQualify(path: String): String =
    HadoopFile.defaultFs.makeQualified(new hadoop.fs.Path(path)).toString
  private def prefixSymbolSyntaxIsOK(prefixSymbol: String): Boolean = {
    prefixSymbol match {
      case symbolicPrefixPattern(_, rest) => rest.isEmpty
      case _ => false
    }
  }
  private def extractKeyAndValue(line: String): (String, String) = {
    val pattern = "([_A-Z][_A-Z0-9]*)=\"([^\"]*)\"".r
    line match {
      case pattern(key, value) =>
        key -> value
      case _ =>
        throw new AssertionError(s"Could not parse $line")
    }
  }

  def parseInput(inputLines: List[String]): List[(String, String)] = {
    inputLines.map { line => "[#].*$".r.replaceAllIn(line, "") } // Strip comments
      .map { line => line.trim } // Strip leading and trailing blanks
      .filter(line => line.nonEmpty) // Strip blank lines
      .map(line => extractKeyAndValue(line))
  }

  private def checkPathSanity(path: String) = {
    assert(path.isEmpty || path.endsWith("@") || path.endsWith("/"),
      s"path: $path should either be empty or end with a @ or with a slash.")
    // Only local clusters can reference local files
    assert(
      LoggedEnvironment.envOrElse("SPARK_MASTER", "").startsWith("local") ||
        !path.startsWith("file:"),
      s"Local file prefix resolution: ${path}. This is illegal in non-local mode.")
  }

}

class PrefixRepositoryImpl(inputLines: List[String]) {
  import PrefixRepositoryImpl._

  private val pathResolutions = scala.collection.mutable.Map[String, String]()
  private val prefixACLs = new PrefixACLs

  parseKeysAndValues(parseInput(inputLines))
  prefixACLs.checkSanity(pathResolutions.keys.toSeq)

  private def getBestCandidate(path: String): Option[(String, String)] = {
    val candidates = pathResolutions.filter { x => path.startsWith(x._2) }
    if (candidates.isEmpty) {
      None
    } else {
      Some(candidates.maxBy(_._2.length))
    }
  }

  def getWriteACL(prefix: String): String = {
    prefixACLs.getWriteACL(prefix)
  }

  def getReadACL(prefix: String): String = {
    prefixACLs.getReadACL(prefix)
  }

  private def tryToSplitBasedOnTheAvailablePrefixes(path: String): (String, String) =
    getBestCandidate(path)
      .map {
        case (prefixSym, resolution) =>
          (prefixSym, path.drop(resolution.length))
      }
      .getOrElse(throw new AssertionError(
        s"Cannot find a prefix notation for path $path. " +
          "See KITE_ADDITIONAL_PREFIX_DEFINITIONS in .kiterc for a possible solution"))

  def splitSymbolicPattern(str: String, legacyMode: Boolean): (String, String) = {
    str match {
      case symbolicPrefixPattern(prefixSymbol, relativePath) =>
        (prefixSymbol, relativePath)
      case _ if legacyMode =>
        if (hasScheme(str)) tryToSplitBasedOnTheAvailablePrefixes(str)
        else tryToSplitBasedOnTheAvailablePrefixes(fullyQualify(str))
      case _ =>
        throw new AssertionError(
          s"File name specification ${str} should start with a registered prefix (XYZ$$)")
    }
  }

  def getPrefixInfo(prefixSymbol: String) = {
    assert(pathResolutions.contains(prefixSymbol), s"Unknown prefix symbol: $prefixSymbol")
    pathResolutions(prefixSymbol)
  }

  // This is only used by the testing module
  def dropResolutions() = {
    pathResolutions.clear()
    prefixACLs.clear()
  }

  def registerPrefix(prefixSymbol: String, prefixResolution: String) = {
    assert(!pathResolutions.contains(prefixSymbol), s"Prefix symbol $prefixSymbol already set")
    assert(prefixSymbolSyntaxIsOK(prefixSymbol), s"Invalid prefix symbol syntax: $prefixSymbol")
    val resolvedResolution = prefixResolution match {
      case symbolicPrefixPattern(prefix, rest) => pathResolutions(prefix) + rest
      case _ => prefixResolution
    }
    assert(
      resolvedResolution.isEmpty || hasScheme(resolvedResolution),
      "Resolved prefix definition has to specify URI scheme (aka filesystem type) or be empty." +
        s"But ${prefixSymbol}'s definition ${prefixResolution} resolved to ${resolvedResolution}.")
    pathResolutions(prefixSymbol) = PathNormalizer.normalize(resolvedResolution)
  }

  def parseKeysAndValues(input: List[(String, String)]): Unit = {
    for ((key, value) <- input) {
      if (key.endsWith("_READ_ACL")) {
        prefixACLs.registerReadACL(key.dropRight("_READ_ACL".length) + "$", value)
      } else if (key.endsWith("_WRITE_ACL")) {
        prefixACLs.registerWriteACL(key.dropRight("_WRITE_ACL".length) + "$", value)
      } else {
        val prefixSymbolNoDollar = key
        val path = value
        checkPathSanity(path)
        registerPrefix(prefixSymbolNoDollar + "$", path)
      }
    }
  }
}

object PrefixRepository {
  val prefixRepository = {
    val prefixDefinitionFile = LoggedEnvironment.envOrElse("KITE_PREFIX_DEFINITIONS", "")
    if (prefixDefinitionFile.nonEmpty)
      new PrefixRepositoryImpl(Source.fromFile(prefixDefinitionFile).getLines.toList)
    else
      new PrefixRepositoryImpl(List())
  }

  def getPrefixInfo(prefixSymbol: String) =
    prefixRepository.getPrefixInfo(prefixSymbol)
  def splitSymbolicPattern(str: String, legacyMode: Boolean) =
    prefixRepository.splitSymbolicPattern(str, legacyMode)
  def dropResolutions() =
    prefixRepository.dropResolutions()
  def registerPrefix(prefixSymbol: String, prefixResolution: String) =
    prefixRepository.registerPrefix(prefixSymbol, prefixResolution)
  def getWriteACL(prefix: String) =
    prefixRepository.getWriteACL(prefix)
  def getReadACL(prefix: String) =
    prefixRepository.getReadACL(prefix)

}

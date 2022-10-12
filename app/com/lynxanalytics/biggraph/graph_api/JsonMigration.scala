// Classes for implementing metagraph version checks and upgrades from version to version.
package com.lynxanalytics.biggraph.graph_api

import java.io.File
import java.util.UUID
import com.lynxanalytics.biggraph.Environment
import org.apache.commons.io.FileUtils
import play.api.libs.json
import play.api.libs.json.Json

import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.{logger => log}
import com.lynxanalytics.biggraph.controllers._

// This file is responsible for the metadata compatibility between versions.
//
// If there is an incompatible change, please increase the version number of the affected class in
// JsonMigration.version and add an "upgrader" function that turns the old format into the new.
object JsonMigration {
  // VersionMap will always have a default value (0) for classes that are not mentioned.
  type VersionMap = Map[String, Int]

  implicit val versionOrdering = new math.Ordering[VersionMap] {
    override def compare(a: VersionMap, b: VersionMap): Int = {
      val cmp = (a.keySet ++ b.keySet).map { k => a(k) compare b(k) }
      if (cmp.forall(_ == 0)) 0
      else if (cmp.forall(_ <= 0)) -1
      else if (cmp.forall(_ >= 0)) 1
      else {
        assert(false, s"Incomparable versions: $a, $b")
        ???
      }
    }
  }

  private def className(o: Any) = o.getClass.getName.replace("$", "")
  val current = new JsonMigration(
    Map(
      className(graph_operations.ExampleGraph) -> 1,
      className(graph_operations.DoubleLT) -> 1,
      className(graph_operations.DoubleLE) -> 1,
      className(graph_operations.DoubleEQ) -> 1,
      className(graph_operations.DoubleGE) -> 1,
      className(graph_operations.DoubleGT) -> 1,
      className(graph_operations.EnhancedExampleGraph) -> 1,
      // These have Set-typed parameters that have unreliable JSON representations across versions.
      // In LynxKite 4.3.0 we switched to saving them as sorted arrays.
      className(graph_operations.CollectAttribute) -> 1,
      className(graph_operations.EdgesForVerticesFromEdgesAndNeighbors) -> 1,
      className(graph_operations.OneOf) -> 1,
      className(graph_operations.RestrictAttributeToIds) -> 1,
      className(graph_operations.SampledView) -> 1,
    )
      .withDefaultValue(0),
    Map(
      (className(graph_operations.ExampleGraph), 0) -> identity,
      (className(graph_operations.DoubleLT), 0) -> identity,
      (className(graph_operations.DoubleLE), 0) -> identity,
      (className(graph_operations.DoubleEQ), 0) -> identity,
      (className(graph_operations.DoubleGE), 0) -> identity,
      (className(graph_operations.DoubleGT), 0) -> identity,
      (className(graph_operations.EnhancedExampleGraph), 0) -> identity,
      (className(graph_operations.CollectAttribute), 0) -> identity,
      (className(graph_operations.EdgesForVerticesFromEdgesAndNeighbors), 0) -> identity,
      (className(graph_operations.OneOf), 0) -> identity,
      (className(graph_operations.RestrictAttributeToIds), 0) -> identity,
      (className(graph_operations.SampledView), 0) -> identity,
    ))
}
import JsonMigration._
class JsonMigration(
    val version: VersionMap,
    // Upgrader functions keyed by class name and starting version.
    // They take the JsObject from version X to version X + 1.
    val upgraders: Map[(String, Int), Function[json.JsObject, json.JsObject]]) {
  // Make sure we have all the upgraders.
  for ((cls, version) <- version) {
    for (i <- 0 until version) {
      assert(upgraders.contains((cls, i)), s"Upgrader missing for ($cls, $i).")
    }
  }
}

object MetaRepositoryManager {
  // Load repository as current version.
  def apply(rootPath: String): MetaGraphManager =
    apply(rootPath, JsonMigration.current)

  // Load repository as a custom version. This is for testing only.
  def apply(rootPath: String, mig: JsonMigration): MetaGraphManager = {
    val current = findCurrentRepository(new File(rootPath), mig).toString
    new MetaGraphManager(current)
  }

  // Returns the path to the repo belonging to the current version.
  // If the newest repo belongs to an older version, it performs migration.
  // If the newest repo belongs to a newer version, an exception is raised.
  private def findCurrentRepository(repo: File, current: JsonMigration): File = {
    log.info("Exploring meta graph directory versions...")
    val dirs = Option(repo.listFiles).getOrElse(Array())
    import JsonMigration.versionOrdering.mkOrderingOps
    case class DV(dir: File, version: JsonMigration.VersionMap)
    val versions =
      dirs
        .flatMap(dir => readVersion(dir).map(v => DV(dir, v)))
        .sortBy(_.dir.getName.toInt).reverse
    log.info("Meta graph directory versions mapped out.")
    if (versions.isEmpty) {
      val currentDir = new File(repo, "1")
      writeVersion(currentDir, current.version)
      currentDir
    } else {
      val newest = versions.head
      val forcedMigration =
        Environment.envOrNone("KITE_FORCED_MIGRATION").map(_.toBoolean).getOrElse(false)
      if (forcedMigration) {
        log.info("Forced migration requested.")
      }
      if ((newest.version == current.version) && !forcedMigration) newest.dir
      else {
        val supported = versions.find(_.version <= current.version)
        assert(
          newest.version < current.version,
          supported match {
            case Some(supported) =>
              s"The repository data in ${newest.dir} is newer than the current version." +
                s" The most recent supported version is in ${supported.dir}."
            case None =>
              s"All repository data in $repo has a newer version than the current version."
          },
        )
        val last = newest.dir.getName.toInt
        val currentDir = new File(repo, (last + 1).toString)
        FileUtils.deleteDirectory(currentDir) // Make sure we start from scratch.
        log.warn(s"Migrating from ${newest.dir} to $currentDir.")
        migrate(newest.dir.toString, currentDir.toString, newest.version, current)
        writeVersion(currentDir, current.version)
        currentDir
      }
    }
  }

  def readVersion(dir: File) = {
    val versionFile = new File(dir, "version")
    if (versionFile.exists) {
      val data = FileUtils.readFileToString(versionFile, "utf-8")
      val j = Json.parse(data)
      val versions = j.as[VersionMap].withDefaultValue(0)
      Some(versions)
    } else None
  }

  def writeVersion(dir: File, version: VersionMap): Unit = {
    dir.mkdirs
    val versionFile = new File(dir, "version")
    val data = json.JsObject(version.mapValues(json.JsNumber(_)).toSeq)
    FileUtils.writeStringToFile(versionFile, Json.prettyPrint(data), "utf-8")
  }

  def migrate(
      src: String, // Directory to read from.
      dst: String, // Directory to write to.
      srcVersion: VersionMap, // Source version map.
      migration: JsonMigration, // JsonMigration for the current version.
  ): Unit = {
    log.info(s"Migrating from $src to $dst.")
    // A mapping for entity GUIDs (from old to new) that have changed in the new version.
    val guidMapping = collection.mutable.Map[String, String]()
    // Manager will write to "dst".
    val mm = new MetaGraphManager(dst)
    // We will read from "src", convert, and feed into the manager.

    log.info("Migrating operations.")
    for ((file, j) <- MetaGraphManager.loadOperations(src)) {
      try {
        applyOperation(mm, j, guidMapping, srcVersion, migration)
      } catch {
        case e: Throwable => throw new Exception(s"Failed to load $file.", e)
      }
    }

    log.info("Migrating checkpoints.")
    val finalGuidMapping = guidMapping.map {
      case (key, value) =>
        UUID.fromString(key) -> UUID.fromString(value)
    }
    def newGUID(old: UUID): UUID = finalGuidMapping.getOrElse(old, old)

    def updatedCheckpoint(cp: CheckpointObject) = {
      if (cp.snapshot.isDefined) {
        cp.copy(snapshot = Some(cp.snapshot.get.mapGuids(newGUID)))
      } else {
        cp
      }
    }

    val oldRepo = MetaGraphManager.getCheckpointRepo(src)
    for ((checkpoint, state) <- oldRepo.allValidCheckpoints) {
      mm.checkpointRepo.saveCheckpointedState(checkpoint, updatedCheckpoint(state))
    }

    log.info("Migrating tags.")
    val oldTags = TagRoot.loadFromRepo(src)
    mm.setTags(oldTags)
  }

  // Applies the operation from JSON, performing the required migrations.
  private def applyOperation(
      mm: MetaGraphManager,
      j: json.JsValue,
      guidMapping: collection.mutable.Map[String, String],
      srcVersion: VersionMap,
      migration: JsonMigration): Unit = {
    // Call upgraders.
    val op = (j \ "operation").as[json.JsObject]
    val cls = (op \ "class").as[String]
    val v1 = srcVersion(cls)
    val v2 = migration.version(cls)
    val newData = (v1 until v2).foldLeft((op \ "data").as[json.JsObject]) {
      (j, v) => migration.upgraders(cls -> v)(j)
    }
    val newOp = op + ("data" -> newData)
    // Map inputs.
    val inputs = (j \ "inputs").as[Map[String, String]]
    val newInputs = Json.toJson(inputs.map { case (name, g) => name -> guidMapping.getOrElse(g, g) })
    val newJson = j.as[json.JsObject] + ("operation" -> newOp) + ("inputs" -> newInputs)
    // Deserialize the upgraded JSON.
    val inst = mm.applyJson(newJson)
    // Add outputs to the GUID mapping.
    for ((name, guid) <- (j \ "outputs").as[Map[String, String]]) {
      guidMapping(guid) = inst.outputs.all(Symbol(name)).gUID.toString
    }
  }
}

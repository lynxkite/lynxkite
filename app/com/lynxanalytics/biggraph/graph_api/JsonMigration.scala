// Classes for implementing metagraph version checks and upgrades from version to version.
package com.lynxanalytics.biggraph.graph_api

import java.io.File
import java.util.UUID
import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import org.apache.commons.io.FileUtils
import play.api.libs.json
import play.api.libs.json.Json

import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.controllers.CommonProjectState
import com.lynxanalytics.biggraph.controllers.ObsoleteProject
import com.lynxanalytics.biggraph.controllers.RootProjectState
import com.lynxanalytics.biggraph.controllers.SegmentationState

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

  // Replaces fields in a JsObject.
  def replaceJson(jv: json.JsValue, replacements: (String, json.JsValue)*): json.JsObject = {
    val j = jv.as[json.JsObject] // For more convenient invocation.
    val oldValues = j.fields.toMap
    val newValues = replacements.toMap
    new json.JsObject((oldValues ++ newValues).toSeq.sortBy(_._1))
  }

  private def className(o: Any) = o.getClass.getName.replace("$", "")
  val current = new JsonMigration(
    Map(
      className(graph_operations.ComputeVertexNeighborhoodFromTriplets) -> 1,
      className(graph_operations.CreateUIStatusScalar) -> 2,
      className(graph_operations.CreateVertexSet) -> 1,
      className(graph_operations.DoubleBucketing) -> 1,
      className(graph_operations.ExampleGraph) -> 1,
      className(graph_operations.EnhancedExampleGraph) -> 1,
      className(graph_operations.FastRandomEdgeBundle) -> 1,
      className(graph_operations.SampledView) -> 1,
      className(graph_operations.VertexBucketGrid) -> 1,
      className(graph_operations.RegressionModelTrainer) -> 1,
      className(graph_util.HadoopFile) -> 1,
      // Forces a migration due to switch to v2 tags.
      "com.lynxanalytics.biggraph.graph_api.ProjectFrame" -> 1)
      .withDefaultValue(0),
    Map(
      ("com.lynxanalytics.biggraph.graph_api.ProjectFrame", 0) -> identity,
      (className(graph_operations.ComputeVertexNeighborhoodFromTriplets), 0) -> {
        j => JsonMigration.replaceJson(j, "maxCount" -> Json.toJson(1000))
      },
      (className(graph_operations.CreateUIStatusScalar), 1) -> {
        j =>
          val value = JsonMigration.replaceJson(
            j \ "value",
            "customVisualizationFilters" -> Json.toJson(true))
          JsonMigration.replaceJson(j, "value" -> value)
      },
      (className(graph_operations.CreateUIStatusScalar), 0) -> {
        j =>
          val default = json.JsString("neutral")
          val animate = JsonMigration.replaceJson(j \ "value" \ "animate", "style" -> default)
          val value = JsonMigration.replaceJson(j \ "value", "animate" -> animate)
          JsonMigration.replaceJson(j, "value" -> value)
      },
      (className(graph_operations.CreateVertexSet), 0) -> identity,
      (className(graph_operations.DoubleBucketing), 0) -> identity,
      (className(graph_operations.ExampleGraph), 0) -> identity,
      (className(graph_operations.EnhancedExampleGraph), 0) -> identity,
      (className(graph_operations.FastRandomEdgeBundle), 0) -> identity,
      (className(graph_operations.SampledView), 0) -> identity,
      (className(graph_operations.VertexBucketGrid), 0) -> identity,
      (className(graph_operations.RegressionModelTrainer), 0) -> identity,
      (className(graph_util.HadoopFile), 0) -> identity))
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
        LoggedEnvironment.envOrNone("KITE_FORCED_MIGRATION").map(_.toBoolean).getOrElse(false)
      if (forcedMigration) {
        log.info("Forced migration requested.")
      }
      if ((newest.version == current.version) && !forcedMigration) newest.dir
      else {
        val supported = versions.find(_.version <= current.version)
        assert(newest.version < current.version,
          supported match {
            case Some(supported) =>
              s"The repository data in ${newest.dir} is newer than the current version." +
                s" The most recent supported version is in ${supported.dir}."
            case None =>
              s"All repository data in $repo has a newer version than the current version."
          })
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
    migration: JsonMigration // JsonMigration for the current version.
    ): Unit = {
    // A mapping for entity GUIDs (from old to new) that have changed in the new version.
    val guidMapping = collection.mutable.Map[String, String]()
    // Manager will write to "dst".
    val mm = new MetaGraphManager(dst)
    // We will read from "src", convert, and feed into the manager.

    // Operations.
    for ((file, j) <- MetaGraphManager.loadOperations(src)) {
      try {
        applyOperation(mm, j, guidMapping, srcVersion, migration)
      } catch {
        case e: Throwable => throw new Exception(s"Failed to load $file.", e)
      }
    }

    // Checkpoints.
    val finalGuidMapping = guidMapping.map {
      case (key, value) =>
        UUID.fromString(key) -> UUID.fromString(value)
    }
    def newGUID(old: UUID): UUID = finalGuidMapping.getOrElse(old, old)
    def updatedProject(state: CommonProjectState): CommonProjectState =
      CommonProjectState(
        state.vertexSetGUID.map(newGUID),
        state.vertexAttributeGUIDs.mapValues(newGUID),
        state.edgeBundleGUID.map(newGUID),
        state.edgeAttributeGUIDs.mapValues(newGUID),
        state.scalarGUIDs.mapValues(newGUID),
        state.segmentations.mapValues(updatedSegmentation),
        state.notes,
        state.elementNotes,
        state.elementMetadata)
    def updatedSegmentation(segmentation: SegmentationState): SegmentationState =
      SegmentationState(
        updatedProject(segmentation.state),
        segmentation.belongsToGUID.map(newGUID))

    def updatedRootProject(rootState: RootProjectState) =
      RootProjectState(
        updatedProject(rootState.state),
        rootState.checkpoint,
        rootState.previousCheckpoint,
        rootState.lastOperationDesc,
        rootState.lastOperationRequest,
        rootState.viewRecipe)

    val oldRepo = MetaGraphManager.getCheckpointRepo(src)
    for ((checkpoint, state) <- oldRepo.allCheckpoints) {
      mm.checkpointRepo.saveCheckpointedState(checkpoint, updatedRootProject(state))
    }

    // Tags.
    val oldTags = TagRoot.loadFromRepo(src)
    val projectVersion = srcVersion("com.lynxanalytics.biggraph.graph_api.ProjectFrame")
    projectVersion match {
      case 1 =>
        // We already use version 1 tags that are GUID agnostic. All we need to do is copy the tags.
        mm.setTags(oldTags)
      case 0 =>
        // First we upgrade guids
        val guidsFixedTags = oldTags.mapValues(g => guidMapping.getOrElse(g, g))
        val v1TagRoot = TagRoot.temporaryRoot
        v1TagRoot.setTags(guidsFixedTags)
        ObsoleteProject.migrateV1ToV2(v1TagRoot, mm)
      case _ =>
        assert(false, s"Unknown project version $projectVersion")
    }
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
    val newOp = JsonMigration.replaceJson(op, "data" -> newData)
    // Map inputs.
    val inputs = (j \ "inputs").as[Map[String, String]]
    val newInputs = Json.toJson(inputs.map { case (name, g) => name -> guidMapping.getOrElse(g, g) })
    val newJson = JsonMigration.replaceJson(j.as[json.JsObject], "operation" -> newOp, "inputs" -> newInputs)
    // Deserialize the upgraded JSON.
    val inst = mm.applyJson(newJson)
    // Add outputs to the GUID mapping.
    for ((name, guid) <- (j \ "outputs").as[Map[String, String]]) {
      guidMapping(guid) = inst.outputs.all(Symbol(name)).gUID.toString
    }
  }
}

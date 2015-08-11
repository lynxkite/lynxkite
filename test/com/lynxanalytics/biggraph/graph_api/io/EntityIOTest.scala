package com.lynxanalytics.biggraph.graph_api.io

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations.EnhancedExampleGraph
import com.lynxanalytics.biggraph.graph_util.HadoopFile

class EntityIOTest extends FunSuite with TestMetaGraphManager with TestDataManager {

  test("Test ratio sorter") {
    val emptySeq = Seq[Int]()

    val emptySorter = RatioSorter(emptySeq, 13)
    assert(emptySorter.best == None)
    assert(emptySorter.getBestWithinTolerance(2.0) == None)
    assert(emptySorter.getBestWithinTolerance(1.0) == None)

    val nonEmptySeq = (1 until 21 by 2)

    val bestItemInMap = RatioSorter(nonEmptySeq, 12)
    assert(bestItemInMap.best == Some(13))
    assert(bestItemInMap.getBestWithinTolerance(1.09) == Some(13))
    assert(bestItemInMap.getBestWithinTolerance(1.01) == None)
  }

  object EntityDirStatus extends Enumeration {
    val VALID, // Everything is OK
    NOSUCCESS, // The directory is there, but the success file is missing
    NONEXISTENT, // The directory is not there
    CORRUPT // Everything is there, but we want to make sure that we never read the file
    = Value
  }

  def cp(src: HadoopFile, dst: HadoopFile) = {
    assert(src.exists)
    val s = src.path
    val d = dst.path
    // It seems to work locally, and these tests are local
    src.fs.copyFromLocalFile(false, true, s, d)
  }

  val numVerticesInExampleGraph = 8

  def withRestoreGlobals[T](verticesPerPartition: Int, tolerance: Double)(fn: => T): T = {
    val savedVerticesPerPartition = EntityIO.verticesPerPartition
    val savedTolerance = EntityIO.tolerance
    try {
      EntityIO.verticesPerPartition = verticesPerPartition
      EntityIO.tolerance = tolerance
      fn
    } finally {
      EntityIO.verticesPerPartition = savedVerticesPerPartition
      EntityIO.tolerance = savedTolerance
    }
  }

  class MultiPartitionedFileStructure(partitions: Seq[Int]) {
    assert(partitions.nonEmpty)
    import Scripting._
    implicit val metaManager = cleanMetaManager
    val operation = EnhancedExampleGraph()
    val vertices = operation().result.vertices
    val repo = cleanDataManager.repositoryPath
    for (p <- partitions) {
      val dataManager = new DataManager(sparkContext, repo)
      withRestoreGlobals(
        tolerance = 1.0,
        verticesPerPartition = numVerticesInExampleGraph / p) {
          dataManager.get(vertices)
        }
    }
  }

  def collectNumericSubdirs(path: HadoopFile) = {
    val dirs = (path / "*").list.map(_.path.getName)
    val number = "[1-9][0-9]*".r
    dirs.filter(x => number.pattern.matcher(x).matches)
  }

  def modifyEntityDir(entityDir: HadoopFile, task: EntityDirStatus.Value): Unit = {
    task match {
      case EntityDirStatus.VALID =>
      case EntityDirStatus.NONEXISTENT =>
        entityDir.deleteIfExists()
      case EntityDirStatus.NOSUCCESS =>
        val success = entityDir / Success
        success.deleteIfExists()
      case EntityDirStatus.CORRUPT =>
        val corrupt = entityDir / "corruption"
        corrupt.mkdirs() // This makes hadoop raise an exception
    }
  }

  case class EntityScenario(partitionedConfig: Map[Int, EntityDirStatus.Value],
                            legacyConfig: EntityDirStatus.Value = EntityDirStatus.NONEXISTENT,
                            metaPresent: Boolean = true,
                            partitionedExists: Boolean = true,
                            opExists: Boolean = true,
                            numPartitions: Int = 1,
                            parentPartitionDir: Int = 1,
                            tolerance: Double = 2.0) {
    val partitionsToCreate =
      partitionedConfig.filterNot(_._2 == EntityDirStatus.NONEXISTENT)
        .keys.toSet ++ Set(numPartitions)
    val mpfs = new MultiPartitionedFileStructure(partitionsToCreate.toSeq)
    val repo = mpfs.repo
    val gUID = mpfs.vertices.gUID.toString
    val partitionedPath = repo / io.PartitionedDir / gUID
    val legacyPath = repo / io.EntitiesDir / gUID
    val onePartitionedPath = partitionedPath / "1"

    // Setup legacy path first
    // It has one partition, so we initialize it from onePartitionedPath, ...
    assert(onePartitionedPath.exists) // which surely exists

    if (legacyConfig != EntityDirStatus.NONEXISTENT) {
      cp(onePartitionedPath, legacyPath)
      modifyEntityDir(legacyPath, legacyConfig)
    }

    // Now we can delete any superfluous directories, even
    // onePartitionedPath, if its creation was not requested explicitly.
    for (i <- collectNumericSubdirs(partitionedPath)) {
      if (!partitionedConfig.contains(i.toInt) ||
        partitionedConfig(i.toInt) == EntityDirStatus.NONEXISTENT) {
        (partitionedPath / i).delete()
      }
    }

    // Configure the requested partition directories:
    for ((partitionNum, task) <- partitionedConfig) {
      val entityDir = partitionedPath / partitionNum.toString
      modifyEntityDir(entityDir, task)
    }

    // Deal with metafile:
    if (!metaPresent || partitionedConfig.filterNot(_._2 == EntityDirStatus.NONEXISTENT).isEmpty) {
      val metaFile = partitionedPath / io.Metadata
      metaFile.deleteIfExists()
    }

    // Deal with whole partitioned directory:
    if (!partitionedExists) {
      partitionedPath.deleteIfExists()
    }

    // Operation exist scenario:
    if (!opExists) {
      val opGUID = mpfs.vertices.source.gUID.toString
      val opPath = repo / io.OperationsDir / opGUID
      opPath.delete()
    }

    mpfs.operation.executionCounter = 0
    withRestoreGlobals(
      tolerance = tolerance,
      verticesPerPartition = numVerticesInExampleGraph / numPartitions) {
        val dataManager = new DataManager(sparkContext, repo)
        dataManager.get(mpfs.vertices)
      }
    val executionCounter = mpfs.operation.executionCounter
  }

  test("Meta test: corruption hack works") {
    intercept[Throwable] {
      EntityScenario(Map(1 -> EntityDirStatus.CORRUPT))
    }
  }

  test("We can migrate old data without recalculation") {
    val es = EntityScenario(Map(),
      legacyConfig = EntityDirStatus.VALID)
    assert(es.executionCounter == 0)
  }

  test("We don't migrate old data if operation was not finished") {
    val es = EntityScenario(Map(),
      legacyConfig = EntityDirStatus.VALID,
      opExists = false)
    assert(es.executionCounter == 1)
  }

  test("We don't migrate incomplete old data; we recalculate instead") {
    val es = EntityScenario(Map(),
      legacyConfig = EntityDirStatus.NOSUCCESS)
    assert(es.executionCounter == 1)
  }

  test("We read from the partitioned directory even if there's available data in legacy") {
    val es = EntityScenario(Map(
      1 -> EntityDirStatus.VALID,
      2 -> EntityDirStatus.CORRUPT,
      4 -> EntityDirStatus.CORRUPT),
      legacyConfig = EntityDirStatus.CORRUPT)
    assert(es.executionCounter == 0)
  }

  test("We read from the partitioned directory even if there's available data in legacy - from partition 2") {
    val es = EntityScenario(Map(
      1 -> EntityDirStatus.CORRUPT,
      2 -> EntityDirStatus.VALID,
      4 -> EntityDirStatus.CORRUPT),
      legacyConfig = EntityDirStatus.CORRUPT,
      numPartitions = 2)
    assert(es.executionCounter == 0)
  }

  test("After recalculation, stale files are deleted") {
    val es = EntityScenario(Map(
      1 -> EntityDirStatus.VALID,
      2 -> EntityDirStatus.VALID,
      4 -> EntityDirStatus.VALID),
      legacyConfig = EntityDirStatus.VALID,
      opExists = false)
    assert(es.executionCounter == 1)
    val legacyDir = es.legacyPath
    assert(!legacyDir.exists)
    val pfiles = (es.partitionedPath / "*").list
    assert(pfiles.size == 2) // One dir and one metafile
  }

  test("Missing metafile triggers recalculation") {
    val es = EntityScenario(Map(
      1 -> EntityDirStatus.VALID),
      metaPresent = false)
    assert(es.executionCounter == 1)
  }

  test("Re-partitioning uses the right source partition (for 2)") {
    val es = EntityScenario(Map(
      1 -> EntityDirStatus.VALID,
      8 -> EntityDirStatus.CORRUPT),
      numPartitions = 2)
    assert(es.executionCounter == 0)
  }

  test("Re-partitioning uses the right source partition (for 4)") {
    val es = EntityScenario(Map(
      1 -> EntityDirStatus.CORRUPT,
      8 -> EntityDirStatus.VALID),
      numPartitions = 4)
    assert(es.executionCounter == 0)
  }

  test("Re-partitioning doesn't happen if there's a candidate within the given tolerance") {
    val es = EntityScenario(Map(
      1 -> EntityDirStatus.VALID,
      8 -> EntityDirStatus.VALID),
      numPartitions = 2,
      tolerance = 3.0)
    assert(es.executionCounter == 0)
    val pfiles = (es.partitionedPath / "*").list.map(_.path.getName).toSet
    assert(pfiles == Set(io.Metadata, "1", "8"))
  }

}

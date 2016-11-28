package com.lynxanalytics.biggraph.graph_api.io

import com.lynxanalytics.biggraph.TestUtils
import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations.EnhancedExampleGraph
import com.lynxanalytics.biggraph.graph_util.{ PrefixRepository, HadoopFile }

class EntityIOTest extends FunSuite with TestMetaGraphManager with TestDataManager {

  val resDir = "/graph_api/io/EntityIOTest/"
  val res = getClass.getResource(resDir).toString
  PrefixRepository.registerPrefix("ENTITYIOTEST$", res)

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
    CORRUPT // Reading should cause an exception
    = Value
  }

  def copyDirContents(srcDir: HadoopFile, dstDir: HadoopFile) = {
    val files = (srcDir / "*").list
    for (f <- files) {
      val name = f.path.getName
      val s = f.path
      val d = (dstDir / name).path
      srcDir.fs.copyFromLocalFile( /* delSrc = */ false, /* overwrite = */ true, s, d)
    }

  }

  val numVerticesInExampleGraph = 8

  // A data repository with a vertex set partitioned in multiple ways.
  class MultiPartitionedFileStructure(partitions: Seq[Int]) {
    import Scripting._
    implicit val metaManager = cleanMetaManager
    val operation = EnhancedExampleGraph()
    val vertices = operation().result.vertices
    val weight = operation().result.weight
    val repo = cleanDataManager.repositoryPath
    for (p <- partitions) {
      val dataManager = new DataManager(sparkContext, repo)
      TestUtils.withRestoreGlobals(
        tolerance = 1.0,
        verticesPerPartition = numVerticesInExampleGraph / p) {
          dataManager.get(vertices)
          dataManager.get(weight) // This line invokes both EdgeBundle and Attribute[double] loads
          dataManager.waitAllFutures()
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
        corrupt.mkdirs() // The unexpected directory will cause the read to fail.
    }
  }

  case class EntityScenario(partitionedConfig: Map[Int, EntityDirStatus.Value],
                            legacyConfig: EntityDirStatus.Value = EntityDirStatus.NONEXISTENT,
                            metaExists: Boolean = true,
                            partitionedExists: Boolean = true,
                            opExists: Boolean = true,
                            numPartitions: Int = 1,
                            parentPartitionDir: Int = 1,
                            tolerance: Double = 2.0) {
    // Create at least one partition so that we would have an operation.
    val partitionsToCreate = partitionedConfig.keySet + 1
    val mpfs = new MultiPartitionedFileStructure(partitionsToCreate.toSeq)
    val repo = mpfs.repo
    val gUID = mpfs.vertices.gUID.toString
    val partitionedPath = repo / io.PartitionedDir / gUID
    val legacyPath = repo / io.EntitiesDir / gUID

    if (legacyConfig != EntityDirStatus.NONEXISTENT) {
      val legacyData = HadoopFile("ENTITYIOTEST$") / gUID
      copyDirContents(legacyData, legacyPath)
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
    if (!metaExists || partitionedConfig.filterNot(_._2 == EntityDirStatus.NONEXISTENT).isEmpty) {
      val metaFile = partitionedPath / io.Metadata
      metaFile.deleteIfExists()
    }

    // Deal with whole partitioned directory:
    if (!partitionedExists) {
      partitionedPath.deleteIfExists()
    }

    // Make the source operation unsuccessful if requested.
    if (!opExists) {
      val opGUID = mpfs.vertices.source.gUID.toString
      val opPath = repo / io.OperationsDir / opGUID
      opPath.delete()
    }

    // See what happens when we try to load the vertex set.
    mpfs.operation.executionCounter = 0
    TestUtils.withRestoreGlobals(
      tolerance = tolerance,
      verticesPerPartition = numVerticesInExampleGraph / numPartitions) {
        val dataManager = new DataManager(sparkContext, repo)
        val data = dataManager.get(mpfs.vertices)
        assert(data.rdd.collect.toSeq.sorted == (0 until numVerticesInExampleGraph).map(_ -> (())))
        dataManager.waitAllFutures()
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
      metaExists = false)
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

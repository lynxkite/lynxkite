package com.lynxanalytics.biggraph.graph_api.io

import org.scalatest.FunSuite

import com.lynxanalytics.biggraph.graph_api._
import com.lynxanalytics.biggraph.graph_operations.{ EnhancedExampleGraph }
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

  def lsDebug(hadoopFile: HadoopFile, pattern: String = ""): Unit = {
    lsDebugRec(hadoopFile, pattern)
    println("")
  }

  def lsDebugRec(hadoopFile: HadoopFile, pattern: String): Unit = {
    if (hadoopFile.resolvedName.contains(pattern)) println(hadoopFile)
    val l = (hadoopFile / "*").list
    def fun(x: HadoopFile) = lsDebugRec(x, pattern)
    l.foreach(fun)
  }

  def cp(src: HadoopFile, dst: HadoopFile) = {
    assert(src.exists)
    val s = src.path
    val d = dst.path
    // It seems to work locally, and these tests are local
    src.fs.copyFromLocalFile(false, true, s, d)
  }

  val numVerticesInExampleGraph = 8

  def withRestoreGlobals[T](fn: => T, verticesPerPart: Int = 0, tolerance: Double = 0.0): T = {
    val savedVerticesPerPartition = EntityIO.getVerticesPerPartition
    val savedTolerance = EntityIO.getTolerance
    if (verticesPerPart > 0) EntityIO.setVerticesPerPartition(verticesPerPart)
    if (tolerance > 0.0) EntityIO.setTolerance(tolerance)
    try {
      fn
    } finally {
      EntityIO.setVerticesPerPartition(savedVerticesPerPartition)
      EntityIO.setTolerance(savedTolerance)
    }
  }

  // This type that should come back from where we prepare the
  // file structure
  type GenesisDataType = (Option[HadoopFile], // The repositoryPath for the DataManager
  VertexSetData, // The vertex set data created
  VertexSet // // Metaset
  )

  def createVertexSetData(sourcePath: Option[HadoopFile] = None): GenesisDataType = {
    val metaManager = cleanMetaManager
    val operation = EnhancedExampleGraph()
    val instance = metaManager.apply(operation)
    val vertices = instance.outputs.vertexSets('vertices)
    val dataManager = sourcePath match {
      case None => cleanDataManager
      case x => new DataManager(sparkContext, x.get)
    }
    val vertexSetData = dataManager.get(vertices)
    (Option(dataManager.repositoryPath), vertexSetData, vertices)
  }

  def wrapper(partNum: Int, path: Option[HadoopFile]): GenesisDataType = {
    withRestoreGlobals(
      {
        createVertexSetData(path)
      },
      tolerance = 1.0,
      verticesPerPart = numVerticesInExampleGraph / partNum)
  }

  def createMultiPartitionedFileStructure(partitions: Seq[Int]): GenesisDataType = {
    assert(partitions.nonEmpty)
    val (path, vertexSetData, vertices) = wrapper(partitions.head, None)
    for (v <- partitions.tail) {
      wrapper(v, path)
    }
    (path, vertexSetData, vertices)
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
    val (path, genesisVertexSetData, genesisVertexSet) =
      createMultiPartitionedFileStructure(partitionsToCreate.toSeq)
    val gUID = genesisVertexSetData.entity.gUID.toString
    val partitionedPath = path.get / io.PartitionedDir / gUID
    val legacyPath = path.get / io.EntitiesDir / gUID
    val onePartitionedPath = partitionedPath / "1"

    //    lsDebug(path.get, gUID)

    // Setup legacy path first
    // It has one partition, so we initialize it from onePartitionedPath, ...
    assert(onePartitionedPath.exists) // which surely exists

    if (legacyConfig != EntityDirStatus.NONEXISTENT) {
      cp(onePartitionedPath, legacyPath)
      modifyEntityDir(legacyPath, legacyConfig)
    }

    //    lsDebug(path.get, gUID)

    // Now we can delete any superfluous directories, even
    // onePartitionedPath, if its creation was not requested explicitly.
    for (i <- collectNumericSubdirs(partitionedPath)) {
      if (!partitionedConfig.contains(i.toInt) ||
        partitionedConfig(i.toInt) == EntityDirStatus.NONEXISTENT) {
        (partitionedPath / i).delete()
      }
    }

    //    lsDebug(path.get, gUID)

    // Configure the requested partition directories:
    for ((partitionNum, task) <- partitionedConfig) {
      val entityDir = partitionedPath / partitionNum.toString
      modifyEntityDir(entityDir, task)
    }

    //    lsDebug(path.get, gUID)

    // Deal with metafile:
    if (!metaPresent || partitionedConfig.filterNot(_._2 == EntityDirStatus.NONEXISTENT).isEmpty) {
      val metaFile = partitionedPath / io.Metadata
      metaFile.deleteIfExists()
    }

    // Deal with whole partitioned directory:
    if (!partitionedExists) {
      partitionedPath.deleteIfExists()
    }

    //    lsDebug(path.get, gUID)

    // Operation exist scenario:
    if (!opExists) {
      val opGUID = genesisVertexSetData.entity.source.gUID.toString
      val opPath = path.get / io.OperationsDir / opGUID
      opPath.delete()
    }

    //    lsDebug(path.get, gUID)

    val (operation, dataManager, vertexSetData, vertices) =
      withRestoreGlobals({
        val metaManager = cleanMetaManager
        val operation = EnhancedExampleGraph()
        val instance = metaManager.apply(operation)
        val vertices = instance.outputs.vertexSets('vertices)
        val dataManager = new DataManager(sparkContext, path.get)
        val vertexSetData = dataManager.get(vertices)
        (operation, dataManager, vertexSetData, vertices)
      },
        tolerance = tolerance,
        verticesPerPart = numVerticesInExampleGraph / numPartitions)

    val executionCounter = operation.executionCounter
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

  test("We're safe against missing metadata") {
    val es = EntityScenario(Map(
      1 -> EntityDirStatus.VALID),
      metaPresent = false)
    assert(es.executionCounter == 1)
  }
}
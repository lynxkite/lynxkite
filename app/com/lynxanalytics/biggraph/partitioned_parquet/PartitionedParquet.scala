// A custom Spark data source that is identical to the normal Parquet data source
// except it preserves the number of partitions.
package com.lynxanalytics.biggraph.partitioned_parquet

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.datasources.v2.parquet._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PartitionedParquet extends FileDataSourceV2 {
  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]
  override def shortName(): String = "partitioned parquet"
  override def getTable(options: CaseInsensitiveStringMap): Table = getTable(options, null)
  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    new PartitionedParquetTable(
      tableName,
      sparkSession,
      optionsWithoutPaths,
      paths,
      Option(schema),
      fallbackFileFormat)
  }
}
object PartitionedParquet {
  val format = classOf[PartitionedParquet].getName
}

class PartitionedParquetTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
    extends ParquetTable(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat) {
  override def newScanBuilder(options: CaseInsensitiveStringMap): ParquetScanBuilder = {
    new PartitionedParquetScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
  }
  override def formatName: String = "PartitionedParquet"
}

class PartitionedParquetScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap)
    extends ParquetScanBuilder(sparkSession, fileIndex, schema, dataSchema, options) {
  override def build(): Scan = {
    new PartitionedParquetScan(
      sparkSession,
      hadoopConf,
      fileIndex,
      dataSchema,
      readDataSchema(),
      readPartitionSchema(),
      pushedParquetFilters,
      options)
  }
}

class PartitionedParquetScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty)
    extends ParquetScan(
      sparkSession,
      hadoopConf,
      fileIndex,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      pushedFilters,
      options,
      partitionFilters,
      dataFilters) {

  override def partitions: Seq[FilePartition] = {
    val numPartitions = options.get("partitions").toInt
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val partPattern = raw"part-([0-9]{5}).*".r
    val allFiles = selectedPartitions.flatMap(_.files)
    val partFiles = selectedPartitions.flatMap { partition =>
      partition.files.map { file =>
        file.getPath.getName match {
          case partPattern(n) =>
            assert(n.toInt < numPartitions, s"Unexpected partition index: ${file.getPath}")
            n.toInt -> PartitionedFileUtil.getPartitionedFile(file, file.getPath, partition.values)
        }
      }
    }.toMap
    assert(partFiles.size == allFiles.size, s"Repeated partition index in $allFiles")
    (0 until numPartitions).map(i => FilePartition(i, partFiles.get(i).map(f => Array(f)).getOrElse(Array())))
  }
}

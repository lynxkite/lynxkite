// Reads Neo4j data files from disk.
package com.lynxanalytics.lynxkite.frontend_operations
import scala.collection.JavaConverters._
import org.apache.spark
import org.neo4j.kernel.impl.store.record.RecordLoad.CHECK
import org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL
import org.neo4j.kernel.impl.store.PropertyType
import com.lynxanalytics.lynxkite.graph_api._
import com.lynxanalytics.lynxkite.graph_util.SoftHashMap
import com.lynxanalytics.lynxkite.graph_util.Timestamp
import com.lynxanalytics.lynxkite.spark_util.Implicits._
import com.lynxanalytics.lynxkite.spark_util.RDDUtils
import com.lynxanalytics.lynxkite.spark_util.SQLHelper
import com.lynxanalytics.lynxkite.{logger => log}
import Scripting._

// Opens a Neo4j storage directory. (A directory with "neostore.XXX.db" files.)
class Neo4j(path: String) {
  val stores = {
    val CONFIG = org.neo4j.kernel.configuration.Config.defaults
    val LOG_PROVIDER = org.neo4j.logging.NullLogProvider.getInstance
    val databaseLayout = org.neo4j.io.layout.DatabaseLayout.of(new java.io.File(path))
    val fs = new org.neo4j.io.fs.DefaultFileSystemAbstraction
    val idGenFactory = new org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory(fs)
    val pageCache = org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache(
      fs,
      org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler)
    val storeFactory =
      new org.neo4j.kernel.impl.store.StoreFactory(
        databaseLayout,
        CONFIG,
        idGenFactory,
        pageCache,
        fs,
        LOG_PROVIDER,
        org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier.EMPTY)
    storeFactory.openAllNeoStores
  }
  def withCursor[T](store: org.neo4j.kernel.impl.store.CommonAbstractStore[_, _], start: Long)(
      fn: org.neo4j.io.pagecache.PageCursor => T): T = {
    val cursor = store.openPageCursorForReading(start)
    try fn(cursor)
    finally cursor.close
  }
}
object Neo4j {
  // Ideally we would independently open the store in each executor. But opening the store locks it,
  // so we can't open it multiple times in parallel. As a temporary hack we just reuse the same
  // instance. This won't work in a distributed setup though. TODO: Fix this.
  private val cache = new SoftHashMap[String, Neo4j]
  def apply(path: String) = {
    cache.getOrElseUpdate(path, new Neo4j(path))
  }

  def readNodes(
      ss: spark.sql.SparkSession,
      path: String,
      labels: Seq[String],
      properties: Seq[(String, SerializableType[_])]): spark.sql.DataFrame = {
    val columns = Seq("_id" -> SerializableType.long, "_labels" -> SerializableType.string) ++ properties
    val schema = SQLHelper.dataFrameSchemaForTypes(columns.map { case (k, v) => (k, v.typeTag) })
    val wantedProperties = properties.map(_._1).toSet
    val propertyTypes = properties.toMap
    // Like CommonAbstractStore.scanAllRecords, but parallel.
    val highId = Neo4j(path).stores.getNodeStore.getHighId
    val partitions = math.max(1, (highId / io.EntityIO.verticesPerPartition).ceil.toInt)
    val partitionStep = highId / partitions + 1 // Rounding up, but overshooting is okay.
    val rdd = ss.sparkContext.parallelize(0 until partitions, partitions).flatMap { i =>
      val neo = Neo4j(path)
      val propStore = neo.stores.getPropertyStore
      val keyNameStore = propStore.getPropertyKeyTokenStore.getNameStore
      val store = neo.stores.getNodeStore
      val labelStore = neo.stores.getLabelTokenStore
      val keyStore = neo.stores.getPropertyKeyTokenStore
      // The range of records to cover in this partition.
      val start = math.max(i * partitionStep, store.getNumberOfReservedLowIds)
      val end = math.min((i + 1) * partitionStep, highId)
      val record = store.newRecord
      val prop = propStore.newRecord
      val keyRec = keyStore.newRecord
      val labelRec = labelStore.newRecord
      val keysSeen = new collection.mutable.HashSet[String]
      neo.withCursor(store, start) { cursor =>
        (start until end).flatMap { id =>
          store.getRecordByCursor(id, record, CHECK, cursor)
          if (!record.inUse) None // It's an empty record, e.g. a deleted node.
          else {
            val ls = org.neo4j.kernel.impl.store.NodeLabelsField.get(record, store).toList
            val lns = ls.map { l => labelStore.getStringFor(labelStore.getRecord(l, labelRec, NORMAL)) }
            // Extract this node if there was no label constraint, or the node fits the constraint.
            val matched = labels.isEmpty || lns.exists(ln => labels.contains(ln))
            if (!matched) None
            else Some {
              spark.sql.Row.fromSeq {
                val propMap = {
                  // Properties are stored in PropertyBlocks within PropertyRecords in the
                  // PropertyStore. The NodeRecord points to the first PropertyRecord, then
                  // we follow the chain of a linked list to visit all the PropertyRecords.
                  val pm = collection.mutable.Map[String, Any]()
                  var pp = record.getNextProp
                  while (pp != -1) {
                    propStore.getRecord(pp, prop, NORMAL)
                    for (p <- prop.iterator.asScala) {
                      val key = p.getKeyIndexId
                      keyStore.getRecord(key, keyRec, NORMAL)
                      val keyName = keyStore.getStringFor(keyRec)
                      // Extract a property if it matches the user-provided schema (name and type).
                      if (wantedProperties.contains(keyName) && typeMatch(p.getType, propertyTypes(keyName))) {
                        try {
                          pm(keyName) = p.newPropertyValue(propStore).asObject
                        } catch {
                          case t: Throwable =>
                            log.error(
                              s"Error while reading $keyName (${p.getType.name}, $p), will treat as missing.",
                              t)
                        }
                        // If it doesn't match, we just log it. (The first time.)
                      } else if (!keysSeen.contains(keyName)) {
                        log.info(s"New ignored property encountered: $keyName (${p.getType.name})")
                        keysSeen += keyName
                      }
                    }
                    pp = prop.getNextProp
                  }
                  pm
                }
                // Build the Spark DataFrame row, matching the schema.
                Seq(record.getId, lns.mkString(", ")) ++ properties.map { case (k, _) => propMap.getOrElse(k, null) }
              }
            }
          }
        }
      }
    }
    ss.createDataFrame(rdd, schema)
  }

  def readRelationships(
      ss: spark.sql.SparkSession,
      path: String,
      types: Seq[String],
      properties: Seq[(String, SerializableType[_])]): spark.sql.DataFrame = {
    val columns = Seq(
      "_id" -> SerializableType.long,
      "_type" -> SerializableType.string,
      "_src" -> SerializableType.long,
      "_dst" -> SerializableType.long) ++ properties
    val schema = SQLHelper.dataFrameSchemaForTypes(columns.map { case (k, v) => (k, v.typeTag) })
    val wantedProperties = properties.map(_._1).toSet
    val propertyTypes = properties.toMap
    // Like CommonAbstractStore.scanAllRecords, but parallel.
    val highId = Neo4j(path).stores.getRelationshipStore.getHighId
    val partitions = math.max(1, (highId / io.EntityIO.verticesPerPartition).ceil.toInt)
    val partitionStep = highId / partitions + 1 // Rounding up, but overshooting is okay.
    val rdd = ss.sparkContext.parallelize(0 until partitions, partitions).flatMap { i =>
      val neo = Neo4j(path)
      val propStore = neo.stores.getPropertyStore
      val keyNameStore = propStore.getPropertyKeyTokenStore.getNameStore
      val store = neo.stores.getRelationshipStore
      val keyStore = neo.stores.getPropertyKeyTokenStore
      val relTypeStore = neo.stores.getRelationshipTypeTokenStore
      // The range of records to cover in this partition.
      val start = math.max(i * partitionStep, store.getNumberOfReservedLowIds)
      val end = math.min((i + 1) * partitionStep, highId)
      val record = store.newRecord
      val prop = propStore.newRecord
      val keyRec = keyStore.newRecord
      val relTypeRec = relTypeStore.newRecord
      val keysSeen = new collection.mutable.HashSet[String]
      neo.withCursor(store, start) { cursor =>
        (start until end).flatMap { id =>
          store.getRecordByCursor(id, record, CHECK, cursor)
          if (!record.inUse) None // It's an empty record, e.g. a deleted edge.
          else {
            val relType = relTypeStore.getStringFor(relTypeStore.getRecord(record.getType, relTypeRec, NORMAL))
            // Extract this edge if there was no type constraint, or the edge fits the constraint.
            val matched = types.isEmpty || types.contains(relType)
            if (!matched) None
            else Some {
              spark.sql.Row.fromSeq {
                val propMap = {
                  // Properties are stored in PropertyBlocks within PropertyRecords in the
                  // PropertyStore. The RelationshipRecord points to the first PropertyRecord, then
                  // we follow the chain of a linked list to visit all the PropertyRecords.
                  val pm = collection.mutable.Map[String, Any]()
                  var pp = record.getNextProp
                  while (pp != -1) {
                    propStore.getRecord(pp, prop, NORMAL)
                    for (p <- prop.iterator.asScala) {
                      val key = p.getKeyIndexId
                      keyStore.getRecord(key, keyRec, NORMAL)
                      val keyName = keyStore.getStringFor(keyRec)
                      // Extract a property if it matches the user-provided schema (name and type).
                      if (wantedProperties.contains(keyName) && typeMatch(p.getType, propertyTypes(keyName))) {
                        try {
                          pm(keyName) = p.newPropertyValue(propStore).asObject
                        } catch {
                          case t: Throwable =>
                            log.error(
                              s"Error while reading $keyName (${p.getType.name}, $p), will treat as missing.",
                              t)
                        }
                        // If it doesn't match, we just log it. (The first time.)
                      } else if (!keysSeen.contains(keyName)) {
                        log.info(s"New ignored property encountered: $keyName (${p.getType.name})")
                        keysSeen += keyName
                      }
                    }
                    pp = prop.getNextProp
                  }
                  pm
                }
                // Build the Spark DataFrame row, matching the schema.
                Seq(
                  record.getId,
                  relType,
                  record.getFirstNode,
                  record.getSecondNode) ++ properties.map { case (k, _) => propMap.getOrElse(k, null) }
              }
            }
          }
        }
      }
    }
    ss.createDataFrame(rdd, schema)
  }

  def typeMatch[T](got: PropertyType, expected: SerializableType[T]) = {
    def isNumber(got: PropertyType) = {
      got == PropertyType.SHORT || got == PropertyType.INT || got == PropertyType.LONG ||
      got == PropertyType.SHORT || got == PropertyType.DOUBLE || got == PropertyType.FLOAT
    }
    expected match {
      case SerializableType.string => got == PropertyType.STRING || got == PropertyType.SHORT_STRING
      case SerializableType.double => isNumber(got)
      case SerializableType.long => isNumber(got)
      case SerializableType.int => isNumber(got)
      case SerializableType.boolean => got == PropertyType.BOOL
      // No strict type validation for other type.
      case _ => true
    }
  }
}
